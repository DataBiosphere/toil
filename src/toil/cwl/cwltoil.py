"""Implemented support for Common Workflow Language (CWL) for Toil."""

# Copyright (C) 2015 Curoverse, Inc
# Copyright (C) 2015-2021 Regents of the University of California
# Copyright (C) 2019-2020 Seven Bridges
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# For an overview of how this all works, see discussion in
# docs/architecture.rst
import base64
import copy
import datetime
import errno
import functools
import glob
import io
import json
import logging
import os
import pprint
import shutil
import stat
import sys
import textwrap
import uuid
from collections.abc import Iterator, Mapping, MutableMapping, MutableSequence
from tempfile import NamedTemporaryFile, TemporaryFile, gettempdir
from threading import Thread
from typing import (
    IO,
    Any,
    Callable,
    Iterator,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    TextIO,
    Tuple,
    TypeVar,
    Union,
    cast,
    Literal,
    Protocol,
)
from urllib.parse import quote, unquote, urlparse, urlsplit

import cwl_utils.errors
import cwl_utils.expression
import cwltool.builder
import cwltool.command_line_tool
import cwltool.context
import cwltool.cwlprov
import cwltool.job
import cwltool.load_tool
import cwltool.main
import cwltool.resolver
import schema_salad.ref_resolver

# This is also in configargparse but MyPy doesn't know it
from argparse import RawDescriptionHelpFormatter
from configargparse import ArgParser, Namespace
from cwltool.loghandler import _logger as cwllogger
from cwltool.loghandler import defaultStreamHandler
from cwltool.mpi import MpiConfig
from cwltool.mutation import MutationManager
from cwltool.pathmapper import MapperEnt, PathMapper
from cwltool.process import (
    Process,
    add_sizes,
    compute_checksums,
    fill_in_defaults,
    shortname,
)
from cwltool.secrets import SecretStore
from cwltool.singularity import SingularityCommandLineJob
from cwltool.software_requirements import (
    DependenciesConfiguration,
    get_container_from_software_requirements,
)
from cwltool.stdfsaccess import StdFsAccess, abspath
from cwltool.utils import (
    CWLObjectType,
    CWLOutputType,
    DirectoryType,
    adjustDirObjs,
    aslist,
    downloadHttpFile,
    get_listing,
    normalizeFilesDirs,
    visit_class,
)
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from schema_salad.avro.schema import Names
from schema_salad.exceptions import ValidationException
from schema_salad.ref_resolver import file_uri, uri_file_path
from schema_salad.sourceline import SourceLine

from toil.batchSystems.abstractBatchSystem import InsufficientSystemResources
from toil.batchSystems.registry import DEFAULT_BATCH_SYSTEM
from toil.common import Config, Toil, addOptions
from toil.cwl import check_cwltool_version
from toil.lib.integration import resolve_workflow
from toil.lib.misc import call_command
from toil.provisioners.clusterScaler import JobTooBigError

check_cwltool_version()
from toil.cwl.utils import (
    CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION,
    CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE,
    download_structure,
    get_from_structure,
    visit_cwl_class_and_reduce,
)
from toil.exceptions import FailedJobsException
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import (
    AcceleratorRequirement,
    Job,
    Promise,
    Promised,
    unwrap,
    ImportsJob,
    get_file_sizes,
    FileMetadata,
    WorkerImportJob,
)
from toil.jobStores.abstractJobStore import (
    AbstractJobStore,
    NoSuchFileException,
    InvalidImportExportUrlException,
    LocatorException,
)
from toil.lib.exceptions import UnimplementedURLException
from toil.jobStores.fileJobStore import FileJobStore
from toil.jobStores.utils import JobStoreUnavailableException, generate_locator
from toil.lib.io import mkdtemp
from toil.lib.threading import ExceptionalThread, global_mutex
from toil.statsAndLogging import DEFAULT_LOGLEVEL

logger = logging.getLogger(__name__)

# Find the default temporary directory
DEFAULT_TMPDIR = gettempdir()
# And compose a CWL-style default prefix inside it.
# We used to not put this inside anything and we would drop loads of temp
# directories in the current directory and leave them there.
DEFAULT_TMPDIR_PREFIX = os.path.join(DEFAULT_TMPDIR, "tmp")


def cwltoil_was_removed() -> None:
    """Complain about deprecated entrypoint."""
    raise RuntimeError(
        'Please run with "toil-cwl-runner" instead of "cwltoil" '
        "(which has been removed)."
    )


# The job object passed into CWLJob and CWLWorkflow
# is a dict mapping to tuple of (key, dict)
# the final dict is derived by evaluating each
# tuple looking up the key in the supplied dict.
#
# This is necessary because Toil jobs return a single value (a dict)
# but CWL permits steps to have multiple output parameters that may
# feed into multiple other steps.  This transformation maps the key in the
# output object to the correct key of the input object.


class UnresolvedDict(dict[Any, Any]):
    """Tag to indicate a dict contains promises that must be resolved."""


class SkipNull:
    """
    Internal sentinel object.

    Indicates a null value produced by each port of a skipped conditional step.
    The CWL 1.2 specification calls for treating this the exactly the same as a
    null value.
    """


def filter_skip_null(name: str, value: Any) -> Any:
    """
    Recursively filter out SkipNull objects from 'value'.

    :param name: Name of port producing this value.
                 Only used when we find an unhandled null from a conditional step
                 and we print out a warning. The name allows the user to better
                 localize which step/port was responsible for the unhandled null.
    :param value: port output value object
    """
    err_flag = [False]
    value = _filter_skip_null(value, err_flag)
    if err_flag[0]:
        logger.warning(
            f"In {name}, SkipNull result found and cast to None. \n"
            "You had a conditional step that did not run, "
            "but you did not use pickValue to handle the skipped input."
        )
    return value


def _filter_skip_null(value: Any, err_flag: list[bool]) -> Any:
    """
    Private implementation for recursively filtering out SkipNull objects from 'value'.

    :param value: port output value object
    :param err_flag: A pass by reference boolean (passed by enclosing in a list) that
                     allows us to flag, at any level of recursion, that we have
                     encountered a SkipNull.
    """
    if isinstance(value, SkipNull):
        err_flag[0] = True
        value = None
    elif isinstance(value, list):
        return [_filter_skip_null(v, err_flag) for v in value]
    elif isinstance(value, dict):
        return {k: _filter_skip_null(v, err_flag) for k, v in value.items()}
    return value


def ensure_no_collisions(
    directory: DirectoryType, dir_description: Optional[str] = None
) -> None:
    """
    Make sure no items in the given CWL Directory have the same name.

    If any do, raise a WorkflowException about a "File staging conflict".

    Does not recurse into subdirectories.
    """

    if dir_description is None:
        # Work out how to describe the directory we are working on.
        dir_description = f"the directory \"{directory.get('basename')}\""

    seen_names = set()

    for child in directory.get("listing", []):
        if "basename" in child:
            # For each child that actually has a path to go at in its parent
            wanted_name = cast(str, child["basename"])
            if wanted_name in seen_names:
                # We used this name already so bail out
                raise cwl_utils.errors.WorkflowException(
                    f'File staging conflict: Duplicate entries for "{wanted_name}"'
                    f" prevent actually creating {dir_description}"
                )
            seen_names.add(wanted_name)


def try_prepull(
    cwl_tool_uri: str, runtime_context: cwltool.context.RuntimeContext, batchsystem: str
) -> None:
    """
    Try to prepull all containers in a CWL workflow with Singularity or Docker.
    This will not prepull the default container specified on the command line.
    :param cwl_tool_uri: CWL workflow URL. Fragments are accepted as well
    :param runtime_context: runtime context of cwltool
    :param batchsystem: type of Toil batchsystem
    :return:
    """
    if runtime_context.singularity:
        if "CWL_SINGULARITY_CACHE" in os.environ:
            logger.info("Prepulling the workflow's containers with Singularity...")
            call_command(
                [
                    "cwl-docker-extract",
                    "--singularity",
                    "--dir",
                    os.environ["CWL_SINGULARITY_CACHE"],
                    cwl_tool_uri,
                ]
            )
    elif not runtime_context.user_space_docker_cmd and not runtime_context.podman:
        # For udocker and podman prefetching is unimplemented
        # This is docker
        if batchsystem == "single_machine":
            # Only on single machine will the docker daemon be accessible by all workers and the leader
            logger.info("Prepulling the workflow's containers with Docker...")
            call_command(["cwl-docker-extract", cwl_tool_uri])


class Conditional:
    """
    Object holding conditional expression until we are ready to evaluate it.

    Evaluation occurs before the enclosing step's inputs are type-checked.
    """

    def __init__(
        self,
        expression: Optional[str] = None,
        outputs: Union[dict[str, CWLOutputType], None] = None,
        requirements: Optional[list[CWLObjectType]] = None,
        container_engine: str = "docker",
    ):
        """
        Instantiate a conditional expression.

        :param expression: Expression from the 'when' field of the step
        :param outputs: The output dictionary for the step. This is needed because
                        if the step is skipped, all the outputs need to be populated
                        with SkipNull values
        :param requirements: The requirements object that is needed for the context the
                             expression will evaluate in.
        """
        self.expression = expression
        self.outputs = outputs or {}
        self.requirements = requirements or []
        self.container_engine = container_engine

    def is_false(self, job: CWLObjectType) -> bool:
        """
        Determine if expression evaluates to False given completed step inputs.

        :param job: job output object
        :return: bool
        """
        if self.expression is None:
            return False

        expr_is_true = cwl_utils.expression.do_eval(
            self.expression,
            {shortname(k): v for k, v in resolve_dict_w_promises(job).items()},
            self.requirements,
            None,
            None,
            {},
            container_engine=self.container_engine,
        )

        if isinstance(expr_is_true, bool):
            return not expr_is_true

        raise cwl_utils.errors.WorkflowException(
            "'%s' evaluated to a non-boolean value" % self.expression
        )

    def skipped_outputs(self) -> dict[str, SkipNull]:
        """Generate a dict of SkipNull objects corresponding to the output structure."""
        outobj = {}

        def sn(n: Any) -> str:
            if isinstance(n, Mapping):
                return shortname(n["id"])
            if isinstance(n, str):
                return shortname(n)
            return shortname(str(n))

        for k in [sn(o) for o in self.outputs]:
            outobj[k] = SkipNull()

        return outobj


class ResolveSource:
    """Apply linkMerge and pickValue operators to values coming into a port."""

    promise_tuples: Union[list[tuple[str, Promise]], tuple[str, Promise]]

    def __init__(
        self,
        name: str,
        input: dict[str, CWLObjectType],
        source_key: str,
        promises: dict[str, Job],
    ):
        """
        Construct a container object.

        It will carry what information it can about the input sources and the
        current promises, ready for evaluation when the time comes.

        :param name: human readable name of step/port that this value refers to
        :param input: CWL input object complete with linkMerge and pickValue fields
        :param source_key: "source" or "outputSource" depending on what it is
        :param promises: incident values packed as promises
        """
        self.name, self.input, self.source_key = name, input, source_key

        source_names = aslist(self.input[self.source_key])
        # Rule is that source: [foo] is just foo
        #                      unless it also has linkMerge: merge_nested
        if input.get("linkMerge") or len(source_names) > 1:
            self.promise_tuples = [
                (shortname(s), promises[s].rv()) for s in source_names
            ]
        else:
            # KG: Cargo culting this logic and the reason given from original Toil code:
            # It seems that an input source with a
            # '#' in the name will be returned as a
            # CommentedSeq list by the yaml parser.
            s = str(source_names[0])
            self.promise_tuples = (shortname(s), promises[s].rv())

    def __repr__(self) -> str:
        """Allow for debug printing."""

        parts = [f"source key {self.source_key}"]

        if "pickValue" in self.input:
            parts.append(f"pick value {self.input['pickValue']} from")

        if isinstance(self.promise_tuples, list):
            names = [n for n, _ in self.promise_tuples]
            parts.append(f"names {names} in promises")
        else:
            name, _ = self.promise_tuples
            parts.append(f"name {name} in promise")

        return f"ResolveSource({', '.join(parts)})"

    def resolve(self) -> Any:
        """First apply linkMerge then pickValue if either present."""

        result: Optional[Any] = None
        if isinstance(self.promise_tuples, list):
            result = self.link_merge(
                cast(
                    CWLObjectType, [rv[name] for name, rv in self.promise_tuples]  # type: ignore[index]
                )
            )
        else:
            name, rv = self.promise_tuples
            result = cast(dict[str, Any], rv).get(name)

        result = self.pick_value(result)
        result = filter_skip_null(self.name, result)
        return result

    def link_merge(
        self, values: CWLObjectType
    ) -> Union[list[CWLOutputType], CWLOutputType]:
        """
        Apply linkMerge operator to `values` object.

        :param values: result of step
        """

        link_merge_type = self.input.get("linkMerge", "merge_nested")

        if link_merge_type == "merge_nested":
            return values

        elif link_merge_type == "merge_flattened":
            result: list[CWLOutputType] = []
            for v in values:
                if isinstance(v, MutableSequence):
                    result.extend(v)
                else:
                    result.append(v)
            return result

        else:
            raise ValidationException(
                f"Unsupported linkMerge '{link_merge_type}' on {self.name}."
            )

    def pick_value(self, values: Union[list[Union[str, SkipNull]], Any]) -> Any:
        """
        Apply pickValue operator to `values` object.

        :param values: Intended to be a list, but other types will be returned
                       without modification.
        :return:
        """

        pick_value_type = cast(str, self.input.get("pickValue"))

        if pick_value_type is None:
            return values

        if isinstance(values, SkipNull):
            return None

        if not isinstance(values, list):
            logger.warning("pickValue used but input %s is not a list." % self.name)
            return values

        result = [v for v in values if not isinstance(v, SkipNull) and v is not None]

        if pick_value_type == "first_non_null":
            if len(result) < 1:
                logger.error(
                    "Could not find non-null entry for %s:\n%s",
                    self.name,
                    pprint.pformat(self.promise_tuples),
                )
                raise cwl_utils.errors.WorkflowException(
                    "%s: first_non_null operator found no non-null values" % self.name
                )
            else:
                return result[0]

        elif pick_value_type == "the_only_non_null":
            if len(result) == 0:
                raise cwl_utils.errors.WorkflowException(
                    "%s: the_only_non_null operator found no non-null values"
                    % self.name
                )
            elif len(result) > 1:
                raise cwl_utils.errors.WorkflowException(
                    "%s: the_only_non_null operator found more than one non-null values"
                    % self.name
                )
            else:
                return result[0]

        elif pick_value_type == "all_non_null":
            return result

        else:
            raise cwl_utils.errors.WorkflowException(
                f"Unsupported pickValue '{pick_value_type}' on {self.name}"
            )


class StepValueFrom:
    """
    A workflow step input which has a valueFrom expression attached to it.

    The valueFrom expression will be evaluated to produce the actual input
    object for the step.
    """

    def __init__(
        self, expr: str, source: Any, req: list[CWLObjectType], container_engine: str
    ):
        """
        Instantiate an object to carry all know about this valueFrom expression.

        :param expr: str: expression as a string
        :param source: the source promise of this step
        :param req: requirements object that is consumed by CWLtool expression evaluator
        :param container_engine: which container engine to use to load nodejs, if needed
        """
        self.expr = expr
        self.source = source
        self.context = None
        self.req = req
        self.container_engine = container_engine

    def __repr__(self) -> str:
        """Allow for debug printing."""

        return f"StepValueFrom({self.expr}, {self.source}, {self.req}, {self.container_engine})"

    def eval_prep(
        self, step_inputs: CWLObjectType, file_store: AbstractFileStore
    ) -> None:
        """
        Resolve the contents of any file in a set of inputs.

        The inputs must be associated with the StepValueFrom object's self.source.

        Called when loadContents is specified.

        :param step_inputs: Workflow step inputs.
        :param file_store: A toil file store, needed to resolve toilfile:// paths.
        """
        for v in step_inputs.values():
            val = cast(CWLObjectType, v)
            source_input = getattr(self.source, "input", {})
            if isinstance(val, dict) and isinstance(source_input, dict):
                if (
                    val.get("contents") is None
                    and source_input.get("loadContents") is True
                ):
                    # This is safe to use even if we're bypassing the file
                    # store for the workflow. In that case, no toilfile:// or
                    # other special URIs will exist in the workflow to be read
                    # from, and ToilFsAccess still supports file:// URIs.
                    fs_access = functools.partial(ToilFsAccess, file_store=file_store)
                    with fs_access("").open(cast(str, val["location"]), "rb") as f:
                        val["contents"] = cwltool.builder.content_limit_respected_read(
                            f
                        )

    def resolve(self) -> Any:
        """
        Resolve the promise in the valueFrom expression's context.

        :return: object that will serve as expression context
        """
        self.context = self.source.resolve()
        return self.context

    def do_eval(self, inputs: CWLObjectType) -> Any:
        """
        Evaluate the valueFrom expression with the given input object.

        :param inputs:
        :return: object
        """
        return cwl_utils.expression.do_eval(
            self.expr,
            inputs,
            self.req,
            None,
            None,
            {},
            context=self.context,
            container_engine=self.container_engine,
        )


class DefaultWithSource:
    """A workflow step input that has both a source and a default value."""

    def __init__(self, default: Any, source: Any):
        """
        Instantiate an object to handle a source that has a default value.

        :param default: the default value
        :param source: the source object
        """
        self.default = default
        self.source = source

    def __repr__(self) -> str:
        """Allow for debug printing."""

        return f"DefaultWithSource({self.default}, {self.source})"

    def resolve(self) -> Any:
        """
        Determine the final input value when the time is right.

        (when the source can be resolved)

        :return: dict
        """
        if self.source:
            result = self.source.resolve()
            if result is not None:
                return result
        return self.default


class JustAValue:
    """A simple value masquerading as a 'resolve'-able object."""

    def __init__(self, val: Any):
        """Store the value."""
        self.val = val

    def __repr__(self) -> str:
        """Allow for debug printing."""

        return f"JustAValue({self.val})"

    def resolve(self) -> Any:
        """Return the value."""
        return self.val


def resolve_dict_w_promises(
    dict_w_promises: Union[
        UnresolvedDict, CWLObjectType, dict[str, Union[str, StepValueFrom]]
    ],
    file_store: Optional[AbstractFileStore] = None,
) -> CWLObjectType:
    """
    Resolve a dictionary of promises evaluate expressions to produce the actual values.

    :param dict_w_promises: input dict for these values
    :return: dictionary of actual values
    """
    if isinstance(dict_w_promises, UnresolvedDict):
        first_pass_results: CWLObjectType = {
            k: v.resolve() for k, v in dict_w_promises.items()
        }
    else:
        first_pass_results = cast(
            CWLObjectType, {k: v for k, v in dict_w_promises.items()}
        )

    result: CWLObjectType = {}
    for k, v in dict_w_promises.items():
        if isinstance(v, StepValueFrom):
            if file_store:
                v.eval_prep(first_pass_results, file_store)
            result[k] = v.do_eval(inputs=first_pass_results)
        else:
            result[k] = first_pass_results[k]

    return result


def simplify_list(maybe_list: Any) -> Any:
    """
    Turn a length one list loaded by cwltool into a scalar.

    Anything else is passed as-is, by reference.
    """
    if isinstance(maybe_list, MutableSequence):
        is_list = aslist(maybe_list)
        if len(is_list) == 1:
            return is_list[0]
    return maybe_list


class ToilPathMapper(PathMapper):
    """
    Keeps track of files in a Toil way.

    Maps between the symbolic identifier of a file (the Toil FileID), its local
    path on the host (the value returned by readGlobalFile) and the
    location of the file inside the software container.
    """

    def __init__(
        self,
        referenced_files: list[CWLObjectType],
        basedir: str,
        stagedir: str,
        separateDirs: bool = True,
        get_file: Union[Any, None] = None,
        stage_listing: bool = False,
        streaming_allowed: bool = True,
    ):
        """
        Initialize this ToilPathMapper.

        :param stage_listing: Stage files and directories inside directories
               even if we also stage the parent.
        :param get_file: A function that takes a URL, an optional "streamable"
               flag for if a file is supposed to be streamable, and an optional
               "streaming_allowed" flag for whether we are running with
               streaming on, and returns a file: URI to where the file or
               directory has been downloaded to. Meant to be a partially-bound
               version of toil_get_file().
        :param referenced_files: List of CWL File and Directory objects, which can have their locations set as both
               virtualized and absolute local paths
        """
        self.get_file = get_file
        self.stage_listing = stage_listing
        self.streaming_allowed = streaming_allowed

        super().__init__(referenced_files, basedir, stagedir, separateDirs=separateDirs)

    def visit(
        self,
        obj: CWLObjectType,
        stagedir: str,
        basedir: str,
        copy: bool = False,
        staged: bool = False,
    ) -> None:
        """
        Iterate over a CWL object, resolving File and Directory path references.

        This is called on each File or Directory CWL object. The Files and
        Directories all have "location" fields. For the Files, these are from
        upload_file(), and for the Directories, these are from
        upload_directory() or cwltool internally. With upload_directory(), they and their children will be assigned
        locations based on listing the Directories using ToilFsAccess. With cwltool, locations will be set as absolute
        paths.

        :param obj: The CWL File or Directory to process

        :param stagedir: The base path for target paths to be generated under,
            except when a File or Directory has an overriding parent directory in
            dirname

        :param basedir: The directory from which relative paths should be
            resolved; used as the base directory for the StdFsAccess that generated
            the listing being processed.

        :param copy: If set, use writable types for Files and Directories.

        :param staged: Starts as True at the top of the recursion. Set to False
            when entering a directory that we can actually download, so we don't
            stage files and subdirectories separately from the directory as a
            whole. Controls the staged flag on generated mappings, and therefore
            whether files and directories are actually placed at their mapped-to
            target locations. If stage_listing is True, we will leave this True
            throughout and stage everything.

        Produces one MapperEnt for every unique location for a File or
        Directory. These MapperEnt objects are instructions to cwltool's
        stage_files function:
        https://github.com/common-workflow-language/cwltool/blob/a3e3a5720f7b0131fa4f9c0b3f73b62a347278a6/cwltool/process.py#L254

        The MapperEnt has fields:

        resolved: An absolute local path anywhere on the filesystem where the
        file/directory can be found, or the contents of a file to populate it
        with if type is CreateWritableFile or CreateFile. Or, a URI understood
        by the StdFsAccess in use (for example, toilfile:).

        target: An absolute path under stagedir that the file or directory will
        then be placed at by cwltool. Except if a File or Directory has a
        dirname field, giving its parent path, that is used instead.

        type: One of:

            File: cwltool will copy or link the file from resolved to target,
            if possible.

            CreateFile: cwltool will create the file at target, treating
            resolved as the contents.

            WritableFile: cwltool will copy the file from resolved to target,
            making it writable.

            CreateWritableFile: cwltool will create the file at target,
            treating resolved as the contents, and make it writable.

            Directory: cwltool will copy or link the directory from resolved to
            target, if possible. Otherwise, cwltool will make the directory at
            target if resolved starts with "_:". Otherwise it will do nothing.

            WritableDirectory: cwltool will copy the directory from resolved to
            target, if possible. Otherwise, cwltool will make the directory at
            target if resolved starts with "_:". Otherwise it will do nothing.

        staged: if set to False, cwltool will not make or copy anything for this entry
        """
        logger.debug(
            "ToilPathMapper mapping into %s from %s for: %s", stagedir, basedir, obj
        )

        # If the file has a dirname set, we can try and put it there instead of
        # wherever else we would stage it.
        # TODO: why would we do that?
        stagedir = cast(Optional[str], obj.get("dirname")) or stagedir

        if obj["class"] not in ("File", "Directory"):
            # We only handle files and directories; only they have locations.
            return

        location = cast(str, obj["location"])
        if location in self:
            # If we've already mapped this, map it consistently.
            tgt = self._pathmap[location].target
            logger.debug(
                "ToilPathMapper re-using target %s for path %s",
                tgt,
                location,
            )
        else:
            # Decide where to put the file or directory, as an absolute path.
            tgt = os.path.join(
                stagedir,
                cast(str, obj["basename"]),
            )
            if self.reversemap(tgt) is not None:
                # If the target already exists in the pathmap, but we haven't yet
                # mapped this, it means we have a conflict.
                i = 2
                new_tgt = f"{tgt}_{i}"
                while self.reversemap(new_tgt) is not None:
                    i += 1
                    new_tgt = f"{tgt}_{i}"
                logger.debug(
                    "ToilPathMapper resolving mapping conflict: %s is now %s",
                    tgt,
                    new_tgt,
                )
                tgt = new_tgt

        if obj["class"] == "Directory":
            # Whether or not we've already mapped this path, we need to map all
            # children recursively.

            logger.debug("ToilPathMapper visiting directory %s", location)

            # We want to check the directory to make sure it is not
            # self-contradictory in its immediate children and their names.
            ensure_no_collisions(cast(DirectoryType, obj))

            # We may need to copy this directory even if we don't copy things inside it.
            copy_here = False

            # Try and resolve the location to a local path
            if location.startswith("file://"):
                # This is still from the local machine, so go find where it is
                resolved = schema_salad.ref_resolver.uri_file_path(location)
            elif location.startswith("toildir:"):
                # We need to download this directory (or subdirectory)
                if self.get_file:
                    # We can actually go get it and its contents
                    resolved = schema_salad.ref_resolver.uri_file_path(
                        self.get_file(location)
                    )
                else:
                    # We are probably staging final outputs on the leader. We
                    # can't go get the directory. Just pass it through.
                    resolved = location
            elif location.startswith("_:"):
                # cwltool made this up for an empty/synthetic directory it
                # wants to make.

                # If we let cwltool make the directory and stage it, and then
                # stage files inside it, we can end up with Docker creating
                # root-owned files in whatever we mounted for the Docker work
                # directory, somehow. So make a directory ourselves instead.
                if self.get_file:
                    # Ask for an empty directory
                    new_dir_uri = self.get_file("_:")
                    # And get a path for it
                    resolved = schema_salad.ref_resolver.uri_file_path(new_dir_uri)

                    if "listing" in obj and obj["listing"] != []:
                        # If there's stuff inside here to stage, we need to copy
                        # this directory here, because we can't Docker mount things
                        # over top of immutable directories.
                        copy_here = True
                else:
                    # We can't really make the directory. Maybe we are
                    # exporting from the leader and it doesn't matter.
                    resolved = location
            elif location.startswith("/"):
                # Test if path is an absolute local path
                # Does not check if the path is relative
                # While Toil encodes paths into a URL with ToilPathMapper,
                # something called internally in cwltool may return an absolute path
                # ex: if cwltool calls itself internally in command_line_tool.py,
                # it collects outputs with collect_output, and revmap_file will use its own internal pathmapper
                resolved = location
            else:
                raise RuntimeError("Unsupported location: " + location)

            if location in self._pathmap:
                # Don't map the same directory twice
                logger.debug(
                    "ToilPathMapper stopping recursion because we have already "
                    "mapped directory: %s",
                    location,
                )
                return

            logger.debug(
                "ToilPathMapper adding directory mapping %s -> %s", resolved, tgt
            )
            self._pathmap[location] = MapperEnt(
                resolved,
                tgt,
                "WritableDirectory" if (copy or copy_here) else "Directory",
                staged,
            )

            if not location.startswith("_:") and not self.stage_listing:
                # Don't stage anything below here separately, since we are able
                # to copy the whole directory from somewhere and and we can't
                # stage files over themselves.
                staged = False

            # Keep recursing
            self.visitlisting(
                cast(list[CWLObjectType], obj.get("listing", [])),
                tgt,
                basedir,
                copy=copy,
                staged=staged,
            )

        elif obj["class"] == "File":
            logger.debug("ToilPathMapper visiting file %s", location)

            if location in self._pathmap:
                # Don't map the same file twice
                logger.debug(
                    "ToilPathMapper stopping recursion because we have already "
                    "mapped file: %s",
                    location,
                )
                return

            ab = abspath(location, basedir)
            if "contents" in obj and location.startswith("_:"):
                # We are supposed to create this file
                self._pathmap[location] = MapperEnt(
                    cast(str, obj["contents"]),
                    tgt,
                    "CreateWritableFile" if copy else "CreateFile",
                    staged,
                )
            else:
                with SourceLine(
                    obj,
                    "location",
                    ValidationException,
                    logger.isEnabledFor(logging.DEBUG),
                ):
                    # If we have access to the Toil file store, we will have a
                    # get_file set, and it will convert this path to a file:
                    # URI for a local file it downloaded.
                    if self.get_file:
                        deref = self.get_file(
                            location,
                            obj.get("streamable", False),
                            self.streaming_allowed,
                        )
                    else:
                        deref = ab
                    if deref.startswith("file:"):
                        deref = schema_salad.ref_resolver.uri_file_path(deref)
                    if urlsplit(deref).scheme in ["http", "https"]:
                        deref = downloadHttpFile(location)
                    elif urlsplit(deref).scheme != "toilfile":
                        # Dereference symbolic links
                        st = os.lstat(deref)
                        while stat.S_ISLNK(st.st_mode):
                            logger.debug("ToilPathMapper following symlink %s", deref)
                            rl = os.readlink(deref)
                            deref = (
                                rl
                                if os.path.isabs(rl)
                                else os.path.join(os.path.dirname(deref), rl)
                            )
                            st = os.lstat(deref)

                    # If we didn't download something that is a toilfile:
                    # reference, we just pass that along.

                    """Link or copy files to their targets. Create them as needed."""

                    logger.debug(
                        "ToilPathMapper adding file mapping %s -> %s", deref, tgt
                    )

                    self._pathmap[location] = MapperEnt(
                        deref, tgt, "WritableFile" if copy else "File", staged
                    )

            # Handle all secondary files that need to be next to this one.
            self.visitlisting(
                cast(list[CWLObjectType], obj.get("secondaryFiles", [])),
                stagedir,
                basedir,
                copy=copy,
                staged=staged,
            )


class ToilSingleJobExecutor(cwltool.executors.SingleJobExecutor):
    """
    A SingleJobExecutor that does not assume it is at the top level of the workflow.

    We need this because otherwise every job thinks it is top level and tries
    to discover secondary files, which may exist when they haven't actually
    been passed at the top level and thus aren't supposed to be visible.
    """

    def run_jobs(
        self,
        process: Process,
        job_order_object: CWLObjectType,
        logger: logging.Logger,
        runtime_context: cwltool.context.RuntimeContext,
    ) -> None:
        """run_jobs from SingleJobExecutor, but not in a top level runtime context."""
        runtime_context.toplevel = False
        if isinstance(
            process, cwltool.command_line_tool.CommandLineTool
        ) and isinstance(
            process.make_job_runner(runtime_context), SingularityCommandLineJob
        ):
            # Set defaults for singularity cache environment variables, similar to what we do in wdltoil
            # Use the same place as the default singularity cache directory
            singularity_cache = os.path.join(os.path.expanduser("~"), ".singularity")
            os.environ["SINGULARITY_CACHEDIR"] = os.environ.get(
                "SINGULARITY_CACHEDIR", singularity_cache
            )

            # If singularity is detected, prepull the image to ensure locking
            (docker_req, docker_is_req) = process.get_requirement(
                feature="DockerRequirement"
            )
            with global_mutex(
                os.environ["SINGULARITY_CACHEDIR"], "toil_singularity_cache_mutex"
            ):
                SingularityCommandLineJob.get_image(
                    dockerRequirement=cast(dict[str, str], docker_req),
                    pull_image=runtime_context.pull_image,
                    force_pull=runtime_context.force_docker_pull,
                    tmp_outdir_prefix=runtime_context.tmp_outdir_prefix,
                )

        return super().run_jobs(process, job_order_object, logger, runtime_context)


class ToilTool:
    """Mixin to hook Toil into a cwltool tool type."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Init hook to set up member variables.
        """
        super().__init__(*args, **kwargs)
        # Reserve a spot for the Toil job that ends up executing this tool.
        self._toil_job: Optional[Job] = None
        # Remember path mappers we have used so we can interrogate them later to find out what the job mapped.
        self._path_mappers: list[cwltool.pathmapper.PathMapper] = []

    def connect_toil_job(self, job: Job) -> None:
        """
        Attach the Toil tool to the Toil job that is executing it. This allows
        it to use the Toil job to stop at certain points if debugging flags are
        set.
        """
        self._toil_job = job

    def make_path_mapper(
        self,
        reffiles: list[Any],
        stagedir: str,
        runtimeContext: cwltool.context.RuntimeContext,
        separateDirs: bool,
    ) -> cwltool.pathmapper.PathMapper:
        """Create the appropriate PathMapper for the situation."""
        if getattr(runtimeContext, "bypass_file_store", False):
            # We only need to understand cwltool's supported URIs
            mapper = PathMapper(
                reffiles, runtimeContext.basedir, stagedir, separateDirs=separateDirs
            )
        else:
            # We need to be able to read from Toil-provided URIs
            mapper = ToilPathMapper(
                reffiles,
                runtimeContext.basedir,
                stagedir,
                separateDirs,
                get_file=getattr(runtimeContext, "toil_get_file", None),
                streaming_allowed=runtimeContext.streaming_allowed,
            )

        # Remember the path mappers
        self._path_mappers.append(mapper)
        return mapper

    def __str__(self) -> str:
        """Return string representation of this tool type."""
        return f'{self.__class__.__name__}({repr(getattr(self, "tool", {}).get("id", "???"))})'


class ToilCommandLineTool(ToilTool, cwltool.command_line_tool.CommandLineTool):
    """Subclass the cwltool command line tool to provide the custom ToilPathMapper."""

    def _initialworkdir(
        self, j: cwltool.job.JobBase, builder: cwltool.builder.Builder
    ) -> None:
        """
        Hook the InitialWorkDirRequirement setup to make sure that there are no
        name conflicts at the top level of the work directory.
        """

        # Set up the initial work dir with all its files
        super()._initialworkdir(j, builder)

        # The initial work dir listing is now in j.generatefiles["listing"]
        # Also j.generatefiles is a CWL Directory.
        # So check the initial working directory.
        logger.debug("Initial work dir: %s", j.generatefiles)
        ensure_no_collisions(
            j.generatefiles,
            "the job's working directory as specified by the InitialWorkDirRequirement",
        )

        if self._toil_job is not None:
            # Make a table of all the places we mapped files to when downloading the inputs.

            # We want to hint which host paths and container (if any) paths correspond
            host_and_job_paths: list[tuple[str, str]] = []

            for pm in self._path_mappers:
                for _, mapper_entry in pm.items_exclude_children():
                    # We know that mapper_entry.target as seen by the task is
                    # mapper_entry.resolved on the host.
                    host_and_job_paths.append(
                        (mapper_entry.resolved, mapper_entry.target)
                    )

            # Notice that we have downloaded our inputs. Explain which files
            # those are here and what the task will expect to call them.
            self._toil_job.files_downloaded_hook(host_and_job_paths)


class ToilExpressionTool(ToilTool, cwltool.command_line_tool.ExpressionTool):
    """Subclass the cwltool expression tool to provide the custom ToilPathMapper."""


def toil_make_tool(
    toolpath_object: CommentedMap,
    loadingContext: cwltool.context.LoadingContext,
) -> Process:
    """
    Emit custom ToilCommandLineTools.

    This factory function is meant to be passed to cwltool.load_tool().
    """
    if isinstance(toolpath_object, Mapping):
        if toolpath_object.get("class") == "CommandLineTool":
            return ToilCommandLineTool(toolpath_object, loadingContext)
        elif toolpath_object.get("class") == "ExpressionTool":
            return ToilExpressionTool(toolpath_object, loadingContext)
    return cwltool.workflow.default_make_tool(toolpath_object, loadingContext)


# When a file we want to have is missing, we can give it this sentinel location
# URI instead of raising an error right away, in case it is optional.
MISSING_FILE = "missing://"

DirectoryContents = dict[str, Union[str, "DirectoryContents"]]


def check_directory_dict_invariants(contents: DirectoryContents) -> None:
    """
    Make sure a directory structure dict makes sense. Throws an error
    otherwise.

    Currently just checks to make sure no empty-string keys exist.
    """

    for name, item in contents.items():
        if name == "":
            raise RuntimeError(
                "Found nameless entry in directory: " + json.dumps(contents, indent=2)
            )
        if isinstance(item, dict):
            check_directory_dict_invariants(item)


def decode_directory(
    dir_path: str,
) -> tuple[DirectoryContents, Optional[str], str]:
    """
    Decode a directory from a "toildir:" path to a directory (or a file in it).

    Returns the decoded directory dict, the remaining part of the path (which may be
    None), and the deduplication key string that uniquely identifies the
    directory.
    """
    if not dir_path.startswith("toildir:"):
        raise RuntimeError(f"Cannot decode non-directory path: {dir_path}")

    # We will decode the directory and then look inside it

    # Since this was encoded by upload_directory we know the
    # next piece is encoded JSON describing the directory structure,
    # and it can't contain any slashes.
    parts = dir_path[len("toildir:") :].split("/", 1)

    # Before the first slash is the encoded data describing the directory contents
    dir_data = parts[0]

    # Decode what to download
    contents = json.loads(
        base64.urlsafe_b64decode(dir_data.encode("utf-8")).decode("utf-8")
    )

    check_directory_dict_invariants(contents)

    if len(parts) == 1 or parts[1] == "/":
        # We didn't have any subdirectory
        return contents, None, dir_data
    else:
        # We have a path below this
        return contents, parts[1], dir_data


def encode_directory(contents: DirectoryContents) -> str:
    """
    Encode a directory from a "toildir:" path to a directory (or a file in it).

    Takes the directory dict, which is a dict from name to URI for a file or
    dict for a subdirectory.
    """

    check_directory_dict_invariants(contents)

    return "toildir:" + base64.urlsafe_b64encode(
        json.dumps(contents).encode("utf-8")
    ).decode("utf-8")


class ToilFsAccess(StdFsAccess):
    """
    Custom filesystem access class which handles toil filestore references.

    Normal file paths will be resolved relative to basedir, but 'toilfile:' and
    'toildir:' URIs will be fulfilled from the Toil file store.

    Also supports URLs supported by Toil job store implementations.
    """

    def __init__(
        self,
        basedir: str,
        file_store: Optional[AbstractFileStore] = None,
    ) -> None:
        """Create a FsAccess object for the given Toil Filestore and basedir."""
        self.file_store = file_store

        # Map encoded directory structures to where we downloaded them, so we
        # don't constantly redownload them.
        # Assumes nobody will touch our files via realpath, or that if they do
        # they know what will happen.
        # Also maps files and directories from external URLs to downloaded
        # locations.
        self.dir_to_download: dict[str, str] = {}

        super().__init__(basedir)

    def _abs(self, path: str) -> str:
        """
        Return a local absolute path for a file (no schema).

        Overwrites StdFsAccess._abs() to account for toil specific schema.
        """
        # TODO: Both we and the ToilPathMapper relate Toil paths to local
        # paths. But we don't share the same mapping, so accesses through
        # different mechanisms will produce different local copies.

        # Used to fetch a path to determine if a file exists in the inherited
        # StdFsAccess, (among other things) so this should not error on missing
        # files.
        # See: https://github.com/common-workflow-language/cwltool/blob/beab66d649dd3ee82a013322a5e830875e8556ba/cwltool/stdfsaccess.py#L43  # noqa B950

        parse = urlparse(path)
        if parse.scheme == "toilfile":
            # Is a Toil file

            if self.file_store is None:
                raise RuntimeError("URL requires a file store: " + path)

            destination = self.file_store.readGlobalFile(
                FileID.unpack(path[len("toilfile:") :]), symlink=True
            )
            logger.debug("Downloaded %s to %s", path, destination)
            if not os.path.exists(destination):
                raise RuntimeError(
                    f"{destination} does not exist after filestore read."
                )
        elif parse.scheme == "toildir":
            # Is a directory or relative to it

            if self.file_store is None:
                raise RuntimeError("URL requires a file store: " + path)

            # We will download the whole directory and then look inside it

            # Decode its contents, the path inside it to the file (if any), and
            # the key to use for caching the directory.
            contents, subpath, cache_key = decode_directory(path)
            logger.debug("Decoded directory contents: %s", contents)

            if cache_key not in self.dir_to_download:
                # Download to a temp directory.
                temp_dir = self.file_store.getLocalTempDir()
                temp_dir += "/toildownload"
                os.makedirs(temp_dir)

                logger.debug("ToilFsAccess downloading %s to %s", cache_key, temp_dir)

                # Save it all into this new temp directory.
                # Guaranteed to fill it with real files and not symlinks.
                download_structure(self.file_store, {}, {}, contents, temp_dir)

                # Make sure we use the same temp directory if we go traversing
                # around this thing.
                self.dir_to_download[cache_key] = temp_dir
            else:
                logger.debug("ToilFsAccess already has %s", cache_key)

            if subpath is None:
                # We didn't have any subdirectory, so just give back
                # the path to the root
                destination = self.dir_to_download[cache_key]
            else:
                # Navigate to the right subdirectory
                destination = self.dir_to_download[cache_key] + "/" + subpath
        elif parse.scheme == "file":
            # This is a File URL. Decode it to an actual path.
            destination = unquote(parse.path)
        elif parse.scheme == "":
            # This is just a local file and not a URL
            destination = path
        else:
            # The destination is something else.
            if AbstractJobStore.get_is_directory(path):
                # Treat this as a directory
                if path not in self.dir_to_download:
                    logger.debug(
                        "ToilFsAccess fetching directory %s from a JobStore", path
                    )
                    dest_dir = mkdtemp()

                    # Recursively fetch all the files in the directory.
                    def download_to(url: str, dest: str) -> None:
                        if AbstractJobStore.get_is_directory(url):
                            os.mkdir(dest)
                            for part in AbstractJobStore.list_url(url):
                                download_to(
                                    os.path.join(url, part), os.path.join(dest, part)
                                )
                        else:
                            AbstractJobStore.read_from_url(url, open(dest, "wb"))

                    download_to(path, dest_dir)
                    self.dir_to_download[path] = dest_dir

                destination = self.dir_to_download[path]
            else:
                # Treat this as a file.
                if path not in self.dir_to_download:
                    logger.debug("ToilFsAccess fetching file %s from a JobStore", path)
                    # Try to grab it with a jobstore implementation, and save it
                    # somewhere arbitrary.
                    dest_file = NamedTemporaryFile(delete=False)
                    AbstractJobStore.read_from_url(path, dest_file)
                    dest_file.close()
                    self.dir_to_download[path] = dest_file.name
                destination = self.dir_to_download[path]
            logger.debug(
                "ToilFsAccess has JobStore-supported URL %s at %s", path, destination
            )

        # Now destination is a local file, so make sure we really do have an
        # absolute path
        destination = super()._abs(destination)
        return destination

    def glob(self, pattern: str) -> list[str]:
        parse = urlparse(pattern)
        if parse.scheme == "file":
            pattern = os.path.abspath(unquote(parse.path))
        elif parse.scheme == "":
            pattern = os.path.abspath(pattern)
        else:
            raise RuntimeError(
                f"Cannot efficiently support globbing on {parse.scheme} URIs"
            )

        # Actually do the glob
        return [schema_salad.ref_resolver.file_uri(f) for f in glob.glob(pattern)]

    def open(self, fn: str, mode: str) -> IO[Any]:
        if "w" in mode or "x" in mode or "+" in mode or "a" in mode:
            raise RuntimeError(f"Mode {mode} for opening {fn} involves writing")

        parse = urlparse(fn)
        if parse.scheme in ["", "file"]:
            # Handle local files
            return open(self._abs(fn), mode)
        elif parse.scheme == "toildir":
            contents, subpath, cache_key = decode_directory(fn)
            if cache_key in self.dir_to_download:
                # This is already available locally, so fall back on the local copy
                return open(self._abs(fn), mode)
            else:
                # We need to get the URI out of the virtual directory
                if subpath is None:
                    raise RuntimeError(f"{fn} is a toildir directory")
                uri = get_from_structure(contents, subpath)
                if not isinstance(uri, str):
                    raise RuntimeError(f"{fn} does not point to a file")
                # Recurse on that URI
                return self.open(uri, mode)
        elif parse.scheme == "toilfile":
            if self.file_store is None:
                raise RuntimeError("URL requires a file store: " + fn)
            # Streaming access to Toil file store files requires being inside a
            # context manager, which we can't require. So we need to download
            # the file.
            return open(self._abs(fn), mode)
        else:
            # This should be supported by a job store.
            byte_stream = AbstractJobStore.open_url(fn)
            if "b" in mode:
                # Pass stream along in binary
                return byte_stream
            else:
                # Wrap it in a text decoder
                return io.TextIOWrapper(byte_stream, encoding="utf-8")

    def exists(self, path: str) -> bool:
        """Test for file existence."""
        parse = urlparse(path)
        if parse.scheme in ["", "file"]:
            # Handle local files
            # toil's _abs() throws errors when files are not found and cwltool's _abs() does not
            try:
                return os.path.exists(self._abs(path))
            except NoSuchFileException:
                return False
        elif parse.scheme == "toildir":
            contents, subpath, cache_key = decode_directory(path)
            if subpath is None:
                # The toildir directory itself exists
                return True
            uri = get_from_structure(contents, subpath)
            if uri is None:
                # It's not in the virtual directory, so it doesn't exist
                return False
            if isinstance(uri, dict):
                # Actually it's a subdirectory, so it exists.
                return True
            # We recurse and poll the URI directly to make sure it really exists
            return self.exists(uri)
        elif parse.scheme == "toilfile":
            # TODO: we assume CWL can't call deleteGlobalFile and so the file always exists
            return True
        else:
            # This should be supported by a job store.
            return AbstractJobStore.url_exists(path)

    def size(self, path: str) -> int:
        parse = urlparse(path)
        if parse.scheme in ["", "file"]:
            return os.stat(self._abs(path)).st_size
        elif parse.scheme == "toildir":
            # Decode its contents, the path inside it to the file (if any), and
            # the key to use for caching the directory.
            contents, subpath, cache_key = decode_directory(path)

            # We can't get the size of just a directory.
            if subpath is None:
                raise RuntimeError(f"Attempted to check size of directory {path}")

            uri = get_from_structure(contents, subpath)

            # We ought to end up with a URI.
            if not isinstance(uri, str):
                raise RuntimeError(f"Did not find a file at {path}")
            return self.size(uri)
        elif parse.scheme == "toilfile":
            if self.file_store is None:
                raise RuntimeError("URL requires a file store: " + path)
            return self.file_store.getGlobalFileSize(
                FileID.unpack(path[len("toilfile:") :])
            )
        else:
            # This should be supported by a job store.
            size = AbstractJobStore.get_size(path)
            if size is None:
                # get_size can be unimplemented or unavailable
                raise RuntimeError(f"Could not get size of {path}")
            return size

    def isfile(self, fn: str) -> bool:
        parse = urlparse(fn)
        if parse.scheme in ["file", ""]:
            return os.path.isfile(self._abs(fn))
        elif parse.scheme == "toilfile":
            # TODO: we assume CWL can't call deleteGlobalFile and so the file always exists
            return True
        elif parse.scheme == "toildir":
            contents, subpath, cache_key = decode_directory(fn)
            if subpath is None:
                # This is the toildir directory itself
                return False
            found = get_from_structure(contents, subpath)
            # If we find a string, that's a file
            # TODO: we assume CWL can't call deleteGlobalFile and so the file always exists
            return isinstance(found, str)
        else:
            return self.exists(fn) and not AbstractJobStore.get_is_directory(fn)

    def isdir(self, fn: str) -> bool:
        logger.debug("ToilFsAccess checking type of %s", fn)
        parse = urlparse(fn)
        if parse.scheme in ["file", ""]:
            return os.path.isdir(self._abs(fn))
        elif parse.scheme == "toilfile":
            return False
        elif parse.scheme == "toildir":
            contents, subpath, cache_key = decode_directory(fn)
            if subpath is None:
                # This is the toildir directory itself.
                # TODO: We assume directories can't be deleted.
                return True
            found = get_from_structure(contents, subpath)
            # If we find a dict, that's a directory.
            # TODO: We assume directories can't be deleted.
            return isinstance(found, dict)
        else:
            status = AbstractJobStore.get_is_directory(fn)
            logger.debug("AbstractJobStore said: %s", status)
            return status

    def listdir(self, fn: str) -> list[str]:
        # This needs to return full URLs for everything in the directory.
        # URLs are not allowed to end in '/', even for subdirectories.
        logger.debug("ToilFsAccess listing %s", fn)

        parse = urlparse(fn)
        if parse.scheme in ["file", ""]:
            # Find the local path
            directory = self._abs(fn)
            # Now list it (it is probably a directory)
            return [abspath(quote(entry), fn) for entry in os.listdir(directory)]
        elif parse.scheme == "toilfile":
            raise RuntimeError(f"Cannot list a file: {fn}")
        elif parse.scheme == "toildir":
            contents, subpath, cache_key = decode_directory(fn)
            here = contents
            if subpath is not None:
                got = get_from_structure(contents, subpath)
                if got is None:
                    raise RuntimeError(f"Cannot list nonexistent directory: {fn}")
                if isinstance(got, str):
                    raise RuntimeError(
                        f"Cannot list file or dubdirectory of a file: {fn}"
                    )
                here = got
            # List all the things in here and make full URIs to them
            return [os.path.join(fn, k) for k in here.keys()]
        else:
            return [
                os.path.join(fn, entry.rstrip("/"))
                for entry in AbstractJobStore.list_url(fn)
            ]

    def join(self, path: str, *paths: str) -> str:
        # This falls back on os.path.join
        return super().join(path, *paths)

    def realpath(self, fn: str) -> str:
        # This also needs to be able to handle URLs, when cwltool uses it in
        # relocateOutputs and some of the outputs didn't come from a local file
        # but were passed through from inputs.
        return super().realpath(self._abs(fn))


def toil_get_file(
    file_store: AbstractFileStore,
    index: dict[str, str],
    existing: dict[str, str],
    uri: str,
    streamable: bool = False,
    streaming_allowed: bool = True,
    pipe_threads: Optional[list[tuple[Thread, int]]] = None,
) -> str:
    """
    Set up the given file or directory from the Toil jobstore at a file URI
    where it can be accessed locally.

    Run as part of the tool setup, inside jobs on the workers.
    Also used as part of reorganizing files to get them uploaded at the end of
    a tool.

    :param file_store: The Toil file store to download from.

    :param index: Maps from downloaded file path back to input Toil URI.

    :param existing: Maps from URI to downloaded file path.

    :param uri: The URI for the file to download.

    :param streamable: If the file is has 'streamable' flag set

    :param streaming_allowed: If streaming is allowed

    :param pipe_threads: List of threads responsible for streaming the data
        and open file descriptors corresponding to those files. Caller is responsible
        to close the file descriptors (to break the pipes) and join the threads
    """
    pipe_threads_real = pipe_threads or []
    # We can't use urlparse here because we need to handle the '_:' scheme and
    # urlparse sees that as a path and not a URI scheme.
    if uri.startswith("toildir:"):
        # This is a file in a directory, or maybe a directory itself.
        # See ToilFsAccess and upload_directory.
        # We will go look for the actual file in the encoded directory
        # structure which will tell us where the toilfile: name for the file is.

        parts = uri[len("toildir:") :].split("/")
        contents = json.loads(
            base64.urlsafe_b64decode(parts[0].encode("utf-8")).decode("utf-8")
        )

        for component in parts[1:]:
            if component != ".":
                # Index into the contents
                contents = contents[component]

        if isinstance(contents, str):
            # This is a reference to a file, so go get it.
            return toil_get_file(file_store, index, existing, contents)
        else:
            # We actually need to fetch the whole directory to a path somewhere.
            dest_path = file_store.getLocalTempDir()
            # Populate the directory
            download_structure(file_store, index, existing, contents, dest_path)
            # Return where we put it, but as a file:// URI
            return schema_salad.ref_resolver.file_uri(dest_path)
    elif uri.startswith("_:"):
        # Someone is asking us for an empty temp directory.
        # We need to check this before the file path case because urlsplit()
        # will call this a path with no scheme.
        dest_path = file_store.getLocalTempDir()
        return schema_salad.ref_resolver.file_uri(dest_path)
    elif uri.startswith("file:") or urlsplit(uri).scheme == "":
        # There's a file: scheme or no scheme, and we know this isn't a _: URL.

        # We need to support file: URIs and local paths, because we might be
        # involved in moving files around on the local disk when uploading
        # things after a job. We might want to catch cases where a leader
        # filesystem file URI leaks in here, but we can't, so we just rely on
        # the rest of the code to be correct.
        return uri
    else:
        # This is a toilfile: uri or other remote URI
        def write_to_pipe(
            file_store: AbstractFileStore, pipe_name: str, uri: str
        ) -> None:
            try:
                with open(pipe_name, "wb") as pipe:
                    if uri.startswith("toilfile:"):
                        # Stream from the file store
                        file_store_id = FileID.unpack(uri[len("toilfile:") :])
                        with file_store.readGlobalFileStream(file_store_id) as fi:
                            chunk_sz = 1024
                            while True:
                                data = fi.read(chunk_sz)
                                if not data:
                                    break
                                pipe.write(data)
                    else:
                        # Stream from some other URI
                        AbstractJobStore.read_from_url(uri, pipe)
            except OSError as e:
                # The other side of the pipe may have been closed by the
                # reading thread, which is OK.
                if e.errno != errno.EPIPE:
                    raise

        if (
            streaming_allowed
            and streamable
            and not isinstance(file_store.jobStore, FileJobStore)
        ):
            logger.debug("Streaming file %s", uri)
            src_path = file_store.getLocalTempFileName()
            os.mkfifo(src_path)
            th = ExceptionalThread(
                target=write_to_pipe,
                args=(
                    file_store,
                    src_path,
                    uri,
                ),
            )
            th.start()
            pipe_threads_real.append((th, os.open(src_path, os.O_RDONLY)))
        else:
            # We need to do a real file
            if uri in existing:
                # Already did it
                src_path = existing[uri]
            else:
                if uri.startswith("toilfile:"):
                    # Download from the file store
                    file_store_id = FileID.unpack(uri[len("toilfile:") :])
                    src_path = file_store.readGlobalFile(file_store_id, symlink=True)
                else:
                    # Download from the URI via the job store.

                    # Figure out where it goes.
                    src_path = file_store.getLocalTempFileName()
                    # Open that path exclusively to make sure we created it
                    with open(src_path, "xb") as fh:
                        # Download into the file
                        size, executable = AbstractJobStore.read_from_url(uri, fh)
                        if executable:
                            # Set the execute bit in the file's permissions
                            os.chmod(src_path, os.stat(src_path).st_mode | stat.S_IXUSR)

        index[src_path] = uri
        existing[uri] = src_path
        return schema_salad.ref_resolver.file_uri(src_path)


def convert_file_uri_to_toil_uri(
    applyFunc: Callable[[str], FileID],
    index: dict[str, str],
    existing: dict[str, str],
    file_uri: str,
) -> str:
    """
    Given a file URI, convert it to a toil file URI. Uses applyFunc to handle the conversion.

    Runs once on every unique file URI.

    'existing' is a set of files retrieved as inputs from toil_get_file. This
    ensures they are mapped back as the same name if passed through.

    Returns a toil uri path to the object.
    """
    # Toil fileStore reference
    if file_uri.startswith("toilfile:") or file_uri.startswith("toildir:"):
        return file_uri
    # File literal outputs with no path, we don't write these and will fail
    # with unsupportedRequirement when retrieving later with getFile
    elif file_uri.startswith("_:"):
        return file_uri
    elif file_uri.startswith(MISSING_FILE):
        # We cannot import a missing file
        raise FileNotFoundError(f"Could not find {file_uri[len(MISSING_FILE):]}")
    else:
        file_uri = existing.get(file_uri, file_uri)
        if file_uri not in index:
            try:
                index[file_uri] = "toilfile:" + applyFunc(file_uri).pack()
                existing[index[file_uri]] = file_uri
            except Exception as e:
                logger.error("Got exception '%s' while copying '%s'", e, file_uri)
                raise
        return index[file_uri]


def path_to_loc(obj: CWLObjectType) -> None:
    """
    Make a path into a location.

    (If a CWL object has a "path" and not a "location")
    """
    if "location" not in obj and "path" in obj:
        obj["location"] = obj["path"]
        del obj["path"]


def extract_file_uri_once(
    fileindex: dict[str, str],
    existing: dict[str, str],
    file_metadata: CWLObjectType,
    mark_broken: bool = False,
    skip_remote: bool = False,
) -> Optional[str]:
    """
    Extract the filename from a CWL file record.

    This function matches the predefined function signature in visit_files, which ensures
    that this function is called on all files inside a CWL object.

    Ensures no duplicate files are returned according to fileindex. If a file has not been resolved already (and had file:// prepended)
    then resolve symlinks.
    :param fileindex: Forward mapping of filename
    :param existing: Reverse mapping of filename. This function does not use this
    :param file_metadata: CWL file record
    :param mark_broken: Whether files should be marked as missing
    :param skip_remote: Whether to skip remote files
    :return:
    """
    location = cast(str, file_metadata["location"])
    if (
        location.startswith("toilfile:")
        or location.startswith("toildir:")
        or location.startswith("_:")
    ):
        return None
    if location in fileindex:
        file_metadata["location"] = fileindex[location]
        return None
    if not location and file_metadata["path"]:
        file_metadata["location"] = location = schema_salad.ref_resolver.file_uri(
            cast(str, file_metadata["path"])
        )
    if location.startswith("file://") and not os.path.isfile(
        schema_salad.ref_resolver.uri_file_path(location)
    ):
        if mark_broken:
            logger.debug("File %s is missing", file_metadata)
            file_metadata["location"] = location = MISSING_FILE + location
        else:
            raise cwl_utils.errors.WorkflowException(
                "File is missing: %s" % file_metadata
            )
    if location.startswith("file://") or not skip_remote:
        # This is a local file or a remote file
        if location not in fileindex:
            # These dictionaries are meant to keep track of what we're going to import
            # In the actual import, this is used as a bidirectional mapping from unvirtualized to virtualized
            # For this case, keep track of the files to prevent returning duplicate files
            # see write_file

            # If there is not a scheme, this file has not been resolved yet or is a URL.
            if not urlparse(location).scheme:
                rp = os.path.realpath(location)
            else:
                rp = location
            return rp
    return None


V = TypeVar("V", covariant=True)


class VisitFunc(Protocol[V]):
    def __call__(
        self,
        fileindex: dict[str, str],
        existing: dict[str, str],
        file_metadata: CWLObjectType,
        mark_broken: bool,
        skip_remote: bool,
    ) -> V: ...


def visit_files(
    func: VisitFunc[V],
    fs_access: StdFsAccess,
    fileindex: dict[str, str],
    existing: dict[str, str],
    cwl_object: Optional[CWLObjectType],
    mark_broken: bool = False,
    skip_remote: bool = False,
    bypass_file_store: bool = False,
) -> list[V]:
    """
    Prepare all files and directories.

    Will be executed from the leader or worker in the context of the given
    CWL tool, order, or output object to be used on the workers. Make
    sure their sizes are set and import all the files.

    Recurses inside directories using the fs_access to find files to upload and
    subdirectory structure to encode, even if their listings are not set or not
    recursive.

    Preserves any listing fields.

    If a file cannot be found (like if it is an optional secondary file that
    doesn't exist), fails, unless mark_broken is set, in which case it applies
    a sentinel location.

    Also does some miscellaneous normalization.

    :param import_function: The function used to upload a URI and get a
        Toil FileID for it.

    :param fs_access: the CWL FS access object we use to access the filesystem
        to find files to import. Needs to support the URI schemes used.

    :param fileindex: Forward map to fill in from file URI to Toil storage
        location, used by write_file to deduplicate writes.

    :param existing: Reverse map to fill in from Toil storage location to file
        URI. Not read from.

    :param cwl_object: CWL tool (or workflow order) we are importing files for

    :param mark_broken: If True, when files can't be imported because they e.g.
        don't exist, set their locations to MISSING_FILE rather than failing
        with an error.

    :param skp_remote: If True, leave remote URIs in place instead of importing
        files.

    :param bypass_file_store: If True, leave file:// URIs in place instead of
        importing files and directories.

    :param log_level: Log imported files at the given level.
    """
    func_return: list[Any] = list()
    tool_id = cwl_object.get("id", str(cwl_object)) if cwl_object else ""

    logger.debug("Importing files for %s", tool_id)
    logger.debug("Importing files in %s", cwl_object)

    # We need to upload all files to the Toil filestore, and encode structure
    # recursively into all Directories' locations. But we cannot safely alter
    # the listing fields of Directory objects, because the handling required by
    # the 1.2 conformance tests does not actually match the spec. See
    # <https://github.com/common-workflow-language/cwl-v1.2/issues/75#issuecomment-858477413>.
    #
    # We need to pass the conformance tests, and only cwltool really knows when
    # to make/destroy the listings in order to do that.

    # First do some preliminary preparation of metadata
    visit_class(cwl_object, ("File", "Directory"), path_to_loc)
    visit_class(cwl_object, ("File",), functools.partial(add_sizes, fs_access))
    normalizeFilesDirs(cwl_object)

    if bypass_file_store:
        # Don't go on to actually import files or encode contents for
        # directories.
        return func_return

    # Otherwise we actually want to put the things in the file store.

    def visit_file_or_directory_down(
        rec: CWLObjectType,
    ) -> Optional[list[CWLObjectType]]:
        """
        Visit each CWL File or Directory on the way down.

        For Files, do nothing.

        For Directories, return the listing key's value if present, or None if
        absent.

        Ensures that the directory's listing is filled in for at least the
        current level, so all direct child File and Directory objects will
        exist.

        Ensures that any child File or Directory objects from the original
        listing remain as child objects, so that they will be hit by the
        recursion.

        Ensures that no directory listings have name collisions.
        """
        if rec.get("class", None) == "File":
            # Nothing to do!
            return None
        elif rec.get("class", None) == "Directory":
            # Check the original listing for collisions
            ensure_no_collisions(cast(DirectoryType, rec))

            # Pull out the old listing, if any
            old_listing = cast(Optional[list[CWLObjectType]], rec.get("listing", None))

            if not cast(str, rec["location"]).startswith("_:"):
                # This is a thing we can list and not just a literal, so we
                # want to ensure that we have at least one level of listing.
                if "listing" in rec and rec["listing"] == []:
                    # Drop the existing listing because it is empty
                    del rec["listing"]
                if "listing" not in rec:
                    # Fill in one level of listing, so we can then traverse it.
                    get_listing(fs_access, rec, recursive=False)
                # Otherwise, we preserve the existing listing (including all
                # its original File objects that we need to process)

                # Check the new listing for collisions
                ensure_no_collisions(cast(DirectoryType, rec))

            return old_listing
        return None

    def visit_file_or_directory_up(
        rec: CWLObjectType,
        down_result: Optional[list[CWLObjectType]],
        child_results: list[DirectoryContents],
    ) -> DirectoryContents:
        """
        For a CWL File or Directory, make sure it is uploaded and it has a
        location that describes its contents as visible to fs_access.

        Replaces each Directory's listing with the down result for the
        directory, or removes it if the down result was None.

        Passes up the tree (and consumes as its second argument a list of)
        information about the directory structure. For a file, we get the
        information for all secondary files, and for a directory, we get a list
        of the information for all contained files and directories.

        The format is a dict from filename to either string Toil file URI, or
        contained similar dict for a subdirectory. We can reduce by joining
        everything into a single dict, when no filenames conflict.
        """
        if rec.get("class", None) == "File":
            # This is a CWL File

            result: DirectoryContents = {}
            # Run a function on the file and store the return
            func_return.append(
                func(
                    fileindex,
                    existing,
                    rec,
                    mark_broken=mark_broken,
                    skip_remote=skip_remote,
                )
            )

            # Make a record for this file under its name
            result[cast(str, rec["basename"])] = cast(str, rec["location"])

            for secondary_file_result in child_results:
                # Glom in the secondary files, if any
                result.update(secondary_file_result)

            return result

        elif rec.get("class", None) == "Directory":
            # This is a CWL Directory

            # Restore the original listing, or its absence
            rec["listing"] = cast(Optional[CWLOutputType], down_result)
            if rec["listing"] is None:
                del rec["listing"]

            # We know we have child results from a fully recursive listing.
            # Build them into a contents dict.
            contents: DirectoryContents = {}
            for child_result in child_results:
                # Keep each child file or directory or child file's secondary
                # file under its name
                contents.update(child_result)

            # Upload the directory itself, which will adjust its location.
            upload_directory(rec, contents, mark_broken=mark_broken)

            # Show those contents as being under our name in our parent.
            return {cast(str, rec["basename"]): contents}

        else:
            raise RuntimeError("Got unexpected class of object: " + str(rec))

    # Process each file and directory in a recursive traversal
    visit_cwl_class_and_reduce(
        cwl_object,
        ("File", "Directory"),
        visit_file_or_directory_down,
        visit_file_or_directory_up,
    )
    return func_return


def upload_directory(
    directory_metadata: CWLObjectType,
    directory_contents: DirectoryContents,
    mark_broken: bool = False,
) -> None:
    """
    Upload a Directory object.

    Ignores the listing (which may not be recursive and isn't safe or efficient
    to touch), and instead uses directory_contents, which is a recursive dict
    structure from filename to file URI or subdirectory contents dict.

    Makes sure the directory actually exists, and rewrites its location to be
    something we can use on another machine.

    If mark_broken is set, ignores missing directories and replaces them with
    directories containing the given (possibly empty) contents.

    We can't rely on the directory's listing as visible to the next tool as a
    complete recursive description of the files we will need to present to the
    tool, since some tools require it to be cleared or single-level but still
    expect to see its contents in the filesystem.
    """
    location = cast(str, directory_metadata["location"])
    if (
        location.startswith("toilfile:")
        or location.startswith("toildir:")
        or location.startswith("_:")
    ):
        # Already in Toil; nothing to do
        return
    if not location and directory_metadata["path"]:
        location = directory_metadata["location"] = schema_salad.ref_resolver.file_uri(
            cast(str, directory_metadata["path"])
        )
    if location.startswith("file://") and not os.path.isdir(
        schema_salad.ref_resolver.uri_file_path(location)
    ):
        if mark_broken:
            logger.debug("Directory %s is missing as a whole", directory_metadata)
        else:
            raise cwl_utils.errors.WorkflowException(
                "Directory is missing: %s" % directory_metadata["location"]
            )

    logger.debug(
        "Uploading directory at %s with contents %s",
        directory_metadata["location"],
        directory_contents,
    )

    # Say that the directory location is just its dumped contents.
    # TODO: store these listings as files in the filestore instead?
    directory_metadata["location"] = encode_directory(directory_contents)


def extract_and_convert_file_to_toil_uri(
    convertfunc: Callable[[str], FileID],
    fileindex: dict[str, str],
    existing: dict[str, str],
    file_metadata: CWLObjectType,
    mark_broken: bool = False,
    skip_remote: bool = False,
) -> None:
    """
    Extract the file URI out of a file object and convert it to a Toil URI.

    Runs convertfunc on the file URI to handle conversion.

    Is used to handle importing files into the jobstore.

    If a file doesn't exist, fails with an error, unless mark_broken is set, in
    which case the missing file is given a special sentinel location.

    Unless skip_remote is set, also run on remote files and sets their locations
    to toil URIs as well.
    """
    location = extract_file_uri_once(
        fileindex, existing, file_metadata, mark_broken, skip_remote
    )
    if location is not None:
        file_metadata["location"] = convert_file_uri_to_toil_uri(
            convertfunc, fileindex, existing, location
        )

    logger.debug("Sending file at: %s", file_metadata["location"])


def writeGlobalFileWrapper(file_store: AbstractFileStore, fileuri: str) -> FileID:
    """Wrap writeGlobalFile to accept file:// URIs."""
    fileuri = fileuri if ":/" in fileuri else f"file://{fileuri}"
    return file_store.writeGlobalFile(schema_salad.ref_resolver.uri_file_path(fileuri))


def remove_empty_listings(rec: CWLObjectType) -> None:
    if rec.get("class") != "Directory":
        finddirs: list[CWLObjectType] = []
        visit_class(rec, ("Directory",), finddirs.append)
        for f in finddirs:
            remove_empty_listings(f)
        return
    if "listing" in rec and rec["listing"] == []:
        del rec["listing"]
        return


class CWLNamedJob(Job):
    """
    Base class for all CWL jobs that do user work, to give them useful names.
    """

    def __init__(
        self,
        cores: Union[float, None] = 1,
        memory: Union[int, str, None] = "1GiB",
        disk: Union[int, str, None] = "1MiB",
        accelerators: Optional[list[AcceleratorRequirement]] = None,
        preemptible: Optional[bool] = None,
        tool_id: Optional[str] = None,
        parent_name: Optional[str] = None,
        subjob_name: Optional[str] = None,
        local: Optional[bool] = None,
    ) -> None:
        """
        Make a new job and set up its requirements and naming.

        :param tool_id: Full CWL tool ID for the job, if applicable.
        :param parent_name: Shortened name of the parent CWL job, if applicable.
        :param subjob_name: Name qualifier for when one CWL tool becomes multiple jobs.
        """

        # Get the name of the class we are, as a final fallback or a name
        # component.
        class_name = self.__class__.__name__

        name_parts = []

        if parent_name is not None:
            # Scope to parent
            name_parts.append(parent_name)
        if tool_id is not None:
            # Start with the short name of the tool
            name_parts.append(shortname(tool_id))
        if subjob_name is not None:
            # We aren't this whole thing, we're a sub-component
            name_parts.append(subjob_name)
        if tool_id is None and subjob_name is None:
            # We need something. Put the class.
            name_parts.append(class_name)

        # String together the hierarchical name
        unit_name = ".".join(name_parts)

        # Display as that along with the class
        display_name = f"{class_name} {unit_name}"

        # Set up the job with the right requirements and names.
        super().__init__(
            cores=cores,
            memory=memory,
            disk=disk,
            accelerators=accelerators,
            preemptible=preemptible,
            unitName=unit_name,
            displayName=display_name,
            local=local,
        )


class ResolveIndirect(CWLNamedJob):
    """
    Helper Job.

    Accepts an unresolved dict (containing promises) and produces a dictionary
    of actual values.
    """

    def __init__(
        self, cwljob: Promised[CWLObjectType], parent_name: Optional[str] = None
    ):
        """Store the dictionary of promises for later resolution."""
        super().__init__(parent_name=parent_name, subjob_name="_resolve", local=True)
        self.cwljob = cwljob

    def run(self, file_store: AbstractFileStore) -> CWLObjectType:
        """Evaluate the promises and return their values."""
        return resolve_dict_w_promises(unwrap(self.cwljob))


def toilStageFiles(
    toil: Toil,
    cwljob: Union[CWLObjectType, list[CWLObjectType]],
    outdir: str,
    destBucket: Union[str, None] = None,
    log_level: int = logging.DEBUG,
) -> None:
    """
    Copy input files out of the global file store and update location and path.

    :param destBucket: If set, export to this base URL instead of to the local
           filesystem.

    :param log_level: Log each file transferred at the given level.
    """

    def _collectDirEntries(
        obj: Union[CWLObjectType, list[CWLObjectType]]
    ) -> Iterator[CWLObjectType]:
        if isinstance(obj, dict):
            if obj.get("class") in ("File", "Directory"):
                yield obj
                yield from _collectDirEntries(obj.get("secondaryFiles", []))
            else:
                for sub_obj in obj.values():
                    yield from _collectDirEntries(sub_obj)
        elif isinstance(obj, list):
            for sub_obj in obj:
                yield from _collectDirEntries(sub_obj)

    # This is all the CWL File and Directory objects we need to export.
    jobfiles = list(_collectDirEntries(cwljob))

    def _realpath(
        ob: CWLObjectType,
    ) -> None:
        location = cast(str, ob["location"])
        if location.startswith("file:"):
            ob["location"] = file_uri(os.path.realpath(uri_file_path(location)))
            logger.debug("realpath %s" % ob["location"])

    visit_class(jobfiles, ("File", "Directory"), _realpath)

    # Now we need to save all the output files and directories.
    # We shall use a ToilPathMapper.
    pm = ToilPathMapper(
        jobfiles,
        "",
        outdir,
        separateDirs=False,
        stage_listing=True,
    )
    for _, p in pm.items():
        if p.staged:
            # We're supposed to copy/expose something.
            # Note that we have to handle writable versions of everything
            # because sometimes we might make them in the PathMapper even if
            # not instructed to copy.
            if destBucket:
                # We are saving to a bucket, directories are fake.
                if p.type in [
                    "File",
                    "CreateFile",
                    "WritableFile",
                    "CreateWritableFile",
                ]:
                    # Directories don't need to be created if we're exporting to a bucket
                    baseName = p.target[len(outdir) :]
                    file_id_or_contents = p.resolved

                    if p.type in [
                        "CreateFile",
                        "CreateWritableFile",
                    ]:  # TODO: CreateFile for buckets is not under testing
                        with NamedTemporaryFile() as f:
                            # Make a file with the right contents
                            f.write(file_id_or_contents.encode("utf-8"))
                            f.close()
                            # Import it and pack up the file ID so we can turn around and export it.
                            file_id_or_contents = (
                                "toilfile:"
                                + toil.import_file(f.name, symlink=False).pack()
                            )

                    if file_id_or_contents.startswith("toildir:"):
                        # Get the directory contents and the path into them, if any
                        here, subpath, _ = decode_directory(file_id_or_contents)
                        if subpath is not None:
                            for part in subpath.split("/"):
                                here = cast(DirectoryContents, here[part])
                        # At the end we should get a direct toilfile: URI
                        file_id_or_contents = cast(str, here)

                    # This might be an e.g. S3 URI now
                    if not file_id_or_contents.startswith("toilfile:"):
                        # We need to import it so we can export it.
                        # TODO: Use direct S3 to S3 copy on exports as well
                        file_id_or_contents = (
                            "toilfile:"
                            + toil.import_file(
                                file_id_or_contents, symlink=False
                            ).pack()
                        )

                    if file_id_or_contents.startswith("toilfile:"):
                        # This is something we can export
                        # TODO: Do we need to urlencode the parts before sending them to S3?
                        dest_url = "/".join(
                            s.strip("/") for s in [destBucket, baseName]
                        )
                        logger.log(log_level, "Saving %s...", dest_url)
                        toil.export_file(
                            FileID.unpack(file_id_or_contents[len("toilfile:") :]),
                            dest_url,
                        )
                    # TODO: can a toildir: "file" get here?
            else:
                # We are saving to the filesystem.
                dest_url = "file://" + quote(p.target)

                # We only really need export_file for actual files.
                if not os.path.exists(p.target) and p.type in [
                    "Directory",
                    "WritableDirectory",
                ]:
                    os.makedirs(p.target)
                if p.type in ["File", "WritableFile"]:
                    if p.resolved.startswith("/"):
                        # Probably staging and bypassing file store. Just copy.
                        logger.log(log_level, "Saving %s...", dest_url)
                        os.makedirs(os.path.dirname(p.target), exist_ok=True)
                        try:
                            shutil.copyfile(p.resolved, p.target)
                        except shutil.SameFileError:
                            # If outdir isn't set and we're passing through an input file/directory as the output,
                            # the file doesn't need to be copied because it is already there
                            pass
                    else:
                        uri = p.resolved
                        if not uri.startswith("toilfile:"):
                            # We need to import so we can export
                            uri = (
                                "toilfile:"
                                + toil.import_file(uri, symlink=False).pack()
                            )

                        # Actually export from the file store
                        logger.log(log_level, "Saving %s...", dest_url)
                        os.makedirs(os.path.dirname(p.target), exist_ok=True)
                        toil.export_file(
                            FileID.unpack(uri[len("toilfile:") :]),
                            dest_url,
                        )
                if p.type in [
                    "CreateFile",
                    "CreateWritableFile",
                ]:
                    # We just need to make a file with particular contents
                    logger.log(log_level, "Saving %s...", dest_url)
                    os.makedirs(os.path.dirname(p.target), exist_ok=True)
                    with open(p.target, "wb") as n:
                        n.write(p.resolved.encode("utf-8"))

    def _check_adjust(f: CWLObjectType) -> CWLObjectType:
        # Figure out where the path mapper put this
        mapped_location: MapperEnt = pm.mapper(cast(str, f["location"]))
        if destBucket:
            # Make the location point to the destination bucket.
            # Reconstruct the path we exported to.
            base_name = mapped_location.target[len(outdir) :]
            dest_url = "/".join(s.strip("/") for s in [destBucket, base_name])
            # And apply it
            f["location"] = dest_url
        else:
            # Make the location point to the place we put this thing on the
            # local filesystem.
            f["location"] = schema_salad.ref_resolver.file_uri(mapped_location.target)
            f["path"] = mapped_location.target

        if "contents" in f:
            del f["contents"]
        return f

    visit_class(cwljob, ("File", "Directory"), _check_adjust)


class CWLJobWrapper(CWLNamedJob):
    """
    Wrap a CWL job that uses dynamic resources requirement.

    When executed, this creates a new child job which has the correct resource
    requirement set.
    """

    def __init__(
        self,
        tool: Process,
        cwljob: CWLObjectType,
        runtime_context: cwltool.context.RuntimeContext,
        parent_name: Optional[str],
        conditional: Union[Conditional, None] = None,
    ):
        """Store our context for later evaluation."""
        super().__init__(
            tool_id=tool.tool.get("id"),
            parent_name=parent_name,
            subjob_name="_wrapper",
            local=True,
        )
        self.cwltool = tool
        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.conditional = conditional or Conditional()
        self.parent_name = parent_name

    def run(self, file_store: AbstractFileStore) -> Any:
        """Create a child job with the correct resource requirements set."""
        cwljob = resolve_dict_w_promises(self.cwljob, file_store)

        # Check confitional to license full evaluation of job inputs.
        if self.conditional.is_false(cwljob):
            return self.conditional.skipped_outputs()

        fill_in_defaults(
            self.cwltool.tool["inputs"],
            cwljob,
            self.runtime_context.make_fs_access(self.runtime_context.basedir or ""),
        )
        # Don't forward the conditional. We checked it already.
        realjob = CWLJob(
            tool=self.cwltool,
            cwljob=cwljob,
            runtime_context=self.runtime_context,
            parent_name=self.parent_name,
        )
        self.addChild(realjob)
        return realjob.rv()


class CWLJob(CWLNamedJob):
    """Execute a CWL tool using cwltool.executors.SingleJobExecutor."""

    def __init__(
        self,
        tool: Process,
        cwljob: CWLObjectType,
        runtime_context: cwltool.context.RuntimeContext,
        parent_name: Optional[str] = None,
        conditional: Union[Conditional, None] = None,
    ):
        """Store the context for later execution."""
        self.cwltool = tool
        self.conditional = conditional or Conditional()

        if runtime_context.builder:
            self.builder = runtime_context.builder
        else:
            self.builder = cwltool.builder.Builder(
                job=cwljob,
                files=[],
                bindings=[],
                schemaDefs={},
                names=Names(),
                requirements=self.cwltool.requirements,
                hints=[],
                resources={},
                mutation_manager=runtime_context.mutation_manager,
                formatgraph=tool.formatgraph,
                make_fs_access=cast(type[StdFsAccess], runtime_context.make_fs_access),
                fs_access=runtime_context.make_fs_access(""),
                job_script_provider=runtime_context.job_script_provider,
                timeout=runtime_context.eval_timeout,
                debug=runtime_context.debug,
                js_console=runtime_context.js_console,
                force_docker_pull=False,
                loadListing=determine_load_listing(tool),
                outdir="",
                tmpdir=DEFAULT_TMPDIR,
                stagedir="/var/lib/cwl",  # TODO: use actual defaults here
                cwlVersion=cast(str, self.cwltool.metadata["cwlVersion"]),
                container_engine=get_container_engine(runtime_context),
            )

        req = tool.evalResources(self.builder, runtime_context)

        tool_own_resources = tool.get_requirement("ResourceRequirement")[0] or {}
        if "ramMin" in tool_own_resources or "ramMax" in tool_own_resources:
            # The tool is actually asking for memory.
            memory = int(req["ram"] * (2**20))
        else:
            # The tool is getting a default ram allocation.
            if getattr(runtime_context, "cwl_default_ram"):
                # We will respect the CWL spec and apply the default cwltool
                # computed, which might be different than Toil's default.
                memory = int(req["ram"] * (2**20))
            else:
                # We use a None requirement and the Toil default applies.
                memory = None

        accelerators: Optional[list[AcceleratorRequirement]] = None
        if req.get("cudaDeviceCount", 0) > 0:
            # There's a CUDARequirement, which cwltool processed for us
            # TODO: How is cwltool deciding what value to use between min and max?
            accelerators = [
                {
                    "kind": "gpu",
                    "api": "cuda",
                    "count": cast(int, req["cudaDeviceCount"]),
                }
            ]

        # cwltool doesn't handle http://arvados.org/cwl#UsePreemptible as part
        # of its resource logic so we have to do it manually.
        #
        # Note that according to
        # https://github.com/arvados/arvados/blob/48a0d575e6de34bcda91c489e4aa98df291a8cca/sdk/cwl/arvados_cwl/arv-cwl-schema-v1.1.yml#L345
        # this can only be a literal boolean! cwltool doesn't want to evaluate
        # expressions in the value for us like it does for CUDARequirement
        # which has a schema which allows for CWL expressions:
        # https://github.com/common-workflow-language/cwltool/blob/1573509eea2faa3cd1dc959224e52ff1d796d3eb/cwltool/extensions.yml#L221
        #
        # By default we have default preemptibility.
        preemptible: Optional[bool] = None
        preemptible_req, _ = tool.get_requirement(
            "http://arvados.org/cwl#UsePreemptible"
        )
        if preemptible_req:
            if "usePreemptible" not in preemptible_req:
                # If we have a requirement it has to have the value
                raise ValidationException(
                    f"Unacceptable syntax for http://arvados.org/cwl#UsePreemptible: "
                    f"expected key usePreemptible but got: {preemptible_req}"
                )
            parsed_value = preemptible_req["usePreemptible"]
            if isinstance(parsed_value, str) and (
                "$(" in parsed_value or "${" in parsed_value
            ):
                # Looks like they tried to use an expression
                raise ValidationException(
                    f"Unacceptable value for usePreemptible in http://arvados.org/cwl#UsePreemptible: "
                    f"expected true or false but got what appears to be an expression: {repr(parsed_value)}. "
                    f"Note that expressions are not allowed here by Arvados's schema."
                )
            if not isinstance(parsed_value, bool):
                # If we have a value it has to be a bool flag
                raise ValidationException(
                    f"Unacceptable value for usePreemptible in http://arvados.org/cwl#UsePreemptible: "
                    f"expected true or false but got: {repr(parsed_value)}"
                )
            preemptible = parsed_value

        # We always need space for the temporary files for the job
        total_disk = cast(int, req["tmpdirSize"]) * (2**20)
        if not getattr(runtime_context, "bypass_file_store", False):
            # If using the Toil file store, we also need space for the output
            # files, which may need to be stored locally and copied off the
            # node.
            total_disk += cast(int, req["outdirSize"]) * (2**20)
        # If not using the Toil file store, output files just go directly to
        # their final homes their space doesn't need to be accounted per-job.

        super().__init__(
            cores=req["cores"],
            memory=memory,
            disk=int(total_disk),
            accelerators=accelerators,
            preemptible=preemptible,
            tool_id=self.cwltool.tool["id"],
            parent_name=parent_name,
            local=isinstance(tool, cwltool.command_line_tool.ExpressionTool),
        )

        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.step_inputs = self.cwltool.tool["inputs"]
        self.workdir: str = runtime_context.workdir  # type: ignore[attr-defined]

    def required_env_vars(self, cwljob: Any) -> Iterator[tuple[str, str]]:
        """Yield environment variables from EnvVarRequirement."""
        if isinstance(cwljob, dict):
            if cwljob.get("class") == "EnvVarRequirement":
                for t in cwljob.get("envDef", {}):
                    yield t["envName"], cast(str, self.builder.do_eval(t["envValue"]))
            for v in cwljob.values():
                yield from self.required_env_vars(v)
        if isinstance(cwljob, list):
            for env_var in cwljob:
                yield from self.required_env_vars(env_var)

    def populate_env_vars(self, cwljob: CWLObjectType) -> dict[str, str]:
        """
        Prepare environment variables necessary at runtime for the job.

        Env vars specified in the CWL "requirements" section should already be
        loaded in self.cwltool.requirements, however those specified with
        "EnvVarRequirement" take precedence and are only populated here. Therefore,
        this not only returns a dictionary with all evaluated "EnvVarRequirement"
        env vars, but checks self.cwltool.requirements for any env vars with the
        same name and replaces their value with that found in the
        "EnvVarRequirement" env var if it exists.
        """
        self.builder.job = cwljob
        required_env_vars = {}
        # iterate over EnvVarRequirement env vars, if any
        for k, v in self.required_env_vars(cwljob):
            required_env_vars[k] = (
                v  # will tell cwltool which env vars to take from the environment
            )
            os.environ[k] = v
            # needs to actually be populated in the environment as well or
            # they're not used

        # EnvVarRequirement env vars take priority over those specified with
        # "requirements" so cwltool.requirements need to be overwritten if an
        # env var with the same name is found
        for req in self.cwltool.requirements:
            if req["class"] == "EnvVarRequirement":
                envDefs = cast(list[dict[str, str]], req["envDef"])
                for env_def in envDefs:
                    env_name = env_def["envName"]
                    if env_name in required_env_vars:
                        env_def["envValue"] = required_env_vars[env_name]
        return required_env_vars

    def run(self, file_store: AbstractFileStore) -> Any:
        """Execute the CWL document."""
        # Adjust cwltool's logging to conform to Toil's settings.
        # We need to make sure this happens in every worker process before we
        # do CWL things.
        cwllogger.removeHandler(defaultStreamHandler)
        cwllogger.setLevel(logger.getEffectiveLevel())

        logger.debug("Loaded order:\n%s", self.cwljob)

        cwljob = resolve_dict_w_promises(self.cwljob, file_store)

        if self.conditional.is_false(cwljob):
            return self.conditional.skipped_outputs()

        fill_in_defaults(
            self.step_inputs, cwljob, self.runtime_context.make_fs_access("")
        )

        required_env_vars = self.populate_env_vars(cwljob)

        immobile_cwljob_dict = copy.deepcopy(cwljob)
        for inp_id in immobile_cwljob_dict.keys():
            found = False
            for field in cast(
                list[dict[str, str]], self.cwltool.inputs_record_schema["fields"]
            ):
                if field["name"] == inp_id:
                    found = True
            if not found:
                cwljob.pop(inp_id)

        # Make sure there aren't any empty listings in directories; they might
        # interfere with the generation of listings of the correct depth by
        # cwltool
        adjustDirObjs(
            cwljob,
            functools.partial(remove_empty_listings),
        )

        index: dict[str, str] = {}
        existing: dict[str, str] = {}

        # Prepare the run instructions for cwltool
        runtime_context = self.runtime_context.copy()

        runtime_context.basedir = os.getcwd()
        runtime_context.preserve_environment = required_env_vars

        # When the tool makes its own PathMappers with make_path_mapper, they
        # will come and grab this function for fetching files from the Toil
        # file store. pipe_threads is used for keeping track of separate
        # threads launched to stream files around.
        pipe_threads: list[tuple[Thread, int]] = []
        setattr(
            runtime_context,
            "toil_get_file",
            functools.partial(
                toil_get_file, file_store, index, existing, pipe_threads=pipe_threads
            ),
        )

        if not getattr(runtime_context, "bypass_file_store", False):
            # Exports temporary directory for batch systems that reset TMPDIR
            os.environ["TMPDIR"] = os.path.realpath(file_store.getLocalTempDir())
            # Make sure temporary files and output files are generated in
            # FileStore-provided places that go away when the job ends.
            runtime_context.outdir = os.path.join(file_store.getLocalTempDir(), "out")
            os.mkdir(runtime_context.outdir)
            # Just keep the temporary output prefix under the job's local temp dir.
            #
            # If we maintain our own system of nested temp directories, we won't
            # know when all the jobs using a higher-level directory are ready for
            # it to be deleted. The local temp dir, under Toil's workDir, will be
            # cleaned up by Toil.
            runtime_context.tmp_outdir_prefix = os.path.join(
                file_store.getLocalTempDir(), "tmp-out"
            )

            runtime_context.tmpdir_prefix = file_store.getLocalTempDir()

            # Intercept file and directory access not already done through the
            # tool's make_path_mapper(), and support the URIs we define for
            # stuff in Toil's FileStore. We need to plug in a make_fs_access
            # function and a path_mapper type or factory function.

            runtime_context.make_fs_access = cast(
                type[StdFsAccess],
                functools.partial(ToilFsAccess, file_store=file_store),
            )

            runtime_context.path_mapper = functools.partial(  # type: ignore[assignment]
                ToilPathMapper,
                get_file=getattr(runtime_context, "toil_get_file"),
                streaming_allowed=runtime_context.streaming_allowed,
            )

        # Collect standard output and standard error somewhere if they don't go to files.
        # We need to keep two FDs to these because cwltool will close what we give it.
        default_stdout = TemporaryFile()
        runtime_context.default_stdout = os.fdopen(
            os.dup(default_stdout.fileno()), "wb"
        )
        default_stderr = TemporaryFile()
        runtime_context.default_stderr = os.fdopen(
            os.dup(default_stderr.fileno()), "wb"
        )

        process_uuid = uuid.uuid4()  # noqa F841
        started_at = datetime.datetime.now()  # noqa F841

        logger.debug("Output disposition: %s", runtime_context.move_outputs)

        logger.debug("Running tool %s with order: %s", self.cwltool, self.cwljob)

        runtime_context.name = self.description.unitName

        if isinstance(self.cwltool, ToilTool):
            # Connect the CWL tool to us so it can call into the Toil job when
            # it reaches points where we might need to debug it.
            self.cwltool.connect_toil_job(self)

        status = "did_not_run"
        try:
            output, status = ToilSingleJobExecutor().execute(
                process=self.cwltool,
                job_order_object=cwljob,
                runtime_context=runtime_context,
                logger=cwllogger,
            )
        finally:
            ended_at = datetime.datetime.now()  # noqa F841

            # Log any output/error data
            default_stdout.seek(0, os.SEEK_END)
            if default_stdout.tell() > 0:
                default_stdout.seek(0)
                file_store.log_user_stream(
                    self.description.unitName + ".stdout", default_stdout
                )
                if status != "success":
                    default_stdout.seek(0)
                    logger.error(
                        "Failed command standard output:\n%s",
                        default_stdout.read().decode("utf-8", errors="replace"),
                    )
            default_stderr.seek(0, os.SEEK_END)
            if default_stderr.tell():
                default_stderr.seek(0)
                file_store.log_user_stream(
                    self.description.unitName + ".stderr", default_stderr
                )
                if status != "success":
                    default_stderr.seek(0)
                    logger.error(
                        "Failed command standard error:\n%s",
                        default_stderr.read().decode("utf-8", errors="replace"),
                    )

        if status != "success":
            raise cwl_utils.errors.WorkflowException(status)

        for t, fd in pipe_threads:
            os.close(fd)
            t.join()

        # Get ahold of the filesystem
        fs_access = runtime_context.make_fs_access(runtime_context.basedir)

        # And a file importer that can go from a file:// URI to a Toil FileID
        def file_import_function(url: str, log_level: int = logging.DEBUG) -> FileID:
            logger.log(log_level, "Loading %s...", url)
            return writeGlobalFileWrapper(file_store, url)

        file_upload_function = functools.partial(
            extract_and_convert_file_to_toil_uri, file_import_function
        )

        # Upload all the Files and set their and the Directories' locations, if
        # needed.
        visit_files(
            file_upload_function,
            fs_access,
            index,
            existing,
            output,
            bypass_file_store=getattr(runtime_context, "bypass_file_store", False),
        )

        logger.debug("Emitting output: %s", output)

        file_store.log_to_leader(f"CWL step complete: {runtime_context.name}")

        # metadata[process_uuid] = {
        #     'started_at': started_at,
        #     'ended_at': ended_at,
        #     'job_order': cwljob,
        #     'outputs': output,
        #     'internal_name': self.jobName
        # }
        return output


def get_container_engine(runtime_context: cwltool.context.RuntimeContext) -> str:
    if runtime_context.podman:
        return "podman"
    elif runtime_context.singularity:
        return "singularity"
    return "docker"


def makeRootJob(
    tool: Process,
    jobobj: CWLObjectType,
    runtime_context: cwltool.context.RuntimeContext,
    initialized_job_order: CWLObjectType,
    options: Namespace,
    toil: Toil,
) -> CWLNamedJob:
    """
    Create the Toil root Job object for the CWL tool. Is the same as makeJob() except this also handles import logic.

    Actually creates what might be a subgraph of two jobs. The second of which may be the follow on of the first.
    If only one job is created, it is returned twice.

    :return:
    """
    if options.run_imports_on_workers:
        filenames = extract_workflow_inputs(options, initialized_job_order, tool)
        metadata = get_file_sizes(
            filenames, toil._jobStore, include_remote_files=options.reference_inputs
        )

        # Mapping of files to metadata for files that will be imported on the worker
        # This will consist of files that we were able to get a file size for
        worker_metadata: dict[str, FileMetadata] = dict()
        # Mapping of files to metadata for files that will be imported on the leader
        # This will consist of files that we were not able to get a file size for
        leader_metadata = dict()
        for filename, file_data in metadata.items():
            if file_data.size is None:
                leader_metadata[filename] = file_data
            else:
                worker_metadata[filename] = file_data

        # import the files for the leader first
        path_to_fileid = WorkerImportJob.import_files(
            list(leader_metadata.keys()), toil._jobStore
        )

        # then install the imported files before importing the other files
        # this way the control flow can fall from the leader to workers
        tool, initialized_job_order = CWLInstallImportsJob.fill_in_files(
            initialized_job_order,
            tool,
            path_to_fileid,
            options.basedir,
            options.reference_inputs,
            options.bypass_file_store,
        )

        import_job = CWLImportWrapper(
            initialized_job_order, tool, runtime_context, worker_metadata, options
        )
        return import_job
    else:
        import_workflow_inputs(
            toil._jobStore,
            options,
            initialized_job_order=initialized_job_order,
            tool=tool,
        )
        root_job, followOn = makeJob(
            tool, jobobj, runtime_context, None, None
        )  # toplevel, no name needed
        root_job.cwljob = initialized_job_order
        return root_job


def makeJob(
    tool: Process,
    jobobj: CWLObjectType,
    runtime_context: cwltool.context.RuntimeContext,
    parent_name: Optional[str],
    conditional: Union[Conditional, None],
) -> Union[
    tuple["CWLWorkflow", ResolveIndirect],
    tuple[CWLJob, CWLJob],
    tuple[CWLJobWrapper, CWLJobWrapper],
]:
    """
    Create the correct Toil Job object for the CWL tool.

    Actually creates what might be a subgraph of two jobs. The second of which may be the follow on of the first.
    If only one job is created, it is returned twice.

    Types: workflow, job, or job wrapper for dynamic resource requirements.

    :return: "wfjob, followOn" if the input tool is a workflow, and "job, job" otherwise
    """
    logger.debug("Make job for tool: %s", tool.tool["id"])
    scan_for_unsupported_requirements(
        tool, bypass_file_store=getattr(runtime_context, "bypass_file_store", False)
    )
    if tool.tool["class"] == "Workflow":
        wfjob = CWLWorkflow(
            cast(cwltool.workflow.Workflow, tool),
            jobobj,
            runtime_context,
            parent_name=parent_name,
            conditional=conditional,
        )
        followOn = ResolveIndirect(wfjob.rv(), parent_name=wfjob.description.unitName)
        wfjob.addFollowOn(followOn)
        return wfjob, followOn
    else:
        # Decied if we have any requirements we care about that are dynamic
        REQUIREMENT_TYPES = [
            "ResourceRequirement",
            "http://commonwl.org/cwltool#CUDARequirement",
        ]
        for requirement_type in REQUIREMENT_TYPES:
            req, _ = tool.get_requirement(requirement_type)
            if req:
                for r in req.values():
                    if isinstance(r, str) and ("$(" in r or "${" in r):
                        # One of the keys in this requirement has a text substitution in it.
                        # TODO: This is not a real lex!

                        # Found a dynamic resource requirement so use a job wrapper
                        job_wrapper = CWLJobWrapper(
                            cast(ToilCommandLineTool, tool),
                            jobobj,
                            runtime_context,
                            parent_name=parent_name,
                            conditional=conditional,
                        )
                        return job_wrapper, job_wrapper
        # Otherwise, all requirements are known now.
        job = CWLJob(
            tool,
            jobobj,
            runtime_context,
            parent_name=parent_name,
            conditional=conditional,
        )
        return job, job


class CWLScatter(Job):
    """
    Implement workflow scatter step.

    When run, this creates a child job for each parameterization of the scatter.
    """

    def __init__(
        self,
        step: cwltool.workflow.WorkflowStep,
        cwljob: CWLObjectType,
        runtime_context: cwltool.context.RuntimeContext,
        parent_name: Optional[str],
        conditional: Union[Conditional, None],
    ):
        """Store our context for later execution."""
        super().__init__(cores=1, memory="1GiB", disk="1MiB", local=True)
        self.step = step
        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.conditional = conditional
        self.parent_name = parent_name

    def flat_crossproduct_scatter(
        self,
        joborder: CWLObjectType,
        scatter_keys: list[str],
        outputs: list[Promised[CWLObjectType]],
        postScatterEval: Callable[[CWLObjectType], CWLObjectType],
    ) -> None:
        """Cartesian product of the inputs, then flattened."""
        scatter_key = shortname(scatter_keys[0])
        for n in range(0, len(cast(list[CWLObjectType], joborder[scatter_key]))):
            updated_joborder = copy.copy(joborder)
            updated_joborder[scatter_key] = cast(
                list[CWLObjectType], joborder[scatter_key]
            )[n]
            if len(scatter_keys) == 1:
                updated_joborder = postScatterEval(updated_joborder)
                subjob, followOn = makeJob(
                    tool=self.step.embedded_tool,
                    jobobj=updated_joborder,
                    runtime_context=self.runtime_context,
                    parent_name=f"{self.parent_name}.{n}",
                    conditional=self.conditional,
                )
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                self.flat_crossproduct_scatter(
                    updated_joborder, scatter_keys[1:], outputs, postScatterEval
                )

    def nested_crossproduct_scatter(
        self,
        joborder: CWLObjectType,
        scatter_keys: list[str],
        postScatterEval: Callable[[CWLObjectType], CWLObjectType],
    ) -> list[Promised[CWLObjectType]]:
        """Cartesian product of the inputs."""
        scatter_key = shortname(scatter_keys[0])
        outputs: list[Promised[CWLObjectType]] = []
        for n in range(0, len(cast(list[CWLObjectType], joborder[scatter_key]))):
            updated_joborder = copy.copy(joborder)
            updated_joborder[scatter_key] = cast(
                list[CWLObjectType], joborder[scatter_key]
            )[n]
            if len(scatter_keys) == 1:
                updated_joborder = postScatterEval(updated_joborder)
                subjob, followOn = makeJob(
                    tool=self.step.embedded_tool,
                    jobobj=updated_joborder,
                    runtime_context=self.runtime_context,
                    parent_name=f"{self.parent_name}.{n}",
                    conditional=self.conditional,
                )
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                outputs.append(
                    self.nested_crossproduct_scatter(  # type: ignore
                        updated_joborder, scatter_keys[1:], postScatterEval
                    )
                )
        return outputs

    def run(self, file_store: AbstractFileStore) -> list[Promised[CWLObjectType]]:
        """Generate the follow on scatter jobs."""
        cwljob = resolve_dict_w_promises(self.cwljob, file_store)

        if isinstance(self.step.tool["scatter"], str):
            scatter = [self.step.tool["scatter"]]
        else:
            scatter = self.step.tool["scatter"]

        scatterMethod = self.step.tool.get("scatterMethod", None)
        if len(scatter) == 1:
            scatterMethod = "dotproduct"
        outputs: list[Promised[CWLObjectType]] = []

        valueFrom = {
            shortname(i["id"]): i["valueFrom"]
            for i in self.step.tool["inputs"]
            if "valueFrom" in i
        }

        def postScatterEval(job_dict: CWLObjectType) -> Any:
            shortio = {shortname(k): v for k, v in job_dict.items()}
            for k in valueFrom:
                job_dict.setdefault(k, None)

            def valueFromFunc(k: str, v: Any) -> Any:
                if k in valueFrom:
                    return cwl_utils.expression.do_eval(
                        valueFrom[k],
                        shortio,
                        self.step.requirements,
                        None,
                        None,
                        {},
                        context=v,
                        container_engine=get_container_engine(self.runtime_context),
                    )
                else:
                    return v

            return {k: valueFromFunc(k, v) for k, v in list(job_dict.items())}

        if scatterMethod == "dotproduct":
            for i in range(
                0, len(cast(list[CWLObjectType], cwljob[shortname(scatter[0])]))
            ):
                copyjob = copy.copy(cwljob)
                for sc in [shortname(x) for x in scatter]:
                    copyjob[sc] = cast(list[CWLObjectType], cwljob[sc])[i]
                copyjob = postScatterEval(copyjob)
                subjob, follow_on = makeJob(
                    tool=self.step.embedded_tool,
                    jobobj=copyjob,
                    runtime_context=self.runtime_context,
                    parent_name=f"{self.parent_name}.{i}",
                    conditional=self.conditional,
                )
                self.addChild(subjob)
                outputs.append(follow_on.rv())
        elif scatterMethod == "nested_crossproduct":
            outputs = self.nested_crossproduct_scatter(cwljob, scatter, postScatterEval)
        elif scatterMethod == "flat_crossproduct":
            self.flat_crossproduct_scatter(cwljob, scatter, outputs, postScatterEval)
        else:
            if scatterMethod:
                raise ValidationException(
                    "Unsupported complex scatter type '%s'" % scatterMethod
                )
            else:
                raise ValidationException(
                    "Must provide scatterMethod to scatter over multiple inputs."
                )

        return outputs


class CWLGather(Job):
    """
    Follows on to a scatter Job.

    This gathers the outputs of each job in the scatter into an array for each
    output parameter.
    """

    def __init__(
        self,
        step: cwltool.workflow.WorkflowStep,
        outputs: Promised[Union[CWLObjectType, list[CWLObjectType]]],
    ):
        """Collect our context for later gathering."""
        super().__init__(cores=1, memory="1GiB", disk="1MiB", local=True)
        self.step = step
        self.outputs = outputs

    @staticmethod
    def extract(
        obj: Union[CWLObjectType, list[CWLObjectType]], k: str
    ) -> Union[CWLOutputType, list[CWLObjectType]]:
        """
        Extract the given key from the obj.

        If the object is a list, extract it from all members of the list.
        """
        if isinstance(obj, Mapping):
            return cast(Union[CWLOutputType, list[CWLObjectType]], obj.get(k))
        elif isinstance(obj, MutableSequence):
            cp: list[CWLObjectType] = []
            for item in obj:
                cp.append(cast(CWLObjectType, CWLGather.extract(item, k)))
            return cp
        else:
            return cast(list[CWLObjectType], [])

    def run(self, file_store: AbstractFileStore) -> dict[str, Any]:
        """Gather all the outputs of the scatter."""
        outobj = {}

        def sn(n: Union[Mapping[str, Any], str]) -> str:
            if isinstance(n, Mapping):
                return shortname(n["id"])
            if isinstance(n, str):
                return shortname(n)

        # TODO: MyPy can't understand that this is the type we should get by unwrapping the promise
        outputs: Union[CWLObjectType, list[CWLObjectType]] = cast(
            Union[CWLObjectType, list[CWLObjectType]], unwrap(self.outputs)
        )
        for k in [sn(i) for i in self.step.tool["out"]]:
            outobj[k] = self.extract(outputs, k)

        return outobj


class SelfJob(Job):
    """Fake job object to facilitate implementation of CWLWorkflow.run()."""

    def __init__(self, j: "CWLWorkflow", v: CWLObjectType):
        """Record the workflow and dictionary."""
        super().__init__(cores=1, memory="1GiB", disk="1MiB")
        self.j = j
        self.v = v

    def rv(self, *path: Any) -> Any:
        """Return our properties dictionary."""
        return self.v

    def addChild(self, c: Job) -> Any:
        """Add a child to our workflow."""
        return self.j.addChild(c)

    def hasChild(self, c: Job) -> Any:
        """Check if the given child is in our workflow."""
        return self.j.hasChild(c)


ProcessType = TypeVar(
    "ProcessType",
    ToilCommandLineTool,
    cwltool.workflow.WorkflowStep,
    cwltool.workflow.Workflow,
    cwltool.command_line_tool.CommandLineTool,
    cwltool.command_line_tool.ExpressionTool,
    Process,
)


def remove_pickle_problems(obj: ProcessType) -> ProcessType:
    """
    Doc_loader does not pickle correctly, causing Toil errors, remove from objects.

    See github issue: https://github.com/mypyc/mypyc/issues/804
    """
    if hasattr(obj, "doc_loader"):
        obj.doc_loader = None
    if isinstance(obj, cwltool.workflow.WorkflowStep):
        obj.embedded_tool = remove_pickle_problems(obj.embedded_tool)
    elif isinstance(obj, cwltool.workflow.Workflow):
        obj.steps = [remove_pickle_problems(s) for s in obj.steps]
    return obj


class CWLWorkflow(CWLNamedJob):
    """
    Toil Job to convert a CWL workflow graph into a Toil job graph.

    The Toil job graph will include the appropriate dependencies.
    """

    def __init__(
        self,
        cwlwf: cwltool.workflow.Workflow,
        cwljob: CWLObjectType,
        runtime_context: cwltool.context.RuntimeContext,
        parent_name: Optional[str] = None,
        conditional: Union[Conditional, None] = None,
    ):
        """Gather our context for later execution."""
        super().__init__(
            tool_id=cwlwf.tool.get("id"), parent_name=parent_name, local=True
        )
        self.cwlwf = cwlwf
        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.conditional = conditional or Conditional()

    def run(
        self, file_store: AbstractFileStore
    ) -> Union[UnresolvedDict, dict[str, SkipNull]]:
        """
        Convert a CWL Workflow graph into a Toil job graph.

        Always runs on the leader, because the batch system knows to schedule
        it as a local job.
        """
        cwljob = resolve_dict_w_promises(self.cwljob, file_store)

        if self.conditional.is_false(cwljob):
            return self.conditional.skipped_outputs()

        # Apply default values set in the workflow
        fs_access = ToilFsAccess(self.runtime_context.basedir, file_store=file_store)
        fill_in_defaults(self.cwlwf.tool["inputs"], cwljob, fs_access)

        # `promises` dict
        # from: each parameter (workflow input or step output)
        #   that may be used as a "source" for a step input workflow output
        #   parameter
        # to: the job that will produce that value.
        promises: dict[str, Job] = {}

        parent_name = shortname(self.cwlwf.tool["id"])

        # `jobs` dict from step id to job that implements that step.
        jobs = {}

        for inp in self.cwlwf.tool["inputs"]:
            promises[inp["id"]] = SelfJob(self, cwljob)

        all_outputs_fulfilled = False
        while not all_outputs_fulfilled:
            # Iteratively go over the workflow steps, scheduling jobs as their
            # dependencies can be fulfilled by upstream workflow inputs or
            # step outputs. Loop exits when the workflow outputs
            # are satisfied.

            all_outputs_fulfilled = True

            for step in self.cwlwf.steps:
                step_id = step.tool["id"]
                if step_id not in jobs:
                    stepinputs_fufilled = True
                    for inp in step.tool["inputs"]:
                        for s in aslist(inp.get("source", [])):
                            if s not in promises:
                                stepinputs_fufilled = False
                    if stepinputs_fufilled:
                        logger.debug("Ready to make job for workflow step %s", step_id)
                        jobobj: dict[
                            str, Union[ResolveSource, DefaultWithSource, StepValueFrom]
                        ] = {}

                        for inp in step.tool["inputs"]:
                            logger.debug("Takes input: %s", inp["id"])
                            key = shortname(inp["id"])
                            if "source" in inp:
                                jobobj[key] = ResolveSource(
                                    name=f"{step_id}/{key}",
                                    input=inp,
                                    source_key="source",
                                    promises=promises,
                                )

                            if "default" in inp:
                                jobobj[key] = DefaultWithSource(
                                    copy.copy(inp["default"]), jobobj.get(key)
                                )

                            if "valueFrom" in inp and "scatter" not in step.tool:
                                jobobj[key] = StepValueFrom(
                                    inp["valueFrom"],
                                    jobobj.get(key, JustAValue(None)),
                                    self.cwlwf.requirements,
                                    get_container_engine(self.runtime_context),
                                )

                            logger.debug(
                                "Value will come from %s", jobobj.get(key, None)
                            )

                        conditional = Conditional(
                            expression=step.tool.get("when"),
                            outputs=step.tool["out"],
                            requirements=self.cwlwf.requirements,
                            container_engine=get_container_engine(self.runtime_context),
                        )

                        if "scatter" in step.tool:
                            wfjob: Union[
                                CWLScatter, CWLWorkflow, CWLJob, CWLJobWrapper
                            ] = CWLScatter(
                                step,
                                UnresolvedDict(jobobj),
                                self.runtime_context,
                                parent_name=parent_name,
                                conditional=conditional,
                            )
                            followOn: Union[
                                CWLGather, ResolveIndirect, CWLJob, CWLJobWrapper
                            ] = CWLGather(step, wfjob.rv())
                            wfjob.addFollowOn(followOn)
                            logger.debug(
                                "Is scatter with job %s and follow-on %s",
                                wfjob,
                                followOn,
                            )
                        else:
                            wfjob, followOn = makeJob(
                                tool=step.embedded_tool,
                                jobobj=UnresolvedDict(jobobj),
                                runtime_context=self.runtime_context,
                                parent_name=f"{parent_name}.{shortname(step_id)}",
                                conditional=conditional,
                            )
                            logger.debug(
                                "Is non-scatter with %s and follow-on %s",
                                wfjob,
                                followOn,
                            )

                        jobs[step_id] = followOn

                        connected = False
                        for inp in step.tool["inputs"]:
                            for s in aslist(inp.get("source", [])):
                                if (
                                    isinstance(promises[s], (CWLJobWrapper, CWLGather))
                                    and not promises[s].hasFollowOn(wfjob)
                                    # promises[s] job has already added wfjob as a followOn prior
                                    and not wfjob.hasPredecessor(promises[s])
                                ):
                                    promises[s].addFollowOn(wfjob)
                                    logger.debug(
                                        "Connect as follow-on based on need for %s from %s",
                                        s,
                                        promises[s],
                                    )
                                    connected = True
                                if not isinstance(
                                    promises[s], (CWLJobWrapper, CWLGather)
                                ) and not promises[s].hasChild(wfjob):
                                    logger.debug(
                                        "Connect as child based on need for %s from %s",
                                        s,
                                        promises[s],
                                    )
                                    promises[s].addChild(wfjob)
                                    connected = True
                        if not connected:
                            # Workflow step is default inputs only & isn't connected
                            # to other jobs, so add it as child of this workflow.
                            self.addChild(wfjob)
                            logger.debug("Run as direct child")

                        for out in step.tool["outputs"]:
                            logger.debug("Provides %s from %s", out["id"], followOn)
                            promises[out["id"]] = followOn

                for inp in step.tool["inputs"]:
                    for source in aslist(inp.get("source", [])):
                        if source not in promises:
                            all_outputs_fulfilled = False

            # may need a test
            for out in self.cwlwf.tool["outputs"]:
                if "source" in out:
                    if out["source"] not in promises:
                        all_outputs_fulfilled = False

        outobj = {}
        for out in self.cwlwf.tool["outputs"]:
            key = shortname(out["id"])
            outobj[key] = ResolveSource(
                name="Workflow output '%s'" % key,
                input=out,
                source_key="outputSource",
                promises=promises,
            )

        return UnresolvedDict(outobj)


class CWLInstallImportsJob(Job):
    def __init__(
        self,
        initialized_job_order: Promised[CWLObjectType],
        tool: Promised[Process],
        basedir: str,
        skip_remote: bool,
        bypass_file_store: bool,
        import_data: Promised[dict[str, FileID]],
        **kwargs: Any,
    ) -> None:
        """
        Job to take the entire CWL object and a mapping of filenames to the imported URIs
        to convert all file locations to URIs.

        This class is only used when runImportsOnWorkers is enabled.
        """
        super().__init__(local=True, **kwargs)
        self.initialized_job_order = initialized_job_order
        self.tool = tool
        self.basedir = basedir
        self.skip_remote = skip_remote
        self.bypass_file_store = bypass_file_store
        self.import_data = import_data

    @staticmethod
    def fill_in_files(
        initialized_job_order: CWLObjectType,
        tool: Process,
        candidate_to_fileid: dict[str, FileID],
        basedir: str,
        skip_remote: bool,
        bypass_file_store: bool,
    ) -> tuple[Process, CWLObjectType]:
        """
        Given a mapping of filenames to Toil file IDs, replace the filename with the file IDs throughout the CWL object.
        """

        def fill_in_file(filename: str) -> FileID:
            """
            Return the file name's associated Toil file ID
            """
            return candidate_to_fileid[filename]

        file_convert_function = functools.partial(
            extract_and_convert_file_to_toil_uri, fill_in_file
        )
        fs_access = ToilFsAccess(basedir)
        fileindex: dict[str, str] = {}
        existing: dict[str, str] = {}
        visit_files(
            file_convert_function,
            fs_access,
            fileindex,
            existing,
            initialized_job_order,
            mark_broken=True,
            skip_remote=skip_remote,
            bypass_file_store=bypass_file_store,
        )
        visitSteps(
            tool,
            functools.partial(
                visit_files,
                file_convert_function,
                fs_access,
                fileindex,
                existing,
                mark_broken=True,
                skip_remote=skip_remote,
                bypass_file_store=bypass_file_store,
            ),
        )

        # We always expect to have processed all files that exist
        for param_name, param_value in initialized_job_order.items():
            # Loop through all the parameters for the workflow overall.
            # Drop any files that aren't either imported (for when we use
            # the file store) or available on disk (for when we don't).
            # This will properly make them cause an error later if they
            # were required.
            rm_unprocessed_secondary_files(param_value)
        return tool, initialized_job_order

    def run(self, file_store: AbstractFileStore) -> Tuple[Process, CWLObjectType]:
        """
        Convert the filenames in the workflow inputs into the URIs
        :return: Promise of transformed workflow inputs. A tuple of the job order and process
        """
        candidate_to_fileid: dict[str, FileID] = unwrap(self.import_data)

        initialized_job_order = unwrap(self.initialized_job_order)
        tool = unwrap(self.tool)
        return CWLInstallImportsJob.fill_in_files(
            initialized_job_order,
            tool,
            candidate_to_fileid,
            self.basedir,
            self.skip_remote,
            self.bypass_file_store,
        )


class CWLImportWrapper(CWLNamedJob):
    """
    Job to organize importing files on workers instead of the leader. Responsible for extracting filenames and metadata,
    calling ImportsJob, applying imports to the job objects, and scheduling the start workflow job

    This class is only used when runImportsOnWorkers is enabled.
    """

    def __init__(
        self,
        initialized_job_order: CWLObjectType,
        tool: Process,
        runtime_context: cwltool.context.RuntimeContext,
        file_to_data: dict[str, FileMetadata],
        options: Namespace,
    ):
        super().__init__(local=False, disk=options.import_workers_threshold)
        self.initialized_job_order = initialized_job_order
        self.tool = tool
        self.options = options
        self.runtime_context = runtime_context
        self.file_to_data = file_to_data

    def run(self, file_store: AbstractFileStore) -> Any:
        imports_job = ImportsJob(
            self.file_to_data,
            self.options.import_workers_threshold,
            self.options.import_workers_disk,
        )
        self.addChild(imports_job)
        install_imports_job = CWLInstallImportsJob(
            initialized_job_order=self.initialized_job_order,
            tool=self.tool,
            basedir=self.options.basedir,
            skip_remote=self.options.reference_inputs,
            bypass_file_store=self.options.bypass_file_store,
            import_data=imports_job.rv(0),
        )
        self.addChild(install_imports_job)
        imports_job.addFollowOn(install_imports_job)

        start_job = CWLStartJob(
            install_imports_job.rv(0),
            install_imports_job.rv(1),
            runtime_context=self.runtime_context,
        )
        self.addChild(start_job)
        install_imports_job.addFollowOn(start_job)

        return start_job.rv()


class CWLStartJob(CWLNamedJob):
    """
    Job responsible for starting the CWL workflow.

    Takes in the workflow/tool and inputs after all files are imported
    and creates jobs to run those workflows.
    """

    def __init__(
        self,
        tool: Promised[Process],
        initialized_job_order: Promised[CWLObjectType],
        runtime_context: cwltool.context.RuntimeContext,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.tool = tool
        self.initialized_job_order = initialized_job_order
        self.runtime_context = runtime_context

    def run(self, file_store: AbstractFileStore) -> Any:
        initialized_job_order = unwrap(self.initialized_job_order)
        tool = unwrap(self.tool)
        cwljob, _ = makeJob(
            tool, initialized_job_order, self.runtime_context, None, None
        )  # toplevel, no name needed
        cwljob.cwljob = initialized_job_order
        self.addChild(cwljob)
        return cwljob.rv()


def extract_workflow_inputs(
    options: Namespace, initialized_job_order: CWLObjectType, tool: Process
) -> list[str]:
    """
    Collect all the workflow input files to import later.
    :param options: namespace
    :param initialized_job_order: cwl object
    :param tool: tool object
    :return:
    """
    fileindex: dict[str, str] = {}
    existing: dict[str, str] = {}

    # Extract out all the input files' filenames
    logger.info("Collecting input files...")
    fs_access = ToilFsAccess(options.basedir)
    filenames = visit_files(
        extract_file_uri_once,
        fs_access,
        fileindex,
        existing,
        initialized_job_order,
        mark_broken=True,
        skip_remote=options.reference_inputs,
        bypass_file_store=options.bypass_file_store,
    )
    # Extract filenames of all the files associated with tools (binaries, etc.).
    logger.info("Collecting tool-associated files...")
    tool_filenames = visitSteps(
        tool,
        functools.partial(
            visit_files,
            extract_file_uri_once,
            fs_access,
            fileindex,
            existing,
            mark_broken=True,
            skip_remote=options.reference_inputs,
            bypass_file_store=options.bypass_file_store,
        ),
    )
    filenames.extend(tool_filenames)
    return [file for file in filenames if file is not None]


def import_workflow_inputs(
    jobstore: AbstractJobStore,
    options: Namespace,
    initialized_job_order: CWLObjectType,
    tool: Process,
    log_level: int = logging.DEBUG,
) -> None:
    """
    Import all workflow inputs on the leader.

    Ran when not importing on workers.
    :param jobstore: Toil jobstore
    :param options: Namespace
    :param initialized_job_order: CWL object
    :param tool: CWL tool
    :param log_level: log level
    :return:
    """
    fileindex: dict[str, str] = {}
    existing: dict[str, str] = {}

    # Define something we can call to import a file and get its file
    # ID.
    def file_import_function(url: str) -> FileID:
        logger.log(log_level, "Loading %s...", url)
        return jobstore.import_file(url, symlink=True)

    import_function = functools.partial(
        extract_and_convert_file_to_toil_uri, file_import_function
    )
    # Import all the input files, some of which may be missing optional
    # files.
    logger.info("Importing input files...")
    fs_access = ToilFsAccess(options.basedir)
    visit_files(
        import_function,
        fs_access,
        fileindex,
        existing,
        initialized_job_order,
        mark_broken=True,
        skip_remote=options.reference_inputs,
        bypass_file_store=options.bypass_file_store,
    )

    # Make another function for importing tool files. This one doesn't allow
    # symlinking, since the tools might be coming from storage not accessible
    # to all nodes.
    tool_import_function = functools.partial(
        extract_and_convert_file_to_toil_uri,
        cast(
            Callable[[str], FileID],
            functools.partial(jobstore.import_file, symlink=False),
        ),
    )

    # Import all the files associated with tools (binaries, etc.).
    # Not sure why you would have an optional secondary file here, but
    # the spec probably needs us to support them.
    logger.info("Importing tool-associated files...")
    visitSteps(
        tool,
        functools.partial(
            visit_files,
            tool_import_function,
            fs_access,
            fileindex,
            existing,
            mark_broken=True,
            skip_remote=options.reference_inputs,
            bypass_file_store=options.bypass_file_store,
        ),
    )

    # We always expect to have processed all files that exist
    for param_name, param_value in initialized_job_order.items():
        # Loop through all the parameters for the workflow overall.
        # Drop any files that aren't either imported (for when we use
        # the file store) or available on disk (for when we don't).
        # This will properly make them cause an error later if they
        # were required.
        rm_unprocessed_secondary_files(param_value)


T = TypeVar("T")


def visitSteps(
    cmdline_tool: Process,
    op: Callable[[CommentedMap], list[T]],
) -> list[T]:
    """
    Iterate over a CWL Process object, running the op on each tool description
    CWL object.
    """
    if isinstance(cmdline_tool, cwltool.workflow.Workflow):
        # For workflows we need to dispatch on steps
        ret = []
        for step in cmdline_tool.steps:
            # Handle the step's tool
            ret.extend(op(step.tool))
            # Recures on the embedded tool; maybe it's a workflow.
            recurse_ret = visitSteps(step.embedded_tool, op)
            ret.extend(recurse_ret)
        return ret
    elif isinstance(cmdline_tool, cwltool.process.Process):
        # All CWL Process objects (including CommandLineTool) will have tools
        # if they bothered to run the Process __init__.
        return op(cmdline_tool.tool)
    raise RuntimeError(
        f"Unsupported type encountered in workflow " f"traversal: {type(cmdline_tool)}"
    )


def rm_unprocessed_secondary_files(job_params: Any) -> None:
    if isinstance(job_params, list):
        for j in job_params:
            rm_unprocessed_secondary_files(j)
    if isinstance(job_params, dict) and "secondaryFiles" in job_params:
        job_params["secondaryFiles"] = filtered_secondary_files(job_params)


def filtered_secondary_files(
    unfiltered_secondary_files: CWLObjectType,
) -> list[CWLObjectType]:
    """
    Remove unprocessed secondary files.

    Interpolated strings and optional inputs in secondary files were added to
    CWL in version 1.1.

    The CWL libraries we call do successfully resolve the interpolated strings,
    but add the resolved fields to the list of unresolved fields so we remove
    them here after the fact.

    We keep secondary files with anything other than MISSING_FILE as their
    location. The 'required' logic seems to be handled deeper in
    cwltool.builder.Builder(), and correctly determines which files should be
    imported. Therefore we remove the files here and if this file is SUPPOSED
    to exist, it will still give the appropriate file does not exist error, but
    just a bit further down the track.
    """
    intermediate_secondary_files = []
    final_secondary_files = []
    # remove secondary files still containing interpolated strings
    for sf in cast(list[CWLObjectType], unfiltered_secondary_files["secondaryFiles"]):
        sf_bn = cast(str, sf.get("basename", ""))
        sf_loc = cast(str, sf.get("location", ""))
        if ("$(" not in sf_bn) and ("${" not in sf_bn):
            if ("$(" not in sf_loc) and ("${" not in sf_loc):
                intermediate_secondary_files.append(sf)
            else:
                logger.debug(
                    "Secondary file %s is dropped because it has an uninterpolated location",
                    sf,
                )
        else:
            logger.debug(
                "Secondary file %s is dropped because it has an uninterpolated basename",
                sf,
            )
    # remove secondary files that are not present in the filestore or pointing
    # to existent things on disk
    for sf in intermediate_secondary_files:
        sf_loc = cast(str, sf.get("location", ""))
        if not sf_loc.startswith(MISSING_FILE) or sf.get("class", "") == "Directory":
            # Pass imported files, and all Directories
            final_secondary_files.append(sf)
        else:
            logger.debug(
                "Secondary file %s is dropped because it is known to be missing", sf
            )
    return final_secondary_files


def scan_for_unsupported_requirements(
    tool: Process, bypass_file_store: bool = False
) -> None:
    """
    Scan the given CWL tool for any unsupported optional features.

    If it has them, raise an informative UnsupportedRequirement.

    :param tool: The CWL tool to check for unsupported requirements.

    :param bypass_file_store: True if the Toil file store is not being used to
        transport files between nodes, and raw origin node file:// URIs are exposed
        to tools instead.

    """

    # We only actually have one unsupported requirement right now, and even
    # that we have a way to support, which we shall explain.
    if not bypass_file_store:
        # If we are using the Toil FileStore we can't do InplaceUpdateRequirement
        req, is_mandatory = tool.get_requirement("InplaceUpdateRequirement")
        if req:
            # The tool actually uses this one. Complain and explain.
            msg = (
                "Toil cannot support InplaceUpdateRequirement when using the Toil file store. "
                "If you are running on a single machine, or a cluster with a shared filesystem, "
                "use the --bypass-file-store option to keep intermediate files on the filesystem. "
                "You can use the --tmp-outdir-prefix, --tmpdir-prefix, --outdir, and --jobStore "
                "options to control where on the filesystem files are placed, if only some parts of "
                "the filesystem are shared."
            )
            if is_mandatory:
                # The requirement cannot be fulfilled. Raise an exception.
                raise CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION(msg)
            else:
                # The hint cannot be fulfilled. Issue a warning.
                logger.warning(msg)


def determine_load_listing(
    tool: Process,
) -> Literal["no_listing", "shallow_listing", "deep_listing"]:
    """
    Determine the directory.listing feature in CWL.

    In CWL, any input directory can have a DIRECTORY_NAME.listing (where
    DIRECTORY_NAME is any variable name) set to one of the following three
    options:

    1. no_listing: DIRECTORY_NAME.listing will be undefined.
        e.g.

            inputs.DIRECTORY_NAME.listing == unspecified

    2. shallow_listing: DIRECTORY_NAME.listing will return a list one level
        deep of DIRECTORY_NAME's contents.
        e.g.

            inputs.DIRECTORY_NAME.listing == [items in directory]
             inputs.DIRECTORY_NAME.listing[0].listing == undefined
             inputs.DIRECTORY_NAME.listing.length == # of items in directory

    3. deep_listing: DIRECTORY_NAME.listing will return a list of the entire
        contents of DIRECTORY_NAME.
        e.g.

            inputs.DIRECTORY_NAME.listing == [items in directory]
            inputs.DIRECTORY_NAME.listing[0].listing == [items in subdirectory
            if it exists and is the first item listed]
            inputs.DIRECTORY_NAME.listing.length == # of items in directory

    See
    https://www.commonwl.org/v1.1/CommandLineTool.html#LoadListingRequirement
    and https://www.commonwl.org/v1.1/CommandLineTool.html#LoadListingEnum

    DIRECTORY_NAME.listing should be determined first from loadListing.
    If that's not specified, from LoadListingRequirement.
    Else, default to "no_listing" if unspecified.

    :param tool: ToilCommandLineTool
    :return str: One of 'no_listing', 'shallow_listing', or 'deep_listing'.
    """
    load_listing_req, _ = tool.get_requirement("LoadListingRequirement")
    load_listing_tool_req = (
        load_listing_req.get("loadListing", "no_listing")
        if load_listing_req
        else "no_listing"
    )
    load_listing = cast(str, tool.tool.get("loadListing", load_listing_tool_req))

    listing_choices = ("no_listing", "shallow_listing", "deep_listing")
    if load_listing not in listing_choices:
        raise ValueError(
            f"Unknown loadListing specified: {load_listing!r}.  Valid choices: {listing_choices}"
        )
    return cast(Literal["no_listing", "shallow_listing", "deep_listing"], load_listing)


class NoAvailableJobStoreException(Exception):
    """Indicates that no job store name is available."""


def generate_default_job_store(
    batch_system_name: Optional[str],
    provisioner_name: Optional[str],
    local_directory: str,
) -> str:
    """
    Choose a default job store appropriate to the requested batch system and
    provisioner, and installed modules. Raises an error if no good default is
    available and the user must choose manually.

    :param batch_system_name: Registry name of the batch system the user has
           requested, if any. If no name has been requested, should be None.
    :param provisioner_name: Name of the provisioner the user has requested,
           if any. Recognized provisioners include 'aws' and 'gce'. None
           indicates that no provisioner is in use.
    :param local_directory: Path to a nonexistent local directory suitable for
           use as a file job store.

    :return str: Job store specifier for a usable job store.
    """

    # Apply default batch system
    batch_system_name = batch_system_name or DEFAULT_BATCH_SYSTEM

    # Work out how to describe where we are
    situation = f"the '{batch_system_name}' batch system"
    if provisioner_name:
        situation += f" with the '{provisioner_name}' provisioner"

    # Default to local job store
    job_store_type = "file"

    try:
        if provisioner_name == "gce":
            # With GCE, always use the Google job store
            job_store_type = "google"
        elif provisioner_name == "aws" or batch_system_name in {"mesos", "kubernetes"}:
            # With AWS or these batch systems, always use the AWS job store
            job_store_type = "aws"
        elif provisioner_name is not None and provisioner_name not in ["aws", "gce"]:
            # We 've never heard of this provisioner and don't know what kind
            # of job store to use with it.
            raise NoAvailableJobStoreException()

        # Then if we get here a job store type has been selected, so try and
        # make it
        return generate_locator(
            job_store_type, local_suggestion=local_directory, decoration="cwl"
        )

    except JobStoreUnavailableException as e:
        raise NoAvailableJobStoreException(
            f"Could not determine a job store appropriate for "
            f"{situation} because: {e}. Please specify a jobstore with the "
            f"--jobStore option."
        )
    except (ImportError, NoAvailableJobStoreException):
        raise NoAvailableJobStoreException(
            f"Could not determine a job store appropriate for "
            f"{situation}. Please specify a jobstore with the "
            f"--jobStore option."
        )


usage_message = "\n\n" + textwrap.dedent(
    """
            NOTE: If you're trying to specify a jobstore, you must use --jobStore, not a positional argument.

            Usage: toil-cwl-runner [options] <workflow> [<input file>] [workflow options]

            Example: toil-cwl-runner \\
                     --jobStore aws:us-west-2:jobstore \\
                     --realTimeLogging \\
                     --logInfo \\
                     example.cwl \\
                     example-job.yaml \\
                     --wf_input="hello world"
    """[
        1:
    ]
)


def get_options(args: list[str]) -> Namespace:
    """
    Parse given args and properly add non-Toil arguments into the cwljob of the Namespace.
    :param args: List of args from command line
    :return: options namespace
    """
    # We can't allow abbreviations in case the workflow defines an option that
    # is a prefix of a Toil option.
    parser = ArgParser(
        allow_abbrev=False,
        usage="%(prog)s [options] WORKFLOW [INFILE] [WF_OPTIONS...]",
        description=textwrap.dedent(
            """
            positional arguments:
              
              WORKFLOW              CWL file to run.

              INFILE                YAML or JSON file of workflow inputs.
              
              WF_OPTIONS            Additional inputs to the workflow as command-line
                                    flags. If CWL workflow takes an input, the name of the
                                    input can be used as an option. For example:
                                    
                                      %(prog)s workflow.cwl --file1 file

                                    If an input has the same name as a Toil option, pass
                                    '--' before it.
        """
        ),
        formatter_class=RawDescriptionHelpFormatter,
    )

    addOptions(parser, jobstore_as_flag=True, cwl=True)
    options: Namespace
    options, extra = parser.parse_known_args(args)
    options.cwljob = extra

    return options


def main(args: Optional[list[str]] = None, stdout: TextIO = sys.stdout) -> int:
    """Run the main loop for toil-cwl-runner."""
    # Remove cwltool logger's stream handler so it uses Toil's
    cwllogger.removeHandler(defaultStreamHandler)

    if args is None:
        args = sys.argv[1:]

    options = get_options(args)

    # Do cwltool setup
    cwltool.main.setup_schema(args=options, custom_schema_callback=None)
    tmpdir_prefix = options.tmpdir_prefix = (
        options.tmpdir_prefix or DEFAULT_TMPDIR_PREFIX
    )
    tmp_outdir_prefix = options.tmp_outdir_prefix or tmpdir_prefix
    workdir = options.workDir or tmp_outdir_prefix

    if options.jobStore is None:
        jobstore = cwltool.utils.create_tmp_dir(tmp_outdir_prefix)
        # Make sure directory doesn't exist so it can be a job store
        os.rmdir(jobstore)
        # Pick a default job store specifier appropriate to our choice of batch
        # system and provisioner and installed modules, given this available
        # local directory name. Fail if no good default can be used.
        options.jobStore = generate_default_job_store(
            options.batchSystem, options.provisioner, jobstore
        )

    options.doc_cache = True
    options.disable_js_validation = False
    options.do_validate = True
    options.pack = False
    options.print_subgraph = False

    if options.batchSystem == "kubernetes":
        # Containers under Kubernetes can only run in Singularity
        options.singularity = True

    if (not options.logLevel or options.logLevel == DEFAULT_LOGLEVEL) and options.quiet:
        # --quiet can override only the default log level, so if you have
        # --logLevel=DEBUG (and debug isn't the default) it beats --quiet.
        options.logLevel = "ERROR"
    if options.logLevel:
        # Make sure cwltool uses Toil's log level.
        # Applies only on the leader.
        cwllogger.setLevel(options.logLevel.upper())

    logger.debug(f"Final job store {options.jobStore} and workDir {options.workDir}")

    outdir = os.path.abspath(options.outdir or os.getcwd())
    conf_file = getattr(options, "beta_dependency_resolvers_configuration", None)
    use_conda_dependencies = getattr(options, "beta_conda_dependencies", None)
    job_script_provider = None
    if conf_file or use_conda_dependencies:
        dependencies_configuration = DependenciesConfiguration(options)
        job_script_provider = dependencies_configuration

    runtime_context = cwltool.context.RuntimeContext(vars(options))
    runtime_context.toplevel = True  # enable discovery of secondaryFiles
    runtime_context.find_default_container = functools.partial(
        find_default_container, options
    )
    runtime_context.workdir = workdir  # type: ignore[attr-defined]
    runtime_context.outdir = outdir
    setattr(runtime_context, "cwl_default_ram", options.cwl_default_ram)
    runtime_context.move_outputs = "leave"
    runtime_context.rm_tmpdir = False
    runtime_context.streaming_allowed = not options.disable_streaming
    if options.cachedir is not None:
        runtime_context.cachedir = os.path.abspath(options.cachedir)
        # Automatically bypass the file store to be compatible with cwltool caching
        # Otherwise, the CWL caching code makes links to temporary local copies
        # of filestore files and caches those.
        logger.debug("CWL task caching is turned on. Bypassing file store.")
        options.bypass_file_store = True
    if options.mpi_config_file is not None:
        runtime_context.mpi_config = MpiConfig.load(options.mpi_config_file)
    if cwltool.main.check_working_directories(runtime_context) is not None:
        logger.error("Failed to create directory. If using tmpdir_prefix, tmpdir_outdir_prefix, or cachedir, consider changing directory locations.")
        return 1
    setattr(runtime_context, "bypass_file_store", options.bypass_file_store)
    if options.bypass_file_store and options.destBucket:
        # We use the file store to write to buckets, so we can't do this (yet?)
        logger.error("Cannot export outputs to a bucket when bypassing the file store")
        return 1
    if not options.bypass_file_store:
        # If we're using Toil's filesystem wrappers and the ability to access
        # URLs implemented by Toil, we need to hook up our own StdFsAccess
        # replacement early, before we try and set up the main CWL document.
        # Otherwise, if it takes a File with loadContents from a URL, we won't
        # be able to load the contents when we need to.
        runtime_context.make_fs_access = ToilFsAccess
    if options.reference_inputs and options.bypass_file_store:
        # We can't do both of these at the same time.
        logger.error("Cannot reference inputs when bypassing the file store")
        return 1

    loading_context = cwltool.main.setup_loadingContext(None, runtime_context, options)

    if options.provenance:
        research_obj = cwltool.cwlprov.ro.ResearchObject(
            temp_prefix_ro=tmp_outdir_prefix,
            orcid=options.orcid,
            full_name=options.cwl_full_name,
            fsaccess=runtime_context.make_fs_access(""),
        )
        runtime_context.research_obj = research_obj

    try:

        if not options.restart:
            # Make a version of the config based on the initial options, for
            # setting up CWL option stuff
            expected_config = Config()
            expected_config.setOptions(options)

            # Before showing the options to any cwltool stuff that wants to
            # load the workflow, transform options.cwltool, where our
            # argument for what to run is, to handle Dockstore workflows.
            options.cwltool = resolve_workflow(options.cwltool)

            # TODO: why are we doing this? Does this get applied to all
            # tools as a default or something?
            loading_context.hints = [
                {
                    "class": "ResourceRequirement",
                    "coresMin": expected_config.defaultCores,
                    # Don't include any RAM requirement because we want to
                    # know when tools don't manually ask for RAM.
                    "outdirMin": expected_config.defaultDisk / (2**20),
                    "tmpdirMin": 0,
                }
            ]
            loading_context.construct_tool_object = toil_make_tool
            loading_context.strict = not options.not_strict
            options.workflow = options.cwltool
            options.job_order = options.cwljob

            try:
                uri, tool_file_uri = cwltool.load_tool.resolve_tool_uri(
                    options.cwltool,
                    loading_context.resolver,
                    loading_context.fetcher_constructor,
                )
            except ValidationException:
                print(
                    "\nYou may be getting this error because your arguments are incorrect or out of order."
                    + usage_message,
                    file=sys.stderr,
                )
                raise

            # Attempt to prepull the containers
            if not options.no_prepull and not options.no_container:
                try_prepull(uri, runtime_context, expected_config.batchSystem)

            options.tool_help = None
            options.debug = options.logLevel == "DEBUG"
            job_order_object, options.basedir, jobloader = cwltool.main.load_job_order(
                options,
                sys.stdin,
                loading_context.fetcher_constructor,
                loading_context.overrides_list,
                tool_file_uri,
            )
            if options.overrides:
                loading_context.overrides_list.extend(
                    cwltool.load_tool.load_overrides(
                        schema_salad.ref_resolver.file_uri(
                            os.path.abspath(options.overrides)
                        ),
                        tool_file_uri,
                    )
                )

            loading_context, workflowobj, uri = cwltool.load_tool.fetch_document(
                uri, loading_context
            )
            loading_context, uri = cwltool.load_tool.resolve_and_validate_document(
                loading_context, workflowobj, uri
            )
            if not loading_context.loader:
                raise RuntimeError("cwltool loader is not set.")
            processobj, metadata = loading_context.loader.resolve_ref(uri)
            processobj = cast(Union[CommentedMap, CommentedSeq], processobj)

            document_loader = loading_context.loader

            if options.provenance and runtime_context.research_obj:
                cwltool.cwlprov.writablebagfile.packed_workflow(
                    runtime_context.research_obj,
                    cwltool.main.print_pack(loading_context, uri),
                )

            try:
                tool = cwltool.load_tool.make_tool(uri, loading_context)
                scan_for_unsupported_requirements(
                    tool, bypass_file_store=options.bypass_file_store
                )
            except CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION as err:
                logging.error(err)
                return CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
            runtime_context.secret_store = SecretStore()

            try:
                # Get the "order" for the execution of the root job. CWLTool
                # doesn't document this much, but this is an "order" in the
                # sense of a "specification" for running a single job. It
                # describes the inputs to the workflow.
                initialized_job_order = cwltool.main.init_job_order(
                    job_order_object,
                    options,
                    tool,
                    jobloader,
                    sys.stdout,
                    make_fs_access=runtime_context.make_fs_access,
                    input_basedir=options.basedir,
                    secret_store=runtime_context.secret_store,
                    input_required=True,
                )
            except SystemExit as err:
                if err.code == 2:  # raised by argparse's parse_args() function
                    print(
                        "\nIf both a CWL file and an input object (YAML/JSON) file were "
                        "provided, the problem may be the argument order."
                        + usage_message,
                        file=sys.stderr,
                    )
                raise

            # Leave the defaults un-filled in the top-level order. The tool or
            # workflow will fill them when it runs

            for inp in tool.tool["inputs"]:
                if (
                    shortname(inp["id"]) in initialized_job_order
                    and inp["type"] == "File"
                ):
                    cast(CWLObjectType, initialized_job_order[shortname(inp["id"])])[
                        "streamable"
                    ] = inp.get("streamable", False)
                    # TODO also for nested types that contain streamable Files

            runtime_context.use_container = not options.no_container
            runtime_context.tmp_outdir_prefix = os.path.realpath(tmp_outdir_prefix)
            runtime_context.job_script_provider = job_script_provider
            runtime_context.force_docker_pull = options.force_docker_pull
            runtime_context.no_match_user = options.no_match_user
            runtime_context.no_read_only = options.no_read_only
            runtime_context.basedir = options.basedir
            if not options.bypass_file_store:
                # If we're using the file store we need to start moving output
                # files now.
                runtime_context.move_outputs = "move"

            # We instantiate an early builder object here to populate indirect
            # secondaryFile references using cwltool's library because we need
            # to resolve them before toil imports them into the filestore.
            # A second builder will be built in the job's run method when toil
            # actually starts the cwl job.
            # Note that this accesses input files for tools, so the
            # ToilFsAccess needs to be set up if we want to be able to use
            # URLs.
            builder = tool._init_job(initialized_job_order, runtime_context)
            if not isinstance(tool, cwltool.workflow.Workflow):
                # make sure this doesn't add listing items; if shallow_listing is
                # selected, it will discover dirs one deep and then again later on
                # (when the cwltool builder gets constructed from the job in the
                # CommandLineTool's job() method,
                # see https://github.com/common-workflow-language/cwltool/blob/9cda157cb4380e9d30dec29f0452c56d0c10d064/cwltool/command_line_tool.py#L951),
                # producing 2+ deep listings instead of only 1.
                # ExpressionTool also uses a builder, see https://github.com/common-workflow-language/cwltool/blob/9cda157cb4380e9d30dec29f0452c56d0c10d064/cwltool/command_line_tool.py#L207
                # Workflows don't need this because they don't go through CommandLineTool or ExpressionTool
                builder.loadListing = "no_listing"

            # make sure this doesn't add listing items; if shallow_listing is
            # selected, it will discover dirs one deep and then again later on
            # (probably when the cwltool builder gets ahold of the job in the
            # CWL job's run()), producing 2+ deep listings instead of only 1.
            builder.loadListing = "no_listing"

            builder.bind_input(
                tool.inputs_record_schema,
                initialized_job_order,
                discover_secondaryFiles=True,
            )

            logger.info("Creating root job")
            logger.debug("Root tool: %s", tool)
            tool = remove_pickle_problems(tool)

        with Toil(options) as toil:
            if options.restart:
                outobj = toil.restart()
            else:
                try:
                    wf1 = makeRootJob(
                        tool=tool,
                        jobobj={},
                        runtime_context=runtime_context,
                        initialized_job_order=initialized_job_order,
                        options=options,
                        toil=toil,
                    )
                except CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION as err:
                    logging.error(err)
                    return CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
                logger.info("Starting workflow")
                outobj = toil.start(wf1)

            # Now the workflow has completed. We need to make sure the outputs (and
            # inputs) end up where the user wants them to be.
            logger.info("Collecting workflow outputs...")
            outobj = resolve_dict_w_promises(outobj)

            # Stage files. Specify destination bucket if specified in CLI
            # options. If destination bucket not passed in,
            # options.destBucket's value will be None.
            toilStageFiles(
                toil,
                outobj,
                outdir,
                destBucket=options.destBucket,
                log_level=logging.INFO,
            )
            logger.info("Stored workflow outputs")

            if runtime_context.research_obj is not None:
                cwltool.cwlprov.writablebagfile.create_job(
                    runtime_context.research_obj, outobj, True
                )

                def remove_at_id(doc: Any) -> None:
                    if isinstance(doc, MutableMapping):
                        for key in list(doc.keys()):
                            if key == "@id":
                                del doc[key]
                            else:
                                value = doc[key]
                                if isinstance(value, MutableMapping):
                                    remove_at_id(value)
                                if isinstance(value, MutableSequence):
                                    for entry in value:
                                        if isinstance(value, MutableMapping):
                                            remove_at_id(entry)

                remove_at_id(outobj)
                visit_class(
                    outobj,
                    ("File",),
                    functools.partial(add_sizes, runtime_context.make_fs_access("")),
                )
                if not document_loader:
                    raise RuntimeError("cwltool loader is not set.")
                prov_dependencies = cwltool.main.prov_deps(
                    workflowobj, document_loader, uri
                )
                runtime_context.research_obj.generate_snapshot(prov_dependencies)
                cwltool.cwlprov.writablebagfile.close_ro(
                    runtime_context.research_obj, options.provenance
                )

            if not options.destBucket and options.compute_checksum:
                logger.info("Computing output file checksums...")
                visit_class(
                    outobj,
                    ("File",),
                    functools.partial(compute_checksums, StdFsAccess("")),
                )

            visit_class(outobj, ("File",), MutationManager().unset_generation)
            stdout.write(json.dumps(outobj, indent=4, default=str))
            stdout.write("\n")
            logger.info("CWL run complete!")
    # Don't expose tracebacks to the user for exceptions that may be expected
    except FailedJobsException as err:
        if err.exit_code == CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE:
            # We figured out that we can't support this workflow.
            logging.error(err)
            logging.error(
                "Your workflow uses a CWL requirement that Toil does not support!"
            )
            return CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
        else:
            logging.error(err)
            return 1
    except (
        InsufficientSystemResources,
        LocatorException,
        InvalidImportExportUrlException,
        UnimplementedURLException,
        JobTooBigError,
        FileNotFoundError
    ) as err:
        logging.error(err)
        return 1

    return 0


def find_default_container(
    args: Namespace, builder: cwltool.builder.Builder
) -> Optional[str]:
    """Find the default constructor by consulting a Toil.options object."""
    if args.default_container:
        return str(args.default_container)
    if args.beta_use_biocontainers:
        return get_container_from_software_requirements(True, builder)
    return None
