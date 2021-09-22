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
import argparse
import base64
import copy
import datetime
import errno
import functools
import json
import logging
import os
import shutil
import socket
import stat
import sys
import tempfile
import textwrap
import urllib
import uuid
from typing import (
    Any,
    Callable,
    Dict,
    IO,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Pattern,
    Text,
    TextIO,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from urllib import parse as urlparse

import cwltool.builder
import cwltool.command_line_tool
import cwltool.context
import cwltool.errors
import cwltool.expression
import cwltool.load_tool
import cwltool.main
import cwltool.provenance
import cwltool.resolver
import cwltool.stdfsaccess
import schema_salad.ref_resolver
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
from cwltool.software_requirements import (
    DependenciesConfiguration,
    get_container_from_software_requirements,
)
from cwltool.utils import (
    CWLObjectType,
    CWLOutputType,
    adjustDirObjs,
    adjustFileObjs,
    aslist,
    get_listing,
    normalizeFilesDirs,
    visit_class,
    downloadHttpFile,
)
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from schema_salad import validate
from schema_salad.exceptions import ValidationException
from schema_salad.avro.schema import Names
from schema_salad.sourceline import SourceLine, cmap
from threading import Thread

from toil.batchSystems.registry import DEFAULT_BATCH_SYSTEM
from toil.common import Config, Toil, addOptions
from toil.cwl.utils import (
    download_structure,
    visit_top_cwl_class,
    visit_cwl_class_and_reduce,
)
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException
from toil.jobStores.fileJobStore import FileJobStore
from toil.lib.threading import ExceptionalThread
from toil.version import baseVersion

logger = logging.getLogger(__name__)

# Define internal jobs we should avoid submitting to batch systems and logging
CWL_INTERNAL_JOBS = (
    "CWLJobWrapper",
    "CWLWorkflow",
    "CWLScatter",
    "CWLGather",
    "ResolveIndirect",
)

# What exit code do we need to bail with if we or any of the local jobs that
# parse workflow files see an unsupported feature?
CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE = 33

# And what error will make the worker exit with that code
CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION = cwltool.errors.UnsupportedRequirement

# Find the default temporary directory
DEFAULT_TMPDIR = tempfile.gettempdir()
# And compose a CWL-style default prefix inside it.
# We used to not put this inside anything and we would drop loads of temp
# directories in the current directory and leave them there.
DEFAULT_TMPDIR_PREFIX = os.path.join(DEFAULT_TMPDIR, "tmp")


def cwltoil_was_removed() -> None:
    """Complain about deprecated entrypoint."""
    raise RuntimeError(
        'Please run with "toil-cwl-runner" instead of "cwltoil" (which has been removed).'
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


class UnresolvedDict(Dict[Any, Any]):
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


def _filter_skip_null(value: Any, err_flag: List[bool]) -> Any:
    """
    Private implementation for recursively filtering out SkipNull objects from 'value'.

    :param value: port output value object
    :param err_flag: A pass by reference boolean (passed by enclosing in a list) that
                     allows us to flag, at any level of recursion, that we have encountered
                     a SkipNull.
    """
    if isinstance(value, SkipNull):
        err_flag[0] = True
        value = None
    elif isinstance(value, list):
        return [_filter_skip_null(v, err_flag) for v in value]
    elif isinstance(value, dict):
        return {k: _filter_skip_null(v, err_flag) for k, v in value.items()}
    return value


class Conditional:
    """
    Object holding conditional expression until we are ready to evaluate it.

    Evaluation occurs at the moment the encloses step is ready to run.
    """

    def __init__(
        self,
        expression: Union[str, None] = None,
        outputs: Union[Dict[str, CWLOutputType], None] = None,
        requirements: List[CWLObjectType] = [],
        container_engine: str = "docker",
    ):
        """
        Instantiate a conditional expression.

        :param expression: A string with the expression from the 'when' field of the step
        :param outputs: The output dictionary for the step. This is needed because if the
                        step is skipped, all the outputs need to be populated with SkipNull
                        values
        :param requirements: The requirements object that is needed for the context the
                             expression will evaluate in.
        """
        self.expression = expression
        self.outputs = outputs
        self.requirements = requirements
        self.container_engine = container_engine

    def is_false(self, job: Union[CWLObjectType, None]) -> bool:
        """
        Determine if expression evaluates to False given completed step inputs.

        :param job: job output object
        :return: bool
        """
        if self.expression is None:
            return False

        expr_is_true = cwltool.expression.do_eval(
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

        raise cwltool.errors.WorkflowException(
            "'%s' evaluated to a non-boolean value" % self.expression
        )

    def skipped_outputs(self) -> Dict[str, SkipNull]:
        """
        Generate a dict of SkipNull objects corresponding to the output structure of the step.

        :return: dict
        """
        outobj = {}

        def sn(n: Any) -> str:
            if isinstance(n, Mapping):
                return shortname(n["id"])
            if isinstance(n, str):
                return shortname(n)

        for k in [sn(o) for o in self.outputs]:
            outobj[k] = SkipNull()

        return outobj


class ResolveSource:
    """Apply linkMerge and pickValue operators to values coming into a port."""

    def __init__(self, name: str, input: dict, source_key: str, promises: dict):
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
        # Rule is that source: [foo] is just foo unless it also has linkMerge: merge_nested
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
            self.promise_tuples = (shortname(s), promises[s].rv())  # type: ignore

    def __repr__(self) -> str:
        """
        Allow for debug printing.
        """

        try:
            return "ResolveSource(" + repr(self.resolve()) + ")"
        except:
            return f"ResolveSource({self.name}, {self.input}, {self.source_key}, {self.promise_tuples})"

    def resolve(self) -> Any:
        """
        First apply linkMerge then pickValue if either present.

        :return: dict
        """
        if isinstance(self.promise_tuples, list):
            result = self.link_merge([v[1][v[0]] for v in self.promise_tuples])  # type: ignore
        else:
            value = self.promise_tuples
            result = value[1].get(value[0])  # type: ignore

        result = self.pick_value(result)
        result = filter_skip_null(self.name, result)
        return result

    def link_merge(self, values: dict) -> Union[list, dict]:
        """
        Apply linkMerge operator to `values` object.

        :param values: dict: result of step
        """
        link_merge_type = self.input.get("linkMerge", "merge_nested")

        if link_merge_type == "merge_nested":
            return values

        elif link_merge_type == "merge_flattened":
            result = []  # type: ignore
            for v in values:
                if isinstance(v, MutableSequence):
                    result.extend(v)
                else:
                    result.append(v)
            return result

        else:
            raise ValidationException(
                "Unsupported linkMerge '%s' on %s." % (link_merge_type, self.name)
            )

    def pick_value(self, values: Union[List, Any]) -> Any:
        """
        Apply pickValue operator to `values` object.

        :param values: Intended to be a list, but other types will be returned
                       without modification.
        :return:
        """
        pick_value_type = self.input.get("pickValue")

        if pick_value_type is None:
            return values

        if not isinstance(values, list):
            logger.warning("pickValue used but input %s is not a list." % self.name)
            return values

        result = [v for v in values if not isinstance(v, SkipNull) and v is not None]

        if pick_value_type == "first_non_null":
            if len(result) < 1:
                raise cwltool.errors.WorkflowException(
                    "%s: first_non_null operator found no non-null values" % self.name
                )
            else:
                return result[0]

        elif pick_value_type == "the_only_non_null":
            if len(result) == 0:
                raise cwltool.errors.WorkflowException(
                    "%s: the_only_non_null operator found no non-null values"
                    % self.name
                )
            elif len(result) > 1:
                raise cwltool.errors.WorkflowException(
                    "%s: the_only_non_null operator found more than one non-null values"
                    % self.name
                )
            else:
                return result[0]

        elif pick_value_type == "all_non_null":
            return result

        else:
            raise cwltool.errors.WorkflowException(
                "Unsupported pickValue '%s' on %s" % (pick_value_type, self.name)
            )


class StepValueFrom:
    """
    A workflow step input which has a valueFrom expression attached to it.

    The valueFrom expression will be evaluated to produce the actual input
    object for the step.
    """

    def __init__(
        self, expr: str, source: Any, req: List[CWLObjectType], container_engine: str
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

    def eval_prep(self, step_inputs: dict, file_store: AbstractFileStore):
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
        return cwltool.expression.do_eval(
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

    def resolve(self) -> Any:
        """Return the value."""
        return self.val


def resolve_dict_w_promises(
    dict_w_promises: dict, file_store: AbstractFileStore = None
) -> dict:
    """
    Resolve a dictionary of promises evaluate expressions to produce the actual values.

    :param dict_w_promises: input dict for these values
    :return: dictionary of actual values
    """
    if isinstance(dict_w_promises, UnresolvedDict):
        first_pass_results = {k: v.resolve() for k, v in dict_w_promises.items()}
    else:
        first_pass_results = {k: v for k, v in dict_w_promises.items()}

    result = {}
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
        referenced_files: list,
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
        """
        self.get_file = get_file
        self.stage_listing = stage_listing
        self.streaming_allowed = streaming_allowed

        super(ToilPathMapper, self).__init__(
            referenced_files, basedir, stagedir, separateDirs=separateDirs
        )

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
        upload_directory(), with their children being assigned
        locations based on listing the Directories using ToilFsAccess.

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

        # Decide where to put the file or directory, as an absolute path.
        tgt = os.path.join(
            stagedir,
            cast(str, obj["basename"]),
        )

        if obj["class"] == "Directory":
            # Whether or not we've already mapped this path, we need to map all
            # children recursively.

            # Grab its location
            location = cast(str, obj["location"])

            logger.debug("ToilPathMapper visiting directory %s", location)

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
                    resolved = schema_salad.ref_resolver.uri_file_path(
                        self.get_file("_:")
                    )

                    if "listing" in obj and obj["listing"] != []:
                        # If there's stuff inside here to stage, we need to copy
                        # this directory here, because we can't Docker mount things
                        # over top of immutable directories.
                        copy_here = True
                else:
                    # We can't really make the directory. Maybe we are
                    # exporting from the leader and it doesn't matter.
                    resolved = location
            else:
                raise RuntimeError("Unsupported location: " + location)

            if location in self._pathmap:
                # Don't map the same directory twice
                logger.debug(
                    "ToilPathMapper stopping recursion because we have already mapped directory: %s",
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
                cast(List, obj.get("listing", [])),
                tgt,
                basedir,
                copy=copy,
                staged=staged,
            )

        elif obj["class"] == "File":
            path = cast(str, obj["location"])

            logger.debug("ToilPathMapper visiting file %s", path)

            if path in self._pathmap:
                # Don't map the same file twice
                logger.debug(
                    "ToilPathMapper stopping recursion because we have already mapped file: %s",
                    path,
                )
                return

            ab = cwltool.stdfsaccess.abspath(path, basedir)
            if "contents" in obj and path.startswith("_:"):
                # We are supposed to create this file
                self._pathmap[path] = MapperEnt(
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
                            path, obj.get("streamable", False), self.streaming_allowed
                        )
                    else:
                        deref = ab

                    if deref.startswith("file:"):
                        deref = schema_salad.ref_resolver.uri_file_path(deref)
                    if urllib.parse.urlsplit(deref).scheme in ["http", "https"]:
                        deref = downloadHttpFile(path)
                    elif urllib.parse.urlsplit(deref).scheme != "toilfile":
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

                    logger.debug(
                        "ToilPathMapper adding file mapping %s -> %s", deref, tgt
                    )
                    self._pathmap[path] = MapperEnt(
                        deref, tgt, "WritableFile" if copy else "File", staged
                    )

            # Handle all secondary files that need to be next to this one.
            self.visitlisting(
                cast(List[CWLObjectType], obj.get("secondaryFiles", [])),
                stagedir,
                basedir,
                copy=copy,
                staged=staged,
            )


class ToilSingleJobExecutor(cwltool.executors.SingleJobExecutor):
    """
    Version of cwltool's SingleJobExecutor that does not assume it is at the
    top level of the workflow.

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
        """
        Override run_jobs from SingleJobExecutor but say we are not in a top
        level runtime context.
        """
        runtime_context.toplevel = False
        return super().run_jobs(process, job_order_object, logger, runtime_context)


class ToilCommandLineTool(cwltool.command_line_tool.CommandLineTool):
    """Subclass the cwltool command line tool to provide the custom Toil.PathMapper."""

    def make_path_mapper(
        self,
        reffiles: List[Any],
        stagedir: str,
        runtimeContext: cwltool.context.RuntimeContext,
        separateDirs: bool,
    ) -> cwltool.pathmapper.PathMapper:
        """Create the appropriate PathMapper for the situation."""

        if getattr(runtimeContext, "bypass_file_store", False):
            # We only need to understand cwltool's supported URIs
            return PathMapper(
                reffiles, runtimeContext.basedir, stagedir, separateDirs=separateDirs
            )
        else:
            # We need to be able to read from Toil-provided URIs
            return ToilPathMapper(
                reffiles,
                runtimeContext.basedir,
                stagedir,
                separateDirs,
                getattr(runtimeContext, "toil_get_file", None),
                streaming_allowed=runtimeContext.streaming_allowed,
            )

    def __str__(self):
        return f'ToilCommandLineTool({repr(self.tool.get("id", "???"))})'


def toil_make_tool(
    toolpath_object: CommentedMap,
    loadingContext: cwltool.context.LoadingContext,
) -> Process:
    """
    Emit custom ToilCommandLineTools.

    This factory function is meant to be passed to cwltool.load_tool().
    """
    if (
        isinstance(toolpath_object, Mapping)
        and toolpath_object.get("class") == "CommandLineTool"
    ):
        return ToilCommandLineTool(toolpath_object, loadingContext)
    return cwltool.workflow.default_make_tool(toolpath_object, loadingContext)


def decode_directory(dir_path: str) -> Tuple[Dict, Optional[str], str]:
    """
    Given a toildir: path to a directory or a file in it, return the
    decoded directory dict, the remaining part of the path (which may be
    None), and the deduplication key string that uniquely identifies the
    directory.
    """

    assert dir_path.startswith(
        "toildir:"
    ), f"Cannot decode non-directory path: {dir_path}"

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

    if len(parts) == 1 or parts[1] == "/":
        # We didn't have any subdirectory
        return contents, None, dir_data
    else:
        # We have a path below this
        return contents, parts[1], dir_data


class ToilFsAccess(cwltool.stdfsaccess.StdFsAccess):
    """
    Custom filesystem access class which handles toil filestore references.
    Normal file paths will be resolved relative to basedir, but 'toilfile:' and
    'toildir:' URIs will be fulfilled from the Toil file store.
    """

    def __init__(self, basedir: str, file_store: AbstractFileStore = None):
        """Create a FsAccess object for the given Toil Filestore and basedir."""
        self.file_store = file_store

        # Map encoded directory structures to where we downloaded them, so we
        # don't constantly redownload them.
        # Assumes nobody will touch our files via realpath, or that if they do
        # they know what will happen.
        self.dir_to_download = {}

        super(ToilFsAccess, self).__init__(basedir)

    def _abs(self, path: str) -> str:
        """
        Return a local absolute path for a file (no schema).

        Overwrites cwltool.stdfsaccess.StdFsAccess._abs() to account for toil specific schema.
        """

        # TODO: Both we and the ToilPathMapper relate Toil paths to local
        # paths. But we don't share the same mapping, so accesses through
        # different mechanisms will produce different local copies.

        # Used to fetch a path to determine if a file exists in the inherited
        # cwltool.stdfsaccess.StdFsAccess, (among other things) so this should
        # not error on missing files.
        # See: https://github.com/common-workflow-language/cwltool/blob/beab66d649dd3ee82a013322a5e830875e8556ba/cwltool/stdfsaccess.py#L43  # noqa B950
        if path.startswith("toilfile:"):
            # Is a Toil file
            destination = self.file_store.readGlobalFile(
                FileID.unpack(path[len("toilfile:") :]), symlink=True
            )
            logger.debug("Downloaded %s to %s", path, destination)
            if not os.path.exists(destination):
                raise RuntimeError(
                    f"{destination} does not exist after filestore read."
                )
        elif path.startswith("toildir:"):
            # Is a directory or relative to it

            # We will download the whole directory and then look inside it

            # Decode its contents, the path inside it to the file (if any), and
            # the key to use for caching the directory.
            contents, subpath, cache_key = decode_directory(path)

            if cache_key not in self.dir_to_download:
                # Download to a temp directory.
                temp_dir = self.file_store.getLocalTempDir()
                temp_dir += "/toildownload"
                os.makedirs(temp_dir)

                logger.debug("ToilFsAccess downloading %s to %s", cache_key, temp_dir)

                # Save it all into this new temp directory
                download_structure(self.file_store, {}, {}, contents, temp_dir)

                # Make sure we use the same temp directory if we go traversing
                # around this thing.
                self.dir_to_download[cache_key] = temp_dir
            else:
                logger.debug("ToilFsAccess already has %s", cache_key)

            if subpath is None:
                # We didn't have any subdirectory, so just give back the path to the root
                destination = self.dir_to_download[cache_key]
            else:
                # Navigate to the right subdirectory
                destination = self.dir_to_download[cache_key] + "/" + subpath
        else:
            # This is just a local file
            destination = path

        # Now destination is a local file, so make sure we really do have an
        # absolute path
        destination = super(ToilFsAccess, self)._abs(destination)
        return destination

    def glob(self, pattern: str) -> List[str]:
        # We know this falls back on _abs
        return super(ToilFsAccess, self).glob(pattern)

    def open(self, fn: str, mode: str) -> IO[Any]:
        # We know this falls back on _abs
        return super(ToilFsAccess, self).open(fn, mode)

    def exists(self, path: str) -> bool:
        """Test for file existance."""
        # toil's _abs() throws errors when files are not found and cwltool's _abs() does not
        try:
            return os.path.exists(self._abs(path))
        except NoSuchFileException:
            return False

    def size(self, path: str) -> int:
        # This should avoid _abs for things actually in the file store, to
        # prevent multiple downloads as in
        # https://github.com/DataBiosphere/toil/issues/3665
        if path.startswith("toilfile:"):
            return self.file_store.getGlobalFileSize(
                FileID.unpack(path[len("toilfile:") :])
            )
        elif path.startswith("toildir:"):
            # Decode its contents, the path inside it to the file (if any), and
            # the key to use for caching the directory.
            here, subpath, cache_key = decode_directory(path)

            # We can't get the size of just a directory.
            assert subpath is not None, f"Attempted to check size of directory {path}"

            for part in subpath.split("/"):
                # Follow the path inside the directory contents.
                here = here[part]

            # We ought to end up with a toilfile: URI.
            assert isinstance(here, str), f"Did not find a file at {path}"
            assert here.startswith(
                "toilfile:"
            ), f"Did not find a filestore file at {path}"

            return self.size(here)
        else:
            # We know this falls back on _abs
            return super().size(path)

    def isfile(self, fn: str) -> bool:
        # We know this falls back on _abs
        return super(ToilFsAccess, self).isfile(fn)

    def isdir(self, fn: str) -> bool:
        # We know this falls back on _abs
        return super(ToilFsAccess, self).isdir(fn)

    def listdir(self, fn: str) -> List[str]:
        logger.debug("ToilFsAccess listing %s", fn)

        # Download the file or directory to a local path
        directory = self._abs(fn)

        # Now list it (it is probably a directory)
        return [
            cwltool.stdfsaccess.abspath(urllib.parse.quote(entry), fn)
            for entry in os.listdir(directory)
        ]

    def join(self, path, *paths):  # type: (str, *str) -> str
        # This falls back on os.path.join
        return super(ToilFsAccess, self).join(path, *paths)

    def realpath(self, path: str) -> str:
        if path.startswith("toilfile:"):
            # import the file and make it available locally if it exists
            path = self._abs(path)
        elif path.startswith("toildir:"):
            # Import the whole directory
            path = self._abs(path)
        return os.path.realpath(path)


def toil_get_file(
    file_store: AbstractFileStore,
    index: Dict[str, str],
    existing: Dict[str, str],
    file_store_id: str,
    streamable: bool = False,
    streaming_allowed: bool = True,
    pipe_threads: list = None,
) -> str:
    """
    Set up the given file or directory from the Toil jobstore at a file URI
    where it can be accessed locally.

    Run as part of the ToilCommandLineTool setup, inside jobs on the workers.

    :param file_store: The Toil file store to download from.

    :param index: Maps from downloaded file path back to input Toil URI.

    :param existing: Maps from file_store_id URI to downloaded file path.

    :param file_store_id: The URI for the file to download.

    :param streamable: If the file is has 'streamable' flag set

    :param streaming_allowed: If streaming is allowed

    :param pipe_threads: List of threads responsible for streaming the data
    and open file descriptors corresponding to those files. Caller is resposible
    to close the file descriptors (to break the pipes) and join the threads
    """

    if file_store_id.startswith("toildir:"):
        # This is a file in a directory, or maybe a directory itself.
        # See ToilFsAccess and upload_directory.
        # We will go look for the actual file in the encoded directory
        # structure which will tell us where the toilfile: name for the file is.

        parts = file_store_id[len("toildir:") :].split("/")
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
    elif file_store_id.startswith("toilfile:"):
        # This is a plain file with no context.
        def write_to_pipe(
            file_store: AbstractFileStore, pipe_name: str, file_store_id: FileID
        ) -> None:
            try:
                with open(pipe_name, "wb") as pipe:
                    with file_store.jobStore.readFileStream(file_store_id) as fi:
                        file_store.logAccess(file_store_id)
                        chunk_sz = 1024
                        while True:
                            data = fi.read(chunk_sz)
                            if not data:
                                break
                            pipe.write(data)
            except IOError as e:
                # The other side of the pipe may have been closed by the
                # reading thread, which is OK.
                if e.errno != errno.EPIPE:
                    raise

        if (
            streaming_allowed
            and streamable
            and not isinstance(file_store.jobStore, FileJobStore)
        ):
            logger.debug(
                "Streaming file %s", FileID.unpack(file_store_id[len("toilfile:") :])
            )
            src_path = file_store.getLocalTempFileName()
            os.mkfifo(src_path)
            th = ExceptionalThread(
                target=write_to_pipe,
                args=(
                    file_store,
                    src_path,
                    FileID.unpack(file_store_id[len("toilfile:") :]),
                ),
            )
            th.start()
            pipe_threads.append((th, os.open(src_path, os.O_RDONLY)))
        else:
            src_path = file_store.readGlobalFile(
                FileID.unpack(file_store_id[len("toilfile:") :]), symlink=True
            )

        # TODO: shouldn't we be using these as a cache?
        index[src_path] = file_store_id
        existing[file_store_id] = src_path
        return schema_salad.ref_resolver.file_uri(src_path)
    elif file_store_id.startswith("_:"):
        # Someone is asking us for an empty temp directory.
        dest_path = file_store.getLocalTempDir()
        return schema_salad.ref_resolver.file_uri(dest_path)
    else:
        raise RuntimeError(
            f"Cannot obtain file {file_store_id} from host "
            f"{socket.gethostname()}; all imports must happen on the "
            f"leader!"
        )


def write_file(writeFunc: Any, index: dict, existing: dict, file_uri: str) -> str:
    """
    Write a file into the Toil jobstore.

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
    else:
        file_uri = existing.get(file_uri, file_uri)
        if file_uri not in index:
            if not urlparse.urlparse(file_uri).scheme:
                rp = os.path.realpath(file_uri)
            else:
                rp = file_uri
            try:
                index[file_uri] = "toilfile:" + writeFunc(rp).pack()
                existing[index[file_uri]] = file_uri
            except Exception as e:
                logger.error("Got exception '%s' while copying '%s'", e, file_uri)
                raise
        return index[file_uri]


def path_to_loc(obj: Dict) -> None:
    """
    If a CWL object has a "path" and not a "location", make the
    path into a location instead.
    """
    if "location" not in obj and "path" in obj:
        obj["location"] = obj["path"]
        del obj["path"]


def import_files(
    import_function: Callable[[str], FileID],
    fs_access: cwltool.stdfsaccess.StdFsAccess,
    fileindex: Dict,
    existing: Dict,
    cwl_object: Dict,
    skip_broken: bool = False,
    bypass_file_store: bool = False,
) -> None:
    """
    From the leader or worker, prepare all files and directories inside the
    given CWL tool, order, or output object to be used on the workers. Make
    sure their sizes are set and import all the files.

    Recurses inside directories using the fs_access to find files to upload and
    subdirectory structure to encode, even if their listings are not set or not
    recursive.

    Preserves any listing fields.

    If a file cannot be found (like if it is an optional secondary file that
    doesn't exist), fails, unless skip_broken is set, in which case it leaves
    the location it was supposed to have been at.

    Also does some miscelaneous normalization.

    :param import_function: The function used to upload a file:// URI and get a
    Toil FileID for it.

    :param fs_access: the CWL FS access object we use to access the filesystem
    to find files to import.

    :param fileindex: Forward map to fill in from file URI to Toil storage
    location, used by write_file to deduplicate writes.

    :param existing: Reverse map to fill in from Toil storage location to file
    URI. Not read from.

    :param cwl_object: CWL tool (or workflow order) we are importing files for

    :param skip_broken: If True, when files can't be imported because they e.g.
    don't exist, leave their locations alone rather than failing with an error.

    :param bypass_file_store: If True, leave file:// URIs in place instead of
    importing files and directories.
    """

    tool_id = cwl_object.get("id", str(cwl_object))

    logger.debug("Importing files for %s", tool_id)

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
        return

    # Otherwise we actually want to put the things in the file store.

    def visit_file_or_directory_down(rec: MutableMapping) -> Optional[List]:
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
        """

        if rec.get("class", None) == "File":
            # Nothing to do!
            return None
        elif rec.get("class", None) == "Directory":
            # Pull out the old listing, if any
            old_listing = rec.get("listing", None)

            if not rec["location"].startswith("_:"):
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

            return old_listing

    def visit_file_or_directory_up(
        rec: MutableMapping,
        down_result: Optional[List],
        child_results: List[Dict[str, Union[str, Dict]]],
    ) -> Dict[str, Union[str, Dict]]:
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

            result = {}

            # Upload the file itself, which will adjust its location.
            upload_file(
                import_function, fileindex, existing, rec, skip_broken=skip_broken
            )

            # Make a record for this file under its name
            result[rec["basename"]] = rec["location"]

            for secondary_file_result in child_results:
                # Glom in the secondary files, if any
                result.update(secondary_file_result)

            return result

        elif rec.get("class", None) == "Directory":
            # This is a CWL Directory

            # Restore the original listing, or its absence
            rec["listing"] = down_result
            if rec["listing"] is None:
                del rec["listing"]

            # We know we have child results from a fully recursive listing.
            # Build them into a contents dict.
            contents = {}
            for child_result in child_results:
                # Keep each child file or directory or child file's secondary
                # file under its name
                contents.update(child_result)

            # Upload the directory itself, which will adjust its location.
            upload_directory(rec, contents, skip_broken=skip_broken)

            # Show those contents as being under our name in our parent.
            return {rec["basename"]: contents}

        else:
            raise RuntimeError("Got unexpected class of object: " + str(rec))

    # Process each file and directory in a recursive traversal
    visit_cwl_class_and_reduce(
        cwl_object,
        ("File", "Directory"),
        visit_file_or_directory_down,
        visit_file_or_directory_up,
    )


def upload_directory(
    directory_metadata: MutableMapping,
    directory_contents: Dict[str, Union[str, Dict]],
    skip_broken: bool = False,
) -> None:
    """
    Upload a Directory object.

    Ignores the listing (which may not be recursive and isn't safe or efficient
    to touch), and instead uses directory_contents, which is a recursive dict
    structure from filename to file URI or subdirectory contents dict.

    Makes sure the directory actually exists, and rewrites its location to be
    something we can use on another machine.

    We can't rely on the directory's listing as visible to the next tool as a
    complete recursive description of the files we will need to present to the
    tool, since some tools require it to be cleared or single-level but still
    expect to see its contents in the filesystem.
    """
    if (
        directory_metadata["location"].startswith("toilfile:")
        or directory_metadata["location"].startswith("toildir:")
        or directory_metadata["location"].startswith("_:")
    ):
        # Already in Toil; nothing to do
        return
    if not directory_metadata["location"] and directory_metadata["path"]:
        directory_metadata["location"] = schema_salad.ref_resolver.file_uri(
            directory_metadata["path"]
        )
    if directory_metadata["location"].startswith("file://") and not os.path.isdir(
        directory_metadata["location"][len("file://") :]
    ):
        if skip_broken:
            return
        else:
            raise cwltool.errors.WorkflowException(
                "Directory is missing: %s" % directory_metadata["location"]
            )

    logger.debug(
        "Uploading directory at %s with contents %s",
        directory_metadata["location"],
        directory_contents,
    )

    # Say that the directory location is just its dumped contents.
    # TODO: store these listings as files in the filestore instead?
    directory_metadata["location"] = "toildir:" + base64.urlsafe_b64encode(
        json.dumps(directory_contents).encode("utf-8")
    ).decode("utf-8")


def upload_file(
    uploadfunc: Any,
    fileindex: dict,
    existing: dict,
    file_metadata: dict,
    skip_broken: bool = False,
) -> None:
    """
    Update a file object so that the location is a reference to the toil file store.

    Write the file object to the file store if necessary.
    """
    if (
        file_metadata["location"].startswith("toilfile:")
        or file_metadata["location"].startswith("toildir:")
        or file_metadata["location"].startswith("_:")
    ):
        return
    if file_metadata["location"] in fileindex:
        file_metadata["location"] = fileindex[file_metadata["location"]]
        return
    if not file_metadata["location"] and file_metadata["path"]:
        file_metadata["location"] = schema_salad.ref_resolver.file_uri(
            file_metadata["path"]
        )
    if file_metadata["location"].startswith("file://") and not os.path.isfile(
        file_metadata["location"][len("file://") :]
    ):
        if skip_broken:
            return
        else:
            raise cwltool.errors.WorkflowException(
                "File is missing: %s" % file_metadata["location"]
            )
    file_metadata["location"] = write_file(
        uploadfunc, fileindex, existing, file_metadata["location"]
    )

    logger.debug("Sending file at: %s", file_metadata["location"])


def writeGlobalFileWrapper(file_store: AbstractFileStore, fileuri: str) -> str:
    """Wrap writeGlobalFile to accept file:// URIs."""
    fileuri = fileuri if ":/" in fileuri else f"file://{fileuri}"
    return file_store.writeGlobalFile(schema_salad.ref_resolver.uri_file_path(fileuri))


def remove_empty_listings(rec: CWLObjectType) -> None:
    if rec.get("class") != "Directory":
        finddirs = []  # type: List[CWLObjectType]
        visit_class(rec, ("Directory",), finddirs.append)
        for f in finddirs:
            remove_empty_listings(f)
        return
    if "listing" in rec and rec["listing"] == []:
        del rec["listing"]
        return


class ResolveIndirect(Job):
    """
    Helper Job.

    Accepts an unresolved dict (containing promises) and produces a dictionary
    of actual values.
    """

    def __init__(self, cwljob: dict):
        """Store the dictionary of promises for later resolution."""
        super(ResolveIndirect, self).__init__(cores=1, memory=1024 ^ 2, disk=0)
        self.cwljob = cwljob

    def run(self, file_store: AbstractFileStore) -> dict:
        """Evaluate the promises and return their values."""
        return resolve_dict_w_promises(self.cwljob)


def toilStageFiles(
    toil: Toil,
    cwljob: Union[Dict[Text, Any], List[Dict[Text, Any]]],
    outdir: str,
    destBucket: Union[str, None] = None,
) -> None:
    """
    Copy input files out of the global file store and update location and path.
    """

    def _collectDirEntries(
        obj: Union[Dict[Text, Any], List[Dict[Text, Any]]]
    ) -> Iterator[Dict[Text, Any]]:
        if isinstance(obj, dict):
            if obj.get("class") in ("File", "Directory"):
                yield obj
                for dir_entry in _collectDirEntries(obj.get("secondaryFiles", [])):
                    yield dir_entry
            else:
                for sub_obj in obj.values():
                    for dir_entry in _collectDirEntries(sub_obj):
                        yield dir_entry
        elif isinstance(obj, list):
            for sub_obj in obj:
                for dir_entry in _collectDirEntries(sub_obj):
                    yield dir_entry

    # This is all the CWL File and Directory objects we need to export.
    jobfiles = list(_collectDirEntries(cwljob))

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
        logger.debug("Staging output: %s", p)
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

                        with tempfile.NamedTemporaryFile() as f:
                            # Make a file with the right contents
                            f.write(file_id_or_contents.encode("utf-8"))
                            f.close()
                            # Import it and pack up the file ID so we can turn around and export it.
                            file_id_or_contents = (
                                "toilfile:" + toil.importFile(f.name).pack()
                            )

                    if file_id_or_contents.startswith("toildir:"):
                        # Get the directory contents and the path into them, if any
                        here, subpath, _ = decode_directory(file_id_or_contents)
                        if subpath is not None:
                            for part in subpath.split("/"):
                                here = here[part]
                        # At the end we should get a direct toilfile: URI
                        file_id_or_contents = here

                    if file_id_or_contents.startswith("toilfile:"):
                        # This is something we can export
                        destUrl = "/".join(s.strip("/") for s in [destBucket, baseName])
                        toil.exportFile(
                            FileID.unpack(file_id_or_contents[len("toilfile:") :]),
                            destUrl,
                        )
                    # TODO: can a toildir: "file" get here?
            else:
                # We are saving to the filesystem so we only really need exportFile for actual files.
                if not os.path.exists(p.target) and p.type in [
                    "Directory",
                    "WritableDirectory",
                ]:
                    os.makedirs(p.target)
                if not os.path.exists(p.target) and p.type in ["File", "WritableFile"]:
                    if p.resolved.startswith("toilfile:"):
                        # We can actually export this
                        os.makedirs(os.path.dirname(p.target), exist_ok=True)
                        toil.exportFile(
                            FileID.unpack(p.resolved[len("toilfile:") :]),
                            "file://" + p.target,
                        )
                    elif p.resolved.startswith("/"):
                        # Probably staging and bypassing file store. Just copy.
                        os.makedirs(os.path.dirname(p.target), exist_ok=True)
                        shutil.copyfile(p.resolved, p.target)
                    # TODO: can a toildir: "file" get here?
                if not os.path.exists(p.target) and p.type in [
                    "CreateFile",
                    "CreateWritableFile",
                ]:
                    # We just need to make a file with particular contents
                    os.makedirs(os.path.dirname(p.target), exist_ok=True)
                    with open(p.target, "wb") as n:
                        n.write(p.resolved.encode("utf-8"))

    def _check_adjust(f: dict) -> dict:
        f["location"] = schema_salad.ref_resolver.file_uri(pm.mapper(f["location"])[1])

        if "contents" in f:
            del f["contents"]
        return f

    visit_class(cwljob, ("File", "Directory"), _check_adjust)


class CWLJobWrapper(Job):
    """
    Wrap a CWL job that uses dynamic resources requirement.

    When executed, this creates a new child job which has the correct resource
    requirement set.
    """

    def __init__(
        self,
        tool: Process,
        cwljob: dict,
        runtime_context: cwltool.context.RuntimeContext,
        conditional: Union[Conditional, None] = None,
    ):
        """Store our context for later evaluation."""
        super(CWLJobWrapper, self).__init__(cores=1, memory=1024 * 1024, disk=8 * 1024)
        self.cwltool = remove_pickle_problems(tool)
        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.conditional = conditional

    def run(self, file_store: AbstractFileStore) -> Any:
        """Create a child job with the correct resource requirements set."""
        cwljob = resolve_dict_w_promises(self.cwljob, file_store)
        fill_in_defaults(
            self.cwltool.tool["inputs"],
            cwljob,
            self.runtime_context.make_fs_access(self.runtime_context.basedir or ""),
        )
        realjob = CWLJob(
            tool=self.cwltool,
            cwljob=cwljob,
            runtime_context=self.runtime_context,
            conditional=self.conditional,
        )
        self.addChild(realjob)
        return realjob.rv()


class CWLJob(Job):
    """Execute a CWL tool using cwltool.executors.SingleJobExecutor."""

    def __init__(
        self,
        tool: Process,
        cwljob: dict,
        runtime_context: cwltool.context.RuntimeContext,
        conditional: Union[Conditional, None] = None,
    ):
        """Store the context for later execution."""

        self.cwltool = remove_pickle_problems(tool)
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
                make_fs_access=runtime_context.make_fs_access,  # type: ignore
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
        # pass the default of None if basecommand is empty
        unitName = self.cwltool.tool.get("baseCommand", None)
        if isinstance(unitName, (MutableSequence, tuple)):
            unitName = " ".join(unitName)

        try:
            displayName = str(self.cwltool.tool["id"])
        except KeyError:
            displayName = None

        super(CWLJob, self).__init__(
            cores=req["cores"],
            memory=int(req["ram"] * (2 ** 20)),
            disk=int(
                (cast(int, req["tmpdirSize"]) * (2 ** 20))
                + (cast(int, req["outdirSize"]) * (2 ** 20))
            ),
            unitName=unitName,
            displayName=displayName,
        )

        self.cwljob = cwljob
        try:
            self.jobName = str(self.cwltool.tool["id"])
        except KeyError:
            # fall back to the Toil defined class name if the tool doesn't have
            # an identifier
            pass
        self.runtime_context = runtime_context
        self.step_inputs = self.cwltool.tool["inputs"]
        self.workdir = runtime_context.workdir  # type: ignore

    def required_env_vars(self, cwljob: Any) -> Iterator[Tuple[str, str]]:
        """Yield environment variables from EnvVarRequirement."""
        if isinstance(cwljob, dict):
            if cwljob.get("class") == "EnvVarRequirement":
                for t in cwljob.get("envDef", {}):
                    yield t["envName"], cast(str, self.builder.do_eval(t["envValue"]))
            for v in cwljob.values():
                for env_name, env_value in self.required_env_vars(v):
                    yield env_name, env_value
        if isinstance(cwljob, list):
            for env_var in cwljob:
                for env_name, env_value in self.required_env_vars(env_var):
                    yield env_name, env_value

    def populate_env_vars(self, cwljob: dict) -> dict:
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
            required_env_vars[
                k
            ] = v  # will tell cwltool which env vars to take from the environment
            os.environ[k] = v
            # needs to actually be populated in the environment as well or
            # they're not used

        # EnvVarRequirement env vars take priority over those specified with
        # "requirements" so cwltool.requirements need to be overwritten if an
        # env var with the same name is found
        for req in self.cwltool.requirements:
            for env_def in cast(Dict, req.get("envDef", {})):
                env_name = env_def.get("envName", "")
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

        logger.debug("Loaded order: %s", self.cwljob)

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
                List[Dict[str, str]], self.cwltool.inputs_record_schema["fields"]
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

        index = {}  # type: ignore
        existing = {}  # type: ignore

        # Prepare the run instructions for cwltool
        runtime_context = self.runtime_context.copy()

        runtime_context.basedir = os.getcwd()
        runtime_context.preserve_environment = required_env_vars

        if not runtime_context.bypass_file_store:
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

            # Intercept file and directory access and use a virtual filesystem
            # through the Toil FileStore.

            runtime_context.make_fs_access = functools.partial(
                ToilFsAccess, file_store=file_store
            )

        pipe_threads = []
        runtime_context.toil_get_file = functools.partial(  # type: ignore
            toil_get_file, file_store, index, existing, pipe_threads=pipe_threads
        )

        process_uuid = uuid.uuid4()  # noqa F841
        started_at = datetime.datetime.now()  # noqa F841

        logger.debug("Output disposition: %s", runtime_context.move_outputs)

        logger.debug("Running tool %s with order: %s", self.cwltool, self.cwljob)

        output, status = ToilSingleJobExecutor().execute(
            process=self.cwltool,
            job_order_object=cwljob,
            runtime_context=runtime_context,
            logger=cwllogger,
        )
        ended_at = datetime.datetime.now()  # noqa F841
        if status != "success":
            raise cwltool.errors.WorkflowException(status)

        for t, fd in pipe_threads:
            os.close(fd)
            t.join()

        # Get ahold of the filesystem
        fs_access = runtime_context.make_fs_access(runtime_context.basedir)

        # And a file importer that can go from a file:// URI to a Toil FileID
        file_import_function = functools.partial(writeGlobalFileWrapper, file_store)

        # Upload all the Files and set their and the Directories' locations, if
        # needed.
        import_files(
            file_import_function,
            fs_access,
            index,
            existing,
            output,
            bypass_file_store=runtime_context.bypass_file_store,
        )

        logger.debug("Emitting output: %s", output)

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


def makeJob(
    tool: Process,
    jobobj: dict,
    runtime_context: cwltool.context.RuntimeContext,
    conditional: Union[Conditional, None],
) -> tuple:
    """
    Create the correct Toil Job object for the CWL tool.

    Types: workflow, job, or job wrapper for dynamic resource requirements.

    :return: "wfjob, followOn" if the input tool is a workflow, and "job, job" otherwise
    """
    logger.debug("Make job for tool: %s", tool)
    scan_for_unsupported_requirements(
        tool, bypass_file_store=runtime_context.bypass_file_store
    )
    if tool.tool["class"] == "Workflow":
        wfjob = CWLWorkflow(
            cast(cwltool.workflow.Workflow, tool),
            jobobj,
            runtime_context,
            conditional=conditional,
        )
        followOn = ResolveIndirect(wfjob.rv())
        wfjob.addFollowOn(followOn)
        return wfjob, followOn
    else:
        resourceReq, _ = tool.get_requirement("ResourceRequirement")
        if resourceReq:
            for req in (
                "coresMin",
                "coresMax",
                "ramMin",
                "ramMax",
                "tmpdirMin",
                "tmpdirMax",
                "outdirMin",
                "outdirMax",
            ):
                r = resourceReq.get(req)
                if isinstance(r, str) and ("$(" in r or "${" in r):
                    # Found a dynamic resource requirement so use a job wrapper
                    job = CWLJobWrapper(
                        cast(ToilCommandLineTool, tool),
                        jobobj,
                        runtime_context,
                        conditional=conditional,
                    )
                    return job, job
        job = CWLJob(tool, jobobj, runtime_context, conditional)  # type: ignore
        return job, job


class CWLScatter(Job):
    """
    Implement workflow scatter step.

    When run, this creates a child job for each parameterization of the scatter.
    """

    def __init__(
        self,
        step: cwltool.workflow.WorkflowStep,
        cwljob: dict,
        runtime_context: cwltool.context.RuntimeContext,
        conditional: Union[Conditional, None],
    ):
        """Store our context for later execution."""
        super(CWLScatter, self).__init__(cores=1, memory=100 * 1024 ^ 2, disk=0)
        self.step = step
        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.conditional = conditional

    def flat_crossproduct_scatter(
        self, joborder: dict, scatter_keys: list, outputs: list, postScatterEval: Any
    ) -> None:
        """Cartesian product of the inputs, then flattened."""
        scatter_key = shortname(scatter_keys[0])
        for n in range(0, len(joborder[scatter_key])):
            updated_joborder = copy.copy(joborder)
            updated_joborder[scatter_key] = joborder[scatter_key][n]
            if len(scatter_keys) == 1:
                updated_joborder = postScatterEval(updated_joborder)
                subjob, followOn = makeJob(
                    tool=self.step.embedded_tool,
                    jobobj=updated_joborder,
                    runtime_context=self.runtime_context,
                    conditional=self.conditional,
                )
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                self.flat_crossproduct_scatter(
                    updated_joborder, scatter_keys[1:], outputs, postScatterEval
                )

    def nested_crossproduct_scatter(
        self, joborder: dict, scatter_keys: list, postScatterEval: Any
    ) -> list:
        """Cartesian product of the inputs."""
        scatter_key = shortname(scatter_keys[0])
        outputs = []
        for n in range(0, len(joborder[scatter_key])):
            updated_joborder = copy.copy(joborder)
            updated_joborder[scatter_key] = joborder[scatter_key][n]
            if len(scatter_keys) == 1:
                updated_joborder = postScatterEval(updated_joborder)
                subjob, followOn = makeJob(
                    tool=self.step.embedded_tool,
                    jobobj=updated_joborder,
                    runtime_context=self.runtime_context,
                    conditional=self.conditional,
                )
                self.addChild(subjob)
                outputs.append(followOn.rv())
            else:
                outputs.append(
                    self.nested_crossproduct_scatter(
                        updated_joborder, scatter_keys[1:], postScatterEval
                    )
                )
        return outputs

    def run(self, file_store: AbstractFileStore) -> list:
        """Generate the follow on scatter jobs."""
        cwljob = resolve_dict_w_promises(self.cwljob, file_store)

        if isinstance(self.step.tool["scatter"], str):
            scatter = [self.step.tool["scatter"]]
        else:
            scatter = self.step.tool["scatter"]

        scatterMethod = self.step.tool.get("scatterMethod", None)
        if len(scatter) == 1:
            scatterMethod = "dotproduct"
        outputs = []

        valueFrom = {
            shortname(i["id"]): i["valueFrom"]
            for i in self.step.tool["inputs"]
            if "valueFrom" in i
        }

        def postScatterEval(job_dict: dict) -> Any:
            shortio = {shortname(k): v for k, v in job_dict.items()}
            for k in valueFrom:
                job_dict.setdefault(k, None)

            def valueFromFunc(k: str, v: Any) -> Any:
                if k in valueFrom:
                    return cwltool.expression.do_eval(
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
            for i in range(0, len(cwljob[shortname(scatter[0])])):
                copyjob = copy.copy(cwljob)
                for sc in [shortname(x) for x in scatter]:
                    copyjob[sc] = cwljob[sc][i]
                copyjob = postScatterEval(copyjob)
                subjob, follow_on = makeJob(
                    tool=self.step.embedded_tool,
                    jobobj=copyjob,
                    runtime_context=self.runtime_context,
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
        outputs: Union[Mapping, MutableSequence],
    ):
        """Collect our context for later gathering."""
        super(CWLGather, self).__init__(cores=1, memory=10 * 1024 ^ 2, disk=0)
        self.step = step
        self.outputs = outputs

    @staticmethod
    def extract(obj: Union[Mapping, MutableSequence], k: str) -> list:
        """
        Extract the given key from the obj.

        If the object is a list, extract it from all members of the list.
        """
        if isinstance(obj, Mapping):
            return obj.get(k)
        elif isinstance(obj, MutableSequence):
            cp = []
            for item in obj:
                cp.append(CWLGather.extract(item, k))
            return cp
        else:
            return []

    def run(self, file_store: AbstractFileStore) -> Dict[str, Any]:
        """Gather all the outputs of the scatter."""
        outobj = {}

        def sn(n):
            if isinstance(n, Mapping):
                return shortname(n["id"])
            if isinstance(n, str):
                return shortname(n)

        for k in [sn(i) for i in self.step.tool["out"]]:
            outobj[k] = self.extract(self.outputs, k)

        return outobj


class SelfJob(Job):
    """Fake job object to facilitate implementation of CWLWorkflow.run()."""

    def __init__(self, j: "CWLWorkflow", v: dict):
        """Record the workflow and dictionary."""
        super(SelfJob, self).__init__(cores=1, memory=1024 ^ 2, disk=0)
        self.j = j
        self.v = v

    def rv(self, *path) -> Any:
        """Return our properties dictionary."""
        return self.v

    def addChild(self, c: str) -> Any:
        """Add a child to our workflow."""
        return self.j.addChild(c)

    def hasChild(self, c: str) -> Any:
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
    """Doc_loader does not pickle correctly, causing Toil errors, remove from objects."""
    if hasattr(obj, "doc_loader"):
        obj.doc_loader = None
    if isinstance(obj, cwltool.workflow.WorkflowStep):
        obj.embedded_tool = remove_pickle_problems(obj.embedded_tool)
    elif isinstance(obj, cwltool.workflow.Workflow):
        obj.steps = [remove_pickle_problems(s) for s in obj.steps]
    return obj


class CWLWorkflow(Job):
    """
    Toil Job to convert a CWL workflow graph into a Toil job graph.

    The Toil job graph will include the appropriate dependencies.
    """

    def __init__(
        self,
        cwlwf: cwltool.workflow.Workflow,
        cwljob: dict,
        runtime_context: cwltool.context.RuntimeContext,
        conditional: Union[Conditional, None] = None,
    ):
        """Gather our context for later execution."""
        super(CWLWorkflow, self).__init__(cores=1, memory=100 * 1024 ^ 2, disk=0)
        self.cwlwf = cwlwf
        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.cwlwf = remove_pickle_problems(self.cwlwf)
        self.conditional = conditional or Conditional()

    def run(self, file_store: AbstractFileStore):
        """
        Convert a CWL Workflow graph into a Toil job graph.

        Always runs on the leader, because the batch system knows to schedule
        it as a local job.
        """
        cwljob = resolve_dict_w_promises(self.cwljob, file_store)

        if self.conditional.is_false(cwljob):
            return self.conditional.skipped_outputs()

        # `promises` dict
        # from: each parameter (workflow input or step output)
        #   that may be used as a "source" for a step input workflow output
        #   parameter
        # to: the job that will produce that value.
        promises: Dict[str, Job] = {}

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
                if step.tool["id"] not in jobs:
                    stepinputs_fufilled = True
                    for inp in step.tool["inputs"]:
                        for s in aslist(inp.get("source", [])):
                            if s not in promises:
                                stepinputs_fufilled = False
                    if stepinputs_fufilled:
                        logger.debug(
                            "Ready to make job for workflow step %s", step.tool["id"]
                        )
                        jobobj = {}

                        for inp in step.tool["inputs"]:
                            logger.debug("Takes input: %s", inp["id"])
                            key = shortname(inp["id"])
                            if "source" in inp:
                                jobobj[key] = ResolveSource(
                                    name=f'{step.tool["id"]}/{key}',
                                    input=inp,
                                    source_key="source",
                                    promises=promises,
                                )

                            if "default" in inp:
                                jobobj[key] = DefaultWithSource(  # type: ignore
                                    copy.copy(inp["default"]), jobobj.get(key)
                                )

                            if "valueFrom" in inp and "scatter" not in step.tool:
                                jobobj[key] = StepValueFrom(  # type: ignore
                                    inp["valueFrom"],
                                    jobobj.get(key, JustAValue(None)),
                                    self.cwlwf.requirements,
                                    get_container_engine(self.runtime_context),
                                )

                        conditional = Conditional(
                            expression=step.tool.get("when"),
                            outputs=step.tool["out"],
                            requirements=self.cwlwf.requirements,
                            container_engine=get_container_engine(self.runtime_context),
                        )

                        if "scatter" in step.tool:
                            wfjob = CWLScatter(
                                step,
                                UnresolvedDict(jobobj),
                                self.runtime_context,
                                conditional=conditional,
                            )
                            followOn = CWLGather(step, wfjob.rv())
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
                                conditional=conditional,
                            )
                            logger.debug(
                                "Is non-scatter with job %s and follow-on %s",
                                wfjob,
                                followOn,
                            )

                        jobs[step.tool["id"]] = followOn

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


def visitSteps(
    cmdline_tool: Process,
    op: Callable[[Dict], Any],
) -> None:
    """
    Iterate over a CWL Process object, running the op on each tool description
    CWL object.
    """
    if isinstance(cmdline_tool, cwltool.workflow.Workflow):
        # For workflows we need to dispatch on steps
        for step in cmdline_tool.steps:
            # Handle the step's tool
            op(step.tool)
            # Recures on the embedded tool; maybe it's a workflow.
            visitSteps(step.embedded_tool, op)
    elif isinstance(cmdline_tool, cwltool.process.Process):
        # All CWL Process objects (including CommandLineTool) will have tools
        # if they bothered to run the Process __init__.
        op(cmdline_tool.tool)
    else:
        raise RuntimeError(
            f"Unsupported type encountered in workflow "
            f"traversal: {type(cmdline_tool)}"
        )


def rm_unprocessed_secondary_files(job_params: Any) -> None:
    if isinstance(job_params, list):
        for j in job_params:
            rm_unprocessed_secondary_files(j)
    if isinstance(job_params, dict) and "secondaryFiles" in job_params:
        job_params["secondaryFiles"] = filtered_secondary_files(job_params)


def filtered_secondary_files(unfiltered_secondary_files: dict) -> list:
    """
    Remove unprocessed secondary files.

    Interpolated strings and optional inputs in secondary files were added to
    CWL in version 1.1.

    The CWL libraries we call do successfully resolve the interpolated strings,
    but add the resolved fields to the list of unresolved fields so we remove
    them here after the fact.

    We keep secondary files using the 'toildir:', or '_:' protocols, or using
    the 'file:' protocol and indicating files or directories that actually
    exist. The 'required' logic seems to be handled deeper in
    cwltool.builder.Builder(), and correctly determines which files should be
    imported. Therefore we remove the files here and if this file is SUPPOSED
    to exist, it will still give the appropriate file does not exist error, but
    just a bit further down the track.
    """
    intermediate_secondary_files = []
    final_secondary_files = []
    # remove secondary files still containing interpolated strings
    for sf in unfiltered_secondary_files["secondaryFiles"]:
        sf_bn = sf.get("basename", "")
        sf_loc = sf.get("location", "")
        if ("$(" not in sf_bn) and ("${" not in sf_bn):
            if ("$(" not in sf_loc) and ("${" not in sf_loc):
                intermediate_secondary_files.append(sf)
    # remove secondary files that are not present in the filestore or pointing
    # to existant things on disk
    for sf in intermediate_secondary_files:
        sf_loc = sf.get("location", "")
        if (
            sf_loc.startswith("toilfile:")
            or sf_loc.startswith("toildir:")
            or sf_loc.startswith("_:")
            or sf.get("class", "") == "Directory"
        ):
            # Pass imported files, and all Directories
            final_secondary_files.append(sf)
        elif sf_loc.startswith("file:") and os.path.exists(
            schema_salad.ref_resolver.uri_file_path(sf_loc)
        ):
            # Pass things that exist on disk (which we presumably declined to
            # import because we aren't using the file store)
            final_secondary_files.append(sf)
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
        if req and is_mandatory:
            # The tool actualy uses this one, and it isn't just a hint.
            # Complain and explain.
            raise cwltool.errors.UnsupportedRequirement(
                "Toil cannot support InplaceUpdateRequirement when using the Toil file store. "
                "If you are running on a single machine, or a cluster with a shared filesystem, "
                "use the --bypass-file-store option to keep intermediate files on the filesystem. "
                "You can use the --tmp-outdir-prefix, --tmpdir-prefix, --outdir, and --jobStore "
                "options to control where on the filesystem files are placed, if only some parts of "
                "the filesystem are shared."
            )


def determine_load_listing(tool: Process):
    """
    Determine the directory.listing feature in CWL.

    In CWL, any input directory can have a DIRECTORY_NAME.listing (where
    DIRECTORY_NAME is any variable name) set to one of the following three
    options:

        no_listing: DIRECTORY_NAME.listing will be undefined.
            e.g. inputs.DIRECTORY_NAME.listing == unspecified

        shallow_listing: DIRECTORY_NAME.listing will return a list one level
                         deep of DIRECTORY_NAME's contents.
            e.g. inputs.DIRECTORY_NAME.listing == [items in directory]
                 inputs.DIRECTORY_NAME.listing[0].listing == undefined
                 inputs.DIRECTORY_NAME.listing.length == # of items in directory

        deep_listing: DIRECTORY_NAME.listing will return a list of the entire
                      contents of DIRECTORY_NAME.
            e.g. inputs.DIRECTORY_NAME.listing == [items in directory]
                 inputs.DIRECTORY_NAME.listing[0].listing == [items
                      in subdirectory if it exists and is the first item listed]
                 inputs.DIRECTORY_NAME.listing.length == # of items in directory

    See: https://www.commonwl.org/v1.1/CommandLineTool.html#LoadListingRequirement
         https://www.commonwl.org/v1.1/CommandLineTool.html#LoadListingEnum

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
    load_listing = tool.tool.get("loadListing", None) or load_listing_tool_req

    listing_choices = ("no_listing", "shallow_listing", "deep_listing")
    if load_listing not in listing_choices:
        raise ValueError(
            f'Unknown loadListing specified: "{load_listing}".  Valid choices: {listing_choices}'
        )
    return load_listing


class NoAvailableJobStoreException(Exception):
    """Indicates that no job store name is available."""

    pass


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

    try:
        if provisioner_name == "gce":
            # We can't use a local directory on Google cloud

            # Make sure we have the Google job store
            from toil.jobStores.googleJobStore import GoogleJobStore

            # Look for a project
            project = os.getenv("TOIL_GOOGLE_PROJECTID")
            project_part = (":" + project) if project else ""

            # Roll a randomn bucket name, possibly in the project.
            return f"google{project_part}:toil-cwl-{str(uuid.uuid4())}"
        elif provisioner_name == "aws" or batch_system_name in {"mesos", "kubernetes"}:
            # We can't use a local directory on AWS or on these cloud batch systems.
            # If we aren't provisioning on Google, we should try an AWS batch system.

            # Make sure we have AWS
            from toil.jobStores.aws.jobStore import AWSJobStore

            # Find a region
            from toil.provisioners.aws import get_current_aws_region

            region = get_current_aws_region()

            if not region:
                # We can't generate an AWS job store without a region
                situation += " running outside AWS with no TOIL_AWS_ZONE set"
                raise NoAvailableJobStoreException()

            # Roll a random name
            return f"aws:{region}:toil-cwl-{str(uuid.uuid4())}"
        elif provisioner_name is not None and provisioner_name not in ["aws", "gce"]:
            # We 've never heard of this provisioner and don't know what kind
            # of job store to use with it.
            raise NoAvailableJobStoreException()

    except (ImportError, NoAvailableJobStoreException):
        raise NoAvailableJobStoreException(
            f"Could not determine a job store appropriate for "
            f"{situation}. Please specify a jobstore with the "
            f"--jobStore option."
        )

    # Usually use the local directory and a file job store.
    return local_directory


usage_message = "\n\n" + textwrap.dedent(
    f"""
            * All positional arguments [cwl, yml_or_json] must always be specified last for toil-cwl-runner.
              Note: If you're trying to specify a jobstore, please use --jobStore.

                  Usage: toil-cwl-runner [options] example.cwl example-job.yaml
                  Example: toil-cwl-runner \\
                           --jobStore aws:us-west-2:jobstore \\
                           --realTimeLogging \\
                           --logInfo \\
                           example.cwl \\
                           example-job.yaml
            """[
        1:
    ]
)


def main(args: Union[List[str]] = None, stdout: TextIO = sys.stdout) -> int:
    """Run the main loop for toil-cwl-runner."""
    # Remove cwltool logger's stream handler so it uses Toil's
    cwllogger.removeHandler(defaultStreamHandler)

    if args is None:
        args = sys.argv[1:]

    config = Config()
    config.disableChaining = True
    config.cwl = True
    parser = argparse.ArgumentParser()
    addOptions(parser, config)
    parser.add_argument("cwltool", type=str)
    parser.add_argument("cwljob", nargs=argparse.REMAINDER)

    # Will override the "jobStore" positional argument, enables
    # user to select jobStore or get a default from logic one below.
    parser.add_argument("--jobStore", "--jobstore", dest="jobStore", type=str)
    parser.add_argument("--not-strict", action="store_true")
    parser.add_argument(
        "--enable-dev",
        action="store_true",
        help="Enable loading and running development versions of CWL",
    )
    parser.add_argument(
        "--enable-ext",
        action="store_true",
        help="Enable loading and running 'cwltool:' extensions to the CWL standards.",
        default=False,
    )
    parser.add_argument("--quiet", dest="logLevel", action="store_const", const="ERROR")
    parser.add_argument("--basedir", type=str)  # TODO: Might be hard-coded?
    parser.add_argument("--outdir", type=str, default=os.getcwd())
    parser.add_argument("--version", action="version", version=baseVersion)
    dockergroup = parser.add_mutually_exclusive_group()
    dockergroup.add_argument(
        "--user-space-docker-cmd",
        help="(Linux/OS X only) Specify a user space docker command (like "
        "udocker or dx-docker) that will be used to call 'pull' and 'run'",
    )
    dockergroup.add_argument(
        "--singularity",
        action="store_true",
        default=False,
        help="[experimental] Use Singularity runtime for running containers. "
        "Requires Singularity v2.6.1+ and Linux with kernel version v3.18+ or "
        "with overlayfs support backported.",
    )
    dockergroup.add_argument(
        "--podman",
        action="store_true",
        default=False,
        help="[experimental] Use Podman runtime for running containers. ",
    )
    dockergroup.add_argument(
        "--no-container",
        action="store_true",
        help="Do not execute jobs in a "
        "Docker container, even when `DockerRequirement` "
        "is specified under `hints`.",
    )
    dockergroup.add_argument(
        "--leave-container",
        action="store_false",
        default=True,
        help="Do not delete Docker container used by jobs after they exit",
        dest="rm_container",
    )
    cidgroup = parser.add_argument_group(
        "Options for recording the Docker container identifier into a file."
    )
    cidgroup.add_argument(
        # Disabled as containerid is now saved by default
        "--record-container-id",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
        dest="record_container_id",
    )

    cidgroup.add_argument(
        "--cidfile-dir",
        type=str,
        help="Store the Docker container ID into a file in the specified directory.",
        default=None,
        dest="cidfile_dir",
    )

    cidgroup.add_argument(
        "--cidfile-prefix",
        type=str,
        help="Specify a prefix to the container ID filename. "
        "Final file name will be followed by a timestamp. "
        "The default is no prefix.",
        default=None,
        dest="cidfile_prefix",
    )

    parser.add_argument(
        "--preserve-environment",
        type=str,
        nargs="+",
        help="Preserve specified environment variables when running"
        " CommandLineTools",
        metavar=("VAR1 VAR2"),
        default=("PATH",),
        dest="preserve_environment",
    )
    parser.add_argument(
        "--preserve-entire-environment",
        action="store_true",
        help="Preserve all environment variable when running CommandLineTools.",
        default=False,
        dest="preserve_entire_environment",
    )
    parser.add_argument(
        "--destBucket",
        type=str,
        help="Specify a cloud bucket endpoint for output files.",
    )
    parser.add_argument("--beta-dependency-resolvers-configuration", default=None)
    parser.add_argument("--beta-dependencies-directory", default=None)
    parser.add_argument("--beta-use-biocontainers", default=None, action="store_true")
    parser.add_argument("--beta-conda-dependencies", default=None, action="store_true")
    parser.add_argument(
        "--tmpdir-prefix",
        type=Text,
        help="Path prefix for temporary directories",
        default=DEFAULT_TMPDIR_PREFIX,
    )
    parser.add_argument(
        "--tmp-outdir-prefix",
        type=Text,
        help="Path prefix for intermediate output directories",
        default=DEFAULT_TMPDIR_PREFIX,
    )
    parser.add_argument(
        "--force-docker-pull",
        action="store_true",
        default=False,
        dest="force_docker_pull",
        help="Pull latest docker image even if it is locally present",
    )
    parser.add_argument(
        "--no-match-user",
        action="store_true",
        default=False,
        help="Disable passing the current uid to `docker run --user`",
    )
    parser.add_argument(
        "--no-read-only",
        action="store_true",
        default=False,
        help="Do not set root directory in the container as read-only",
    )
    parser.add_argument(
        "--strict-memory-limit",
        action="store_true",
        help="When running with "
        "software containers and the Docker engine, pass either the "
        "calculated memory allocation from ResourceRequirements or the "
        "default of 1 gigabyte to Docker's --memory option.",
    )
    parser.add_argument(
        "--relax-path-checks",
        action="store_true",
        default=False,
        help="Relax requirements on path names to permit "
        "spaces and hash characters.",
        dest="relax_path_checks",
    )
    parser.add_argument(
        "--default-container",
        help="Specify a default docker container that will be "
        "used if the workflow fails to specify one.",
    )
    parser.add_argument(
        "--eval-timeout",
        help="Time to wait for a Javascript expression to evaluate before giving "
        "an error, default 20s.",
        type=float,
        default=20,
    )
    parser.add_argument(
        "--overrides",
        type=str,
        default=None,
        help="Read process requirement overrides from file.",
    )

    parser.add_argument(
        "--mpi-config-file",
        type=str,
        default=None,
        help="Platform specific configuration for MPI (parallel "
        "launcher, its flag etc). See the cwltool README "
        "section 'Running MPI-based tools' for details of the format: "
        "https://github.com/common-workflow-language/cwltool#running-mpi-based-tools-that-need-to-be-launched",
    )
    parser.add_argument(
        "--bypass-file-store",
        action="store_true",
        default=False,
        help="Do not use Toil's file store and assume all "
        "paths are accessible in place from all nodes.",
        dest="bypass_file_store",
    )
    parser.add_argument(
        "--disable-streaming",
        action="store_true",
        default=False,
        help="Disable file streaming for files that have 'streamable' flag True",
        dest="disable_streaming",
    )

    provgroup = parser.add_argument_group(
        "Options for recording provenance information of the execution"
    )
    provgroup.add_argument(
        "--provenance",
        help="Save provenance to specified folder as a "
        "Research Object that captures and aggregates "
        "workflow execution and data products.",
        type=Text,
    )

    provgroup.add_argument(
        "--enable-user-provenance",
        default=False,
        action="store_true",
        help="Record user account info as part of provenance.",
        dest="user_provenance",
    )
    provgroup.add_argument(
        "--disable-user-provenance",
        default=False,
        action="store_false",
        help="Do not record user account info in provenance.",
        dest="user_provenance",
    )
    provgroup.add_argument(
        "--enable-host-provenance",
        default=False,
        action="store_true",
        help="Record host info as part of provenance.",
        dest="host_provenance",
    )
    provgroup.add_argument(
        "--disable-host-provenance",
        default=False,
        action="store_false",
        help="Do not record host info in provenance.",
        dest="host_provenance",
    )
    provgroup.add_argument(
        "--orcid",
        help="Record user ORCID identifier as part of "
        "provenance, e.g. https://orcid.org/0000-0002-1825-0097 "
        "or 0000-0002-1825-0097. Alternatively the environment variable "
        "ORCID may be set.",
        dest="orcid",
        default=os.environ.get("ORCID", ""),
        type=Text,
    )
    provgroup.add_argument(
        "--full-name",
        help="Record full name of user as part of provenance, "
        "e.g. Josiah Carberry. You may need to use shell quotes to preserve "
        "spaces. Alternatively the environment variable CWL_FULL_NAME may "
        "be set.",
        dest="cwl_full_name",
        default=os.environ.get("CWL_FULL_NAME", ""),
        type=Text,
    )
    # Problem: we want to keep our job store somewhere auto-generated based on
    # our options, unless overridden by... an option. So we will need to parse
    # options twice, because we need to feed the parser a job store.

    # Propose a local workdir, probably under /tmp.
    # mkdtemp actually creates the directory, but
    # toil requires that the directory not exist,
    # since it might become our jobstore,
    # so make it and delete it and allow
    # toil to create it again (!)
    workdir = tempfile.mkdtemp()
    os.rmdir(workdir)

    # we use the workdir as the default jobStore for the first parsing pass:
    options = parser.parse_args([workdir] + args)
    cwltool.main.setup_schema(args=options, custom_schema_callback=None)

    # Determine if our default will actually be in use
    using_default_job_store = options.jobStore == workdir

    # if tmpdir_prefix is not the default value, set workDir if unset, and move
    # workdir and the job store under it
    if options.tmpdir_prefix != DEFAULT_TMPDIR_PREFIX:
        workdir = cwltool.utils.create_tmp_dir(options.tmpdir_prefix)
        os.rmdir(workdir)

    if using_default_job_store:
        # Pick a default job store specifier appropriate to our choice of batch
        # system and provisioner and installed modules, given this available
        # local directory name. Fail if no good default can be used.
        chosen_job_store = generate_default_job_store(
            options.batchSystem, options.provisioner, workdir
        )
    else:
        # Since the default won't be used, just pass through the user's choice
        chosen_job_store = options.jobStore

    # Re-parse arguments with the new selected jobstore.
    options = parser.parse_args([chosen_job_store] + args)
    options.doc_cache = True
    options.disable_js_validation = False
    options.do_validate = True
    options.pack = False
    options.print_subgraph = False
    if options.tmpdir_prefix != DEFAULT_TMPDIR_PREFIX and options.workDir is None:
        # We need to override workDir because by default Toil will pick
        # somewhere under the system temp directory if unset, ignoring
        # --tmpdir-prefix.
        #
        # If set, workDir needs to exist, so we directly use the prefix
        options.workDir = cwltool.utils.create_tmp_dir(options.tmpdir_prefix)

    if options.batchSystem == "kubernetes":
        # Containers under Kubernetes can only run in Singularity
        options.singularity = True

    if options.logLevel:
        # Make sure cwltool uses Toil's log level.
        # Applies only on the leader.
        cwllogger.setLevel(options.logLevel.upper())

    logger.debug(
        f"Using job store {chosen_job_store} from workdir {workdir} with default status {using_default_job_store}"
    )
    logger.debug(f"Final job store {options.jobStore} and workDir {options.workDir}")

    outdir = os.path.abspath(options.outdir)
    tmp_outdir_prefix = os.path.abspath(options.tmp_outdir_prefix)

    fileindex = dict()  # type: ignore
    existing = dict()  # type: ignore
    conf_file = getattr(options, "beta_dependency_resolvers_configuration", None)
    use_conda_dependencies = getattr(options, "beta_conda_dependencies", None)
    job_script_provider = None
    if conf_file or use_conda_dependencies:
        dependencies_configuration = DependenciesConfiguration(options)
        job_script_provider = dependencies_configuration

    options.default_container = None
    runtime_context = cwltool.context.RuntimeContext(vars(options))
    runtime_context.toplevel = True  # enable discovery of secondaryFiles
    runtime_context.find_default_container = functools.partial(
        find_default_container, options
    )
    runtime_context.workdir = workdir  # type: ignore
    runtime_context.move_outputs = "leave"
    runtime_context.rm_tmpdir = False
    runtime_context.streaming_allowed = not options.disable_streaming
    if options.mpi_config_file is not None:
        runtime_context.mpi_config = MpiConfig.load(options.mpi_config_file)
    runtime_context.bypass_file_store = options.bypass_file_store
    if options.bypass_file_store and options.destBucket:
        # We use the file store to write to buckets, so we can't do this (yet?)
        logger.error("Cannot export outputs to a bucket when bypassing the file store")
        return 1

    loading_context = cwltool.main.setup_loadingContext(None, runtime_context, options)

    if options.provenance:
        research_obj = cwltool.provenance.ResearchObject(
            temp_prefix_ro=options.tmp_outdir_prefix,
            orcid=options.orcid,
            full_name=options.cwl_full_name,
            fsaccess=runtime_context.make_fs_access(""),
        )
        runtime_context.research_obj = research_obj

    with Toil(options) as toil:
        if options.restart:
            try:
                outobj = toil.restart()
            except Exception as err:
                # TODO: We can't import FailedJobsException due to a circular
                # import but that's what we'd expect here.
                if getattr(err, "exit_code") == CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE:
                    # We figured out that we can't support this workflow.
                    logging.error(err)
                    logging.error(
                        "Your workflow uses a CWL requirement that Toil does not support!"
                    )
                    return CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
                else:
                    raise
        else:
            loading_context.hints = [
                {
                    "class": "ResourceRequirement",
                    "coresMin": toil.config.defaultCores,
                    "ramMin": toil.config.defaultMemory / (2 ** 20),
                    "outdirMin": toil.config.defaultDisk / (2 ** 20),
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
                        file_uri(os.path.abspath(options.overrides)), tool_file_uri
                    )
                )

            loading_context, workflowobj, uri = cwltool.load_tool.fetch_document(
                uri, loading_context
            )
            loading_context, uri = cwltool.load_tool.resolve_and_validate_document(
                loading_context, workflowobj, uri
            )

            processobj, metadata = loading_context.loader.resolve_ref(uri)
            processobj = cast(Union[CommentedMap, CommentedSeq], processobj)

            document_loader = loading_context.loader
            metadata = loading_context.metadata

            if options.provenance and runtime_context.research_obj:
                runtime_context.research_obj.packed_workflow(
                    cwltool.main.print_pack(loading_context, uri)
                )

            try:
                tool = cwltool.load_tool.make_tool(uri, loading_context)
                scan_for_unsupported_requirements(
                    tool, bypass_file_store=options.bypass_file_store
                )
            except cwltool.errors.UnsupportedRequirement as err:
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
            except SystemExit as e:
                if e.code == 2:  # raised by argparse's parse_args() function
                    print(
                        "\nIf both a CWL file and an input object (YAML/JSON) file were "
                        "provided, this may be the argument order." + usage_message,
                        file=sys.stderr,
                    )
                raise

            fs_access = cwltool.stdfsaccess.StdFsAccess(options.basedir)
            fill_in_defaults(tool.tool["inputs"], initialized_job_order, fs_access)

            for inp in tool.tool["inputs"]:
                if (
                    shortname(inp["id"]) in initialized_job_order
                    and inp["type"] == "File"
                ):
                    initialized_job_order[shortname(inp["id"])]["streamable"] = inp.get(
                        "streamable", False
                    )  # TODO also for nested types that contain streamable Files

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
            builder = tool._init_job(initialized_job_order, runtime_context)

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

            # Define something we can call to import a file and get its file
            # ID.
            file_import_function = functools.partial(toil.importFile, symlink=True)

            # Import all the input files, some of which may be missing optional
            # files.
            import_files(
                file_import_function,
                fs_access,
                fileindex,
                existing,
                initialized_job_order,
                skip_broken=True,
                bypass_file_store=options.bypass_file_store,
            )
            # Import all the files associated with tools (binaries, etc.).
            # Not sure why you would have an optional secondary file here, but
            # the spec probably needs us to support them.
            visitSteps(
                tool,
                functools.partial(
                    import_files,
                    file_import_function,
                    fs_access,
                    fileindex,
                    existing,
                    skip_broken=True,
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

            try:
                wf1, _ = makeJob(
                    tool=tool,
                    jobobj={},
                    runtime_context=runtime_context,
                    conditional=None,
                )
            except cwltool.errors.UnsupportedRequirement as err:
                logging.error(err)
                return CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
            wf1.cwljob = initialized_job_order
            try:
                outobj = toil.start(wf1)
            except Exception as err:
                # TODO: We can't import FailedJobsException due to a circular
                # import but that's what we'd expect here.
                if getattr(err, "exit_code") == CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE:
                    # We figured out that we can't support this workflow.
                    logging.error(err)
                    logging.error(
                        "Your workflow uses a CWL requirement that Toil does not support!"
                    )
                    return CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE
                else:
                    raise

        # Now the workflow has completed. We need to make sure the outputs (and
        # inputs) end up where the user wants them to be.

        outobj = resolve_dict_w_promises(outobj)

        # Stage files. Specify destination bucket if specified in CLI
        # options. If destination bucket not passed in,
        # options.destBucket's value will be None.
        toilStageFiles(toil, outobj, outdir, destBucket=options.destBucket)

        if runtime_context.research_obj is not None:
            runtime_context.research_obj.create_job(outobj, True)

            def remove_at_id(doc):
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
            prov_dependencies = cwltool.main.prov_deps(
                workflowobj, document_loader, uri
            )
            runtime_context.research_obj.generate_snapshot(prov_dependencies)
            runtime_context.research_obj.close(options.provenance)

        if not options.destBucket:
            visit_class(
                outobj,
                ("File",),
                functools.partial(
                    compute_checksums, cwltool.stdfsaccess.StdFsAccess("")
                ),
            )

        visit_class(outobj, ("File",), MutationManager().unset_generation)
        stdout.write(json.dumps(outobj, indent=4, default=str))

    return 0


def find_default_container(
    args: argparse.Namespace, builder: cwltool.builder.Builder
) -> str:
    """Find the default constuctor by consulting a Toil.options object."""
    if args.default_container:
        return args.default_container
    if args.beta_use_biocontainers:
        return get_container_from_software_requirements(True, builder)
    return None
