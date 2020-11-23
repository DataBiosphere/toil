"""Implemented support for Common Workflow Language (CWL) for Toil."""
# Copyright (C) 2015 Curoverse, Inc
# Copyright (C) 2016-2020 Regents of the University of California
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
import copy
import datetime
import functools
import json
import logging
import os
import stat
import sys
import tempfile
import urllib
import uuid
import shutil
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
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
import cwltool.errors
import cwltool.expression
import cwltool.load_tool
import cwltool.main
import cwltool.provenance
import cwltool.resolver
import cwltool.stdfsaccess
import cwltool.workflow
import schema_salad.ref_resolver
from cwltool.loghandler import _logger as cwllogger
from cwltool.loghandler import defaultStreamHandler
from cwltool.mutation import MutationManager
from cwltool.pathmapper import MapperEnt, PathMapper, downloadHttpFile
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
    CWLOutputAtomType,
    CWLObjectType,
    adjustDirObjs,
    adjustFileObjs,
    aslist,
    convert_pathsep_to_unix,
    get_listing,
    normalizeFilesDirs,
    visit_class,
)
from ruamel.yaml.comments import CommentedMap
from schema_salad import validate
from schema_salad.schema import Names
from schema_salad.sourceline import SourceLine

from toil.common import Config, Toil, addOptions
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import Job
from toil.jobStores.abstractJobStore import NoSuchFileException, NoSuchJobStoreException
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


def cwltoil_was_removed():
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


class UnresolvedDict(dict):
    """Tag to indicate a dict contains promises that must be resolved."""

    pass


class SkipNull:
    """
    Internal sentinel object.

    Indicates a null value produced by each port of a skipped conditional step.
    The CWL 1.2 specification calls for treating this the exactly the same as a
    null value.
    """

    pass


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
            "In %s, SkipNull result found and cast to None. \n"
            "You had a conditional step that did not run, "
            "but you did not use pickValue to handle the skipped input." % name
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
        outputs: Union[dict, None] = None,
        requirements: List[CWLObjectType] = [],
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

    def is_false(self, job: Union[dict, None]) -> bool:
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
        )

        if isinstance(expr_is_true, bool):
            return not expr_is_true

        raise cwltool.errors.WorkflowException(
            "'%s' evaluated to a non-boolean value" % self.expression
        )

    def skipped_outputs(self) -> dict:
        """
        Generate a dict of SkipNull objects corresponding to the output structure of the step.

        :return: dict
        """
        outobj = {}

        def sn(n):
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
            raise validate.ValidationException(
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

    def __init__(self, expr: str, source: Any, req: List[CWLObjectType]):
        """
        Instantiate an object to carry all know about this valueFrom expression.

        :param expr: str: expression as a string
        :param source: the source promise of this step
        :param req: requirements object that is consumed by CWLtool expression evaluator
        """
        self.expr = expr
        self.source = source
        self.context = None
        self.req = req

    def eval_prep(self, step_inputs: dict, file_store: AbstractFileStore):
        """
        Resolve the contents of any file in a set of inputs.

        The inputs must be associated with the StepValueFrom object's self.source.

        Called when loadContents is specified.

        :param step_inputs: Workflow step inputs.
        :param file_store: A toil file store, needed to resolve toil fs:// paths.
        """
        for v in step_inputs.values():
            val = cast(CWLObjectType, v)
            source_input = getattr(self.source, "input", {})
            if isinstance(val, dict) and isinstance(source_input, dict):
                if (
                    val.get("contents") is None
                    and source_input.get("loadContents") is True
                ):
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
            self.expr, inputs, self.req, None, None, {}, context=self.context
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

    # '_:' prefixed file paths are a signal to cwltool to create folders in place
    # rather than copying them, so we make them here
    for entry in result:
        if isinstance(result[entry], dict):
            location = result[entry].get("location")
            if location:
                if location.startswith("_:file://"):
                    local_dir_path = location[len("_:file://") :]
                    os.makedirs(local_dir_path, exist_ok=True)
                    result[entry]["location"] = local_dir_path
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

    Maps the symbolic identifier of a file (the Toil FileID), its local path on
    the host (the value returned by readGlobalFile) and the the location of the
    file inside the software container.
    """

    def __init__(
        self,
        referenced_files: list,
        basedir: str,
        stagedir: str,
        separateDirs: bool = True,
        get_file: Union[Any, None] = None,
        stage_listing: bool = False,
    ):
        """Initialize this ToilPathMapper."""
        self.get_file = get_file
        self.stage_listing = stage_listing
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
        """Iterate over a CWL object, resolving File and Directory path references."""
        tgt = convert_pathsep_to_unix(
            os.path.join(stagedir, cast(str, obj["basename"]))
        )
        if obj["location"] in self._pathmap:
            return
        if obj["class"] == "Directory":
            location = cast(str, obj["location"])
            if location.startswith("file://"):
                resolved = schema_salad.ref_resolver.uri_file_path(location)
            else:
                resolved = location
            self._pathmap[location] = MapperEnt(
                resolved, tgt, "WritableDirectory" if copy else "Directory", staged
            )

            if location.startswith("file://") and not self.stage_listing:
                staged = False

            self.visitlisting(
                cast(List, obj.get("listing", [])),
                tgt,
                basedir,
                copy=copy,
                staged=staged,
            )

        elif obj["class"] == "File":
            path = cast(str, obj["location"])
            ab = cwltool.stdfsaccess.abspath(path, basedir)
            if "contents" in obj and path.startswith("_:"):
                self._pathmap[path] = MapperEnt(
                    cast(str, obj["contents"]),
                    tgt,
                    "CreateWritableFile" if copy else "CreateFile",
                    # "CreateFile",  # TODO: Allow "WritableFile" here; see base class
                    staged,
                )
            else:
                with SourceLine(
                    obj,
                    "location",
                    validate.ValidationException,
                    logger.isEnabledFor(logging.DEBUG),
                ):
                    deref = self.get_file(path) if self.get_file else ab
                    if deref.startswith("file:"):
                        deref = schema_salad.ref_resolver.uri_file_path(deref)
                    if urllib.parse.urlsplit(deref).scheme in ["http", "https"]:
                        deref = downloadHttpFile(path)
                    elif urllib.parse.urlsplit(deref).scheme != "toilfs":
                        # Dereference symbolic links
                        st = os.lstat(deref)
                        while stat.S_ISLNK(st.st_mode):
                            rl = os.readlink(deref)
                            deref = (
                                rl
                                if os.path.isabs(rl)
                                else os.path.join(os.path.dirname(deref), rl)
                            )
                            st = os.lstat(deref)

                    self._pathmap[path] = MapperEnt(
                        deref, tgt, "WritableFile" if copy else "File", staged
                    )

            self.visitlisting(
                cast(List[CWLObjectType], obj.get("secondaryFiles", [])),
                stagedir,
                basedir,
                copy=copy,
                staged=staged,
            )


class ToilCommandLineTool(cwltool.command_line_tool.CommandLineTool):
    """Subclass the cwltool command line tool to provide the custom Toil.PathMapper."""

    def make_path_mapper(
        self,
        reffiles: List[Any],
        stagedir: str,
        runtimeContext: cwltool.context.RuntimeContext,
        separateDirs: bool,
    ) -> cwltool.pathmapper.PathMapper:
        """Create the appropriate ToilPathMapper for the situation."""
        return ToilPathMapper(
            reffiles,
            runtimeContext.basedir,
            stagedir,
            separateDirs,
            runtimeContext.toil_get_file,  # type: ignore
        )


def toil_make_tool(
    toolpath_object: CommentedMap,
    loadingContext: cwltool.context.LoadingContext,
) -> Process:
    """
    Emit custom ToilCommandLineTools.

    This factory funciton is meant to be passed to cwltool.load_tool().
    """
    if (
        isinstance(toolpath_object, Mapping)
        and toolpath_object.get("class") == "CommandLineTool"
    ):
        return ToilCommandLineTool(toolpath_object, loadingContext)
    return cwltool.workflow.default_make_tool(toolpath_object, loadingContext)


class ToilFsAccess(cwltool.stdfsaccess.StdFsAccess):
    """Custom filesystem access class which handles toil filestore references."""

    def __init__(self, basedir: str, file_store: AbstractFileStore = None):
        """Create a FsAccess object for the given Toil Filestore and basedir."""
        self.file_store = file_store
        super(ToilFsAccess, self).__init__(basedir)

    def exists(self, path: str) -> bool:
        """Test for file existance."""
        # toil's _abs() throws errors when files are not found and cwltool's _abs() does not
        try:
            return os.path.exists(self._abs(path))
        except NoSuchFileException:
            return False

    def realpath(self, path: str) -> str:
        if path.startswith("toilfs:"):
            # import the file and make it available locally if it exists
            path = self._abs(path)
        elif path.startswith("_:"):
            return path
        return os.path.realpath(path)

    def listdir(self, fn: str) -> List[str]:
        directory = self._abs(fn)
        if fn.startswith("_:file://"):
            directory = fn[len("_:file://") :]
            if os.path.isdir(directory):
                return [
                    cwltool.stdfsaccess.abspath(urllib.parse.quote(entry), fn)
                    for entry in os.listdir(directory)
                ]
            else:
                return []
        else:
            return [
                cwltool.stdfsaccess.abspath(urllib.parse.quote(entry), fn)
                for entry in os.listdir(self._abs(directory))
            ]

    def _abs(self, path: str) -> str:
        """
        Return a local absolute path for a file (no schema).

        Overwrites cwltool.stdfsaccess.StdFsAccess._abs() to account for toil specific schema.
        """
        # Used to fetch a path to determine if a file exists in the inherited
        # cwltool.stdfsaccess.StdFsAccess, (among other things) so this should
        # not error on missing files.
        # See: https://github.com/common-workflow-language/cwltool/blob/beab66d649dd3ee82a013322a5e830875e8556ba/cwltool/stdfsaccess.py#L43  # noqa B950
        if path.startswith("toilfs:"):
            logger.debug("Need to download file to get a local absolute path.")
            destination = self.file_store.readGlobalFile(FileID.unpack(path[7:]))
            logger.debug("Downloaded %s to %s", path, destination)
            if not os.path.exists(destination):
                raise RuntimeError(
                    f"{destination} does not exist after filestore import."
                )
        elif path.startswith("_:file://"):
            destination = path
        else:
            destination = super(ToilFsAccess, self)._abs(path)
        return destination


def toil_get_file(
    file_store: AbstractFileStore, index: dict, existing: dict, file_store_id: str
) -> str:
    """Get path to input file from Toil jobstore."""
    if not file_store_id.startswith("toilfs:"):
        return file_store.jobStore.getPublicUrl(
            file_store.jobStore.importFile(file_store_id)
        )
    src_path = file_store.readGlobalFile(FileID.unpack(file_store_id[7:]))
    index[src_path] = file_store_id
    existing[file_store_id] = src_path
    return schema_salad.ref_resolver.file_uri(src_path)


def write_file(writeFunc: Any, index: dict, existing: dict, file_uri: str) -> str:
    """
    Write a file into the Toil jobstore.

    'existing' is a set of files retrieved as inputs from toil_get_file. This
    ensures they are mapped back as the same name if passed through.

    Returns a toil uri path to the object.
    """
    # Toil fileStore reference
    if file_uri.startswith("toilfs:"):
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
                index[file_uri] = "toilfs:" + writeFunc(rp).pack()
                existing[index[file_uri]] = file_uri
            except Exception as e:
                logger.error("Got exception '%s' while copying '%s'", e, file_uri)
                raise
        return index[file_uri]


def prepareDirectoryForUpload(
    directory_metadata: dict, skip_broken: bool = False
) -> None:
    """
    Prepare a Directory object to be uploaded.

    Assumes listings are already filled in.

    Makes sure the directory actually exists, and rewrites its location to be
    something we can use on another machine.

    Since Files and sub-Directories are already tracked by the directory's
    listing, we just need some sentinel path to represent the existence of a
    directory coming from Toil and not the local filesystem.
    """
    if directory_metadata["location"].startswith("toilfs:") or directory_metadata[
        "location"
    ].startswith("_:"):
        # Already in Toil; nothing to do
        return
    if not directory_metadata["location"] and directory_metadata["path"]:
        directory_metadata["location"] = schema_salad.ref_resolver.file_uri(
            directory_metadata["path"]
        )
    if directory_metadata["location"].startswith("file://") and not os.path.isdir(
        directory_metadata["location"][7:]
    ):
        if skip_broken:
            return
        else:
            raise cwltool.errors.WorkflowException(
                "Directory is missing: %s" % directory_metadata["location"]
            )

    # The metadata for a directory is all we need to keep around for it. It
    # doesn't have a real location. But each directory needs a unique location
    # or cwltool won't ship the metadata along. cwltool takes "_:" as a signal
    # to make directories instead of copying from somewhere. So we give every
    # directory a unique _: location and cwltool's machinery Just Works.
    directory_metadata["location"] = "_:" + directory_metadata["location"]

    logger.debug("Sending directory at %s", directory_metadata["location"])


def uploadFile(
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
    if file_metadata["location"].startswith("toilfs:") or file_metadata[
        "location"
    ].startswith("_:"):
        return
    if file_metadata["location"] in fileindex:
        file_metadata["location"] = fileindex[file_metadata["location"]]
        return
    if not file_metadata["location"] and file_metadata["path"]:
        file_metadata["location"] = schema_salad.ref_resolver.file_uri(
            file_metadata["path"]
        )
    if file_metadata["location"].startswith("file://") and not os.path.isfile(
        file_metadata["location"][7:]
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
        super(ResolveIndirect, self).__init__()
        self.cwljob = cwljob

    def run(self, file_store: AbstractFileStore) -> dict:
        """Evaluate the promises and return their values."""
        return resolve_dict_w_promises(self.cwljob)


def toilStageFiles(
    file_store: AbstractFileStore,
    cwljob: Union[Dict[Text, Any], List[Dict[Text, Any]]],
    outdir: str,
    destBucket: Union[str, None] = None,
) -> None:
    """Copy input files out of the global file store and update location and path."""

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

    jobfiles = list(_collectDirEntries(cwljob))
    pm = ToilPathMapper(jobfiles, "", outdir, separateDirs=False, stage_listing=True)
    for _, p in pm.items():
        if p.staged:
            if destBucket and p.type in ["File", "CreateFile"]:
                # Directories don't need to be created if we're exporting to a bucket
                baseName = p.target[len(outdir) :]
                local_file_path = p.resolved[len("file://") :]

                if (
                    p.type == "CreateFile"
                ):  # TODO: CreateFile for buckets is not under testing
                    local_file_path = os.path.join(
                        file_store.getLocalTempDir(), baseName
                    )
                    with open(local_file_path, "wb") as n:
                        n.write(p.resolved.encode("utf-8"))

                destUrl = "/".join(s.strip("/") for s in [destBucket, baseName])
                file_store.exportFile(FileID.unpack(local_file_path), destUrl)
            else:
                if not os.path.exists(p.target) and p.type == "Directory":
                    os.makedirs(p.target)
                if not os.path.exists(p.target) and p.type == "File":
                    os.makedirs(os.path.dirname(p.target), exist_ok=True)
                    file_store.exportFile(
                        FileID.unpack(p.resolved[7:]), "file://" + p.target
                    )
                if not os.path.exists(p.target) and p.type == "CreateFile":
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
        tool: ToilCommandLineTool,
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
        tool: ToilCommandLineTool,
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
                mutation_manager=None,
                formatgraph=None,
                make_fs_access=runtime_context.make_fs_access,  # type: ignore
                fs_access=runtime_context.make_fs_access(""),
                job_script_provider=None,
                timeout=runtime_context.eval_timeout,
                debug=False,
                js_console=False,
                force_docker_pull=False,
                loadListing=determine_load_listing(tool),
                outdir="",
                tmpdir="/tmp",  # TODO: use actual defaults here
                stagedir="/var/lib/cwl",  # TODO: use actual defaults here
                cwlVersion=cast(str, self.cwltool.metadata["cwlVersion"]),
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

        adjustDirObjs(
            cwljob,
            functools.partial(remove_empty_listings),
        )

        # Exports temporary directory for batch systems that reset TMPDIR
        os.environ["TMPDIR"] = os.path.realpath(file_store.getLocalTempDir())
        outdir = os.path.join(file_store.getLocalTempDir(), "out")
        os.mkdir(outdir)
        # Just keep the temporary output prefix under the job's local temp dir,
        # next to the outdir.
        #
        # If we maintain our own system of nested temp directories, we won't
        # know when all the jobs using a higher-level directory are ready for
        # it to be deleted. The local temp dir, under Toil's workDir, will be
        # cleaned up by Toil.
        tmp_outdir_prefix = os.path.join(file_store.getLocalTempDir(), "tmp-out")

        index = {}  # type: ignore
        existing = {}  # type: ignore
        # Prepare the run instructions for cwltool
        runtime_context = self.runtime_context.copy()
        runtime_context.basedir = os.getcwd()
        runtime_context.outdir = outdir
        runtime_context.tmp_outdir_prefix = tmp_outdir_prefix
        runtime_context.tmpdir_prefix = file_store.getLocalTempDir()
        runtime_context.make_fs_access = functools.partial(
            ToilFsAccess, file_store=file_store
        )
        runtime_context.preserve_environment = required_env_vars

        runtime_context.toil_get_file = functools.partial(  # type: ignore
            toil_get_file, file_store, index, existing
        )

        process_uuid = uuid.uuid4()  # noqa F841
        started_at = datetime.datetime.now()  # noqa F841

        logger.debug("Running CWL job: %s", cwljob)

        output, status = cwltool.executors.SingleJobExecutor().execute(
            process=self.cwltool,
            job_order_object=cwljob,
            runtime_context=runtime_context,
            logger=cwllogger,
        )
        ended_at = datetime.datetime.now()  # noqa F841
        if status != "success":
            raise cwltool.errors.WorkflowException(status)

        adjustDirObjs(
            output,
            functools.partial(
                get_listing, cwltool.stdfsaccess.StdFsAccess(outdir), recursive=True
            ),
        )

        adjustDirObjs(output, prepareDirectoryForUpload)

        # write the outputs into the jobstore
        adjustFileObjs(
            output,
            functools.partial(
                uploadFile,
                functools.partial(writeGlobalFileWrapper, file_store),
                index,
                existing,
            ),
        )

        # metadata[process_uuid] = {
        #     'started_at': started_at,
        #     'ended_at': ended_at,
        #     'job_order': cwljob,
        #     'outputs': output,
        #     'internal_name': self.jobName
        # }
        return output


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
        job = CWLJob(tool, jobobj, runtime_context, conditional=conditional)  # type: ignore
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
        super(CWLScatter, self).__init__()
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
                raise validate.ValidationException(
                    "Unsupported complex scatter type '%s'" % scatterMethod
                )
            else:
                raise validate.ValidationException(
                    "Must provide scatterMethod to scatter over multiple" " inputs."
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
        super(CWLGather, self).__init__()
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
    Toil Job to convert a CWL workflow grah into a Toil job graph.

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
        super(CWLWorkflow, self).__init__()
        self.cwlwf = cwlwf
        self.cwljob = cwljob
        self.runtime_context = runtime_context
        self.cwlwf = remove_pickle_problems(self.cwlwf)
        self.conditional = conditional or Conditional()

    def run(self, file_store: AbstractFileStore):
        """Convert a CWL Workflow graph into a Toil job graph."""
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
            # dependencies can be fufilled by upstream workflow inputs or
            # step outputs. Loop exits when the workflow outputs
            # are satisfied.

            all_outputs_fulfilled = True

            for step in self.cwlwf.steps:
                if step.tool["id"] not in jobs:
                    stepinputs_fufilled = True
                    for inp in step.tool["inputs"]:
                        if "source" in inp:
                            for s in aslist(inp["source"]):
                                if s not in promises:
                                    stepinputs_fufilled = False
                    if stepinputs_fufilled:
                        jobobj = {}

                        for inp in step.tool["inputs"]:
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
                                )

                        conditional = Conditional(
                            expression=step.tool.get("when"),
                            outputs=step.tool["out"],
                            requirements=self.cwlwf.requirements,
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
                        else:
                            wfjob, followOn = makeJob(
                                tool=step.embedded_tool,
                                jobobj=UnresolvedDict(jobobj),
                                runtime_context=self.runtime_context,
                                conditional=conditional,
                            )

                        jobs[step.tool["id"]] = followOn

                        connected = False
                        for inp in step.tool["inputs"]:
                            for s in aslist(inp.get("source", [])):
                                if isinstance(
                                    promises[s], (CWLJobWrapper, CWLGather)
                                ) and not promises[s].hasFollowOn(wfjob):
                                    promises[s].addFollowOn(wfjob)
                                    connected = True
                                if not isinstance(
                                    promises[s], (CWLJobWrapper, CWLGather)
                                ) and not promises[s].hasChild(wfjob):
                                    promises[s].addChild(wfjob)
                                    connected = True
                        if not connected:
                            # the workflow step has default inputs only & isn't
                            # connected to other jobs, so add it as child of
                            # this workflow.
                            self.addChild(wfjob)

                        for out in step.tool["outputs"]:
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
    op: Any,
) -> None:
    """Iterate over a CWL Process object, running the op on each WorkflowStep."""
    if isinstance(cmdline_tool, cwltool.workflow.Workflow):
        for step in cmdline_tool.steps:
            op(step.tool)
            visitSteps(step.embedded_tool, op)


def remove_unprocessed_secondary_files(unfiltered_secondary_files: dict) -> list:
    """
    Remove unprocessed secondary files.

    Interpolated strings and optional inputs in secondary files were added to
    CWL in version 1.1.

    The CWL libraries we call do successfully resolve the interpolated strings,
    but add the resolved fields to the list of unresolved fields so we remove
    them here after the fact.

    We also remove any secondary files here not containing 'toilfs:', which
    means that it was not successfully imported into the toil jobstore.  The
    'required' logic seems to be handled deeper in cwltool.builder.Builder(),
    and correctly determines which files should be imported.  Therefore we
    remove the files here and if this file is SUPPOSED to exist, it will still
    give the appropriate file does not exist error, but just a bit further down
    the track.
    """
    intermediate_secondary_files = []
    final_secondary_files = []
    # remove secondary files still containing interpolated strings
    for sf in unfiltered_secondary_files["secondaryFiles"]:
        sf_bn = sf.get("basename", "")
        if ("$(" not in sf_bn) and ("${" not in sf_bn):
            intermediate_secondary_files.append(sf)
    # remove secondary files that are not present in the filestore
    # i.e. 'file://' only gets converted to 'toilfs:' upon a successful import
    for sf in intermediate_secondary_files:
        sf_loc = sf.get("location", "")
        if sf_loc.startswith("toilfs:"):
            final_secondary_files.append(sf)
    return final_secondary_files


def determine_load_listing(tool: ToilCommandLineTool):
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


def main(args: Union[List[str]] = None, stdout: TextIO = sys.stdout) -> int:
    """Run the main loop for toil-cwl-runner."""
    # Remove cwltool logger's stream handler so it uses Toil's
    cwllogger.removeHandler(defaultStreamHandler)
    config = Config()
    config.disableChaining = True
    config.cwl = True
    parser = argparse.ArgumentParser()
    addOptions(parser, config)
    parser.add_argument("cwltool", type=str)
    parser.add_argument("cwljob", nargs=argparse.REMAINDER)

    # Will override the "jobStore" positional argument, enables
    # user to select jobStore or get a default from logic one below.
    parser.add_argument("--jobStore", type=str)
    parser.add_argument("--not-strict", action="store_true")
    parser.add_argument(
        "--enable-dev",
        action="store_true",
        help="Enable loading and running development versions of CWL",
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
        help="Preserve all environment variable when running " "CommandLineTools.",
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
        default="tmp",
    )
    parser.add_argument(
        "--tmp-outdir-prefix",
        type=Text,
        help="Path prefix for intermediate output directories",
        default="tmp",
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
    if args is None:
        args = sys.argv[1:]

    provgroup = parser.add_argument_group(
        "Options for recording provenance " "information of the execution"
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
    # options twice, because we need to feed the parser the job store.

    # Propose a local workdir, probably under /tmp.
    # mkdtemp actually creates the directory, but
    # toil requires that the directory not exist,
    # since it is going to be our jobstore,
    # so make it and delete it and allow
    # toil to create it again (!)
    workdir = tempfile.mkdtemp()
    os.rmdir(workdir)

    # we use workdir as default default jobStore:
    options = parser.parse_args([workdir] + args)

    # if tmpdir_prefix is not the default value, set workDir if unset, and move
    # workdir and the job store under it
    if options.tmpdir_prefix != "tmp":
        workdir = cwltool.utils.create_tmp_dir(options.tmpdir_prefix)
        os.rmdir(workdir)
        # Re-parse arguments with the new default jobstore under the temp dir.
        # It still might be overridden by a --jobStore option
        options = parser.parse_args([workdir] + args)
        if options.workDir is None:
            # We need to override workDir because by default Toil will pick
            # somewhere under the system temp directory if unset, ignoring
            # --tmpdir-prefix.
            #
            # If set, workDir needs to exist, so we directly use the prefix
            options.workDir = cwltool.utils.create_tmp_dir(options.tmpdir_prefix)

    if options.provisioner and not options.jobStore:
        raise NoSuchJobStoreException(
            "Please specify a jobstore with the --jobStore option when "
            "specifying a provisioner."
        )

    if options.batchSystem == "kubernetes":
        options.singularity = True

    use_container = not options.no_container

    if options.logLevel:
        # Make sure cwltool uses Toil's log level.
        # Applies only on the leader.
        cwllogger.setLevel(options.logLevel)

    outdir = os.path.abspath(options.outdir)
    tmp_outdir_prefix = os.path.abspath(options.tmp_outdir_prefix)

    fileindex = {}  # type: ignore
    existing = {}  # type: ignore
    conf_file = getattr(options, "beta_dependency_resolvers_configuration", None)
    use_conda_dependencies = getattr(options, "beta_conda_dependencies", None)
    job_script_provider = None
    if conf_file or use_conda_dependencies:
        dependencies_configuration = DependenciesConfiguration(options)
        job_script_provider = dependencies_configuration

    options.default_container = None
    runtime_context = cwltool.context.RuntimeContext(vars(options))
    runtime_context.find_default_container = functools.partial(
        find_default_container, options
    )
    runtime_context.workdir = workdir  # type: ignore
    runtime_context.move_outputs = "leave"
    runtime_context.rm_tmpdir = False
    loading_context = cwltool.context.LoadingContext(vars(options))

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
            outobj = toil.restart()
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
            loading_context.resolver = cwltool.resolver.tool_resolver
            loading_context.strict = not options.not_strict
            options.workflow = options.cwltool
            options.job_order = options.cwljob
            uri, tool_file_uri = cwltool.load_tool.resolve_tool_uri(
                options.cwltool,
                loading_context.resolver,
                loading_context.fetcher_constructor,
            )
            options.tool_help = None
            options.debug = options.logLevel == "DEBUG"
            job_order_object, options.basedir, jobloader = cwltool.main.load_job_order(
                options,
                sys.stdin,
                loading_context.fetcher_constructor,
                loading_context.overrides_list,
                tool_file_uri,
            )

            loading_context, workflowobj, uri = cwltool.load_tool.fetch_document(
                uri, loading_context
            )
            loading_context, uri = cwltool.load_tool.resolve_and_validate_document(
                loading_context, workflowobj, uri
            )
            loading_context.overrides_list.extend(
                cast(
                    List[CWLObjectType],
                    loading_context.metadata.get("cwltool:overrides", []),
                )
            )

            document_loader = loading_context.loader
            metadata = loading_context.metadata
            processobj = document_loader.idx

            if options.provenance and runtime_context.research_obj:
                runtime_context.research_obj.packed_workflow(
                    cwltool.main.print_pack(loading_context, uri)
                )

            try:
                tool = cwltool.load_tool.make_tool(uri, loading_context)
            except cwltool.process.UnsupportedRequirement as err:
                logging.error(err)
                return 33
            runtime_context.secret_store = SecretStore()
            initialized_job_order = cwltool.main.init_job_order(
                job_order_object,
                options,
                tool,
                jobloader,
                sys.stdout,
                secret_store=runtime_context.secret_store,
            )

            fs_access = cwltool.stdfsaccess.StdFsAccess(options.basedir)
            fill_in_defaults(tool.tool["inputs"], initialized_job_order, fs_access)

            for inp in tool.tool["inputs"]:

                def set_secondary(fileobj):
                    if isinstance(fileobj, Mapping) and fileobj.get("class") == "File":
                        if "secondaryFiles" not in fileobj:
                            # inits all secondary files with 'file://' schema
                            # later changed to 'toilfs:' when imported into the jobstore
                            fileobj["secondaryFiles"] = [
                                {
                                    "location": cwltool.builder.substitute(
                                        fileobj["location"], sf["pattern"]
                                    ),
                                    "class": "File",
                                }
                                for sf in inp["secondaryFiles"]
                            ]

                    if isinstance(fileobj, MutableSequence):
                        for entry in fileobj:
                            set_secondary(entry)

                if shortname(inp["id"]) in initialized_job_order and inp.get(
                    "secondaryFiles"
                ):
                    set_secondary(initialized_job_order[shortname(inp["id"])])

            runtime_context.use_container = use_container
            runtime_context.tmp_outdir_prefix = os.path.realpath(tmp_outdir_prefix)
            runtime_context.job_script_provider = job_script_provider
            runtime_context.force_docker_pull = options.force_docker_pull
            runtime_context.no_match_user = options.no_match_user
            runtime_context.no_read_only = options.no_read_only
            runtime_context.basedir = options.basedir
            runtime_context.move_outputs = "move"

            # We instantiate an early builder object here to populate indirect
            # secondaryFile references using cwltool's library because we need
            # to resolve them before toil imports them into the filestore.
            # A second builder will be built in the job's run method when toil
            # actually starts the cwl job.
            builder = tool._init_job(initialized_job_order, runtime_context)

            # make sure this doesn't add listing items; if shallow_listing is
            # selected, it will discover dirs one deep and then again later on
            # (producing 2+ deep listings instead of only 1)
            builder.loadListing = "no_listing"

            builder.bind_input(
                tool.inputs_record_schema,
                initialized_job_order,
                discover_secondaryFiles=True,
            )

            def path_to_loc(obj):
                if "location" not in obj and "path" in obj:
                    obj["location"] = obj["path"]
                    del obj["path"]

            def import_files(inner_tool):
                visit_class(inner_tool, ("File", "Directory"), path_to_loc)
                visit_class(
                    inner_tool, ("File",), functools.partial(add_sizes, fs_access)
                )
                normalizeFilesDirs(inner_tool)

                adjustFileObjs(
                    inner_tool,
                    functools.partial(
                        uploadFile,
                        toil.importFile,
                        fileindex,
                        existing,
                        skip_broken=True,
                    ),
                )

            # files with the 'file://' uri are imported into the jobstore and
            # changed to 'toilfs:'
            import_files(initialized_job_order)

            visitSteps(tool, import_files)

            for job_name in initialized_job_order:
                if isinstance(initialized_job_order[job_name], list):
                    for job_params in cast(List, initialized_job_order[job_name]):
                        if isinstance(job_params, dict):
                            if "secondaryFiles" in job_params:
                                job_params[
                                    "secondaryFiles"
                                ] = remove_unprocessed_secondary_files(job_params)

            try:
                wf1, _ = makeJob(
                    tool=tool,
                    jobobj={},
                    runtime_context=runtime_context,
                    conditional=None,
                )
            except cwltool.process.UnsupportedRequirement as err:
                logging.error(err)
                return 33
            wf1.cwljob = initialized_job_order
            outobj = toil.start(wf1)

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
        stdout.write(json.dumps(outobj, indent=4))

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
