#!/usr/bin/env python3
# Copyright (C) 2018-2022 UCSC Computational Genomics Lab
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
import argparse
import asyncio
import collections
import copy
import errno
import glob
import io
import itertools
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid

from contextlib import ExitStack
from typing import cast, Any, Callable, Union, Dict, List, Optional, Set, Sequence, Tuple, Type, TypeVar, Iterator
from urllib.parse import urlsplit, urljoin, quote, unquote

import WDL
from WDL._util import byte_size_units
from WDL.runtime.task_container import TaskContainer
from WDL.runtime.backend.singularity import SingularityContainer
from WDL.runtime.backend.docker_swarm import SwarmContainer
import WDL.runtime.config

from toil.common import Config, Toil, addOptions
from toil.job import AcceleratorRequirement, Job, JobFunctionWrappingJob, Promise, Promised, accelerators_fully_satisfy, parse_accelerator, unwrap, unwrap_all
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.jobStores.abstractJobStore import AbstractJobStore, UnimplementedURLException
from toil.lib.conversions import convert_units, human2bytes
from toil.lib.misc import get_user_name
from toil.lib.threading import global_mutex

logger = logging.getLogger(__name__)

def potential_absolute_uris(uri: str, path: List[str], importer: Optional[WDL.Tree.Document] = None) -> Iterator[str]:
    """
    Get potential absolute URIs to check for an imported file.

    Given a URI or bare path, yield in turn all the URIs, with schemes, where we
    should actually try to find it, given that we want to search under/against
    the given paths or URIs, the current directory, and the given importing WDL
    document if any.
    """

    if uri == "":
        # Empty URIs can't come from anywhere.
        return

    # We need to brute-force find this URI relative to:
    #
    # 1. Itself if a full URI.
    #
    # 2. Importer's URL, if importer is a URL and this is a
    #    host-root-relative URL starting with / or scheme-relative
    #    starting with //, or just plain relative.
    #
    # 3. Current directory, if a relative path.
    #
    # 4. All the prefixes in "path".
    #
    # If it can't be found anywhere, we ought to (probably) throw
    # FileNotFoundError like the MiniWDL implementation does, with a
    # correct errno.
    #
    # To do this, we have AbstractFileStore.read_from_url, which can read a
    # URL into a binary-mode writable, or throw some kind of unspecified
    # exception if the source doesn't exist or can't be fetched.

    # This holds scheme-applied full URIs for all the places to search.
    full_path_list = []

    if importer is not None:
        # Add the place the imported file came form, to search first.
        full_path_list.append(Toil.normalize_uri(importer.pos.abspath))

    # Then the current directory. We need to make sure to include a filename component here or it will treat the current directory with no trailing / as a document and relative paths will look 1 level up.
    full_path_list.append(Toil.normalize_uri('.') + '/.')

    # Then the specified paths.
    # TODO:
    # https://github.com/chanzuckerberg/miniwdl/blob/e3e8ef74e80fbe59f137b0ad40b354957915c345/WDL/Tree.py#L1479-L1482
    # seems backward actually and might do these first!
    full_path_list += [Toil.normalize_uri(p) for p in path]

    # This holds all the URIs we tried and failed with.
    failures: Set[str] = set()

    for candidate_base in full_path_list:
        # Try fetching based off each base URI
        candidate_uri = urljoin(candidate_base, uri)

        if candidate_uri in failures:
            # Already tried this one, maybe we have an absolute uri input.
            continue
        logger.debug('Consider %s which is %s off of %s', candidate_uri, uri, candidate_base)

        # Try it
        yield candidate_uri
        # If we come back it didn't work
        failures.add(candidate_uri)

async def toil_read_source(uri: str, path: List[str], importer: Optional[WDL.Tree.Document]) -> WDL.ReadSourceResult:
    """
    Implementation of a MiniWDL read_source function that can use any
    filename or URL supported by Toil.

    Needs to be async because MiniWDL will await its result.
    """

    # We track our own failures for debugging
    tried = []

    for candidate_uri in potential_absolute_uris(uri, path, importer):
        # For each place to try in order
        destination_buffer = io.BytesIO()
        logger.debug('Fetching %s', candidate_uri)
        tried.append(candidate_uri)
        try:
            # TODO: this is probably sync work that would be better as async work here
            AbstractJobStore.read_from_url(candidate_uri, destination_buffer)
        except Exception as e:
            # TODO: we need to assume any error is just a not-found,
            # because the exceptions thrown by read_from_url()
            # implementations are not specified.
            logger.debug('Tried to fetch %s from %s but got %s', uri, candidate_uri, e)
            continue
        # If we get here, we got it probably.
        try:
            string_data = destination_buffer.getvalue().decode('utf-8')
        except UnicodeDecodeError:
            # But if it isn't actually unicode text, pretend it doesn't exist.
            logger.warning('Data at %s is not text; skipping!', candidate_uri)
            continue

        # Return our result and its URI. TODO: Should we de-URI files?
        return WDL.ReadSourceResult(string_data, candidate_uri)

    # If we get here we could not find it anywhere. Do exactly what MiniWDL
    # does:
    # https://github.com/chanzuckerberg/miniwdl/blob/e3e8ef74e80fbe59f137b0ad40b354957915c345/WDL/Tree.py#L1493
    # TODO: Make a more informative message?
    logger.error('Could not find %s at any of: %s', uri, tried)
    raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), uri)




# Bindings have a long type name
WDLBindings = WDL.Env.Bindings[WDL.Value.Base]

def combine_bindings(all_bindings: Sequence[WDLBindings]) -> WDLBindings:
    """
    Combine variable bindings from multiple predecessor tasks into one set for
    the current task.
    """

    # We can't just use WDL.Env.merge, because if a value is shadowed in a
    # binding, WDL.Env.merge can resurrect it to haunt us and become the
    # winning value in the merge result. See
    # <https://github.com/chanzuckerberg/miniwdl/issues/637>
    #
    # It also just strings the resolution chains of all the bindings together,
    # which is a bad plan if we aren't careful to avoid shadowing most of the
    # time. Whereas we actually routinely merge bindings of the whole current
    # environment together to propagate one or zero new values.
    #
    # So we do the merge manually.

    if len(all_bindings) == 0:
        # Combine nothing
        return WDL.Env.Bindings()
    else:
        # Sort, largest first
        all_bindings = sorted(all_bindings, key=lambda x: -len(x))

        merged = all_bindings[0]
        for bindings in all_bindings[1:]:
            for binding in bindings:
                if binding.name in merged:
                    # This is a duplicate
                    existing_value = merged[binding.name]
                    if existing_value != binding.value:
                        raise RuntimeError('Conflicting bindings for %s with values %s and %s', binding.name, existing_value, binding.value)
                    else:
                        logger.debug('Drop duplicate binding for %s', binding.name)
                else:
                    merged = merged.bind(binding.name, binding.value, binding.info)

    return merged

# TODO: Develop a Protocol that can match the logging function type more closely
def log_bindings(log_function: Callable[..., None], message: str, all_bindings: Sequence[Promised[WDLBindings]]) -> None:
    """
    Log bindings to the console, even if some are still promises.

    :param log_function: Function (like logger.info) to call to log data
    :param message: Message to log before the bindings
    :param all_bindings: A list of bindings or promises for bindings, to log
    """
    log_function(message)
    for bindings in all_bindings:
        if isinstance(bindings, WDL.Env.Bindings):
            for binding in bindings:
                log_function("%s = %s", binding.name, binding.value)
        elif isinstance(bindings, Promise):
            log_function("<Unfulfilled promise for bindings>")

def get_supertype(types: Sequence[Optional[WDL.Type.Base]]) -> WDL.Type.Base:
    """
    Get the supertype that can hold values of all the given types.
    """

    if None in types:
        # Need to allow optional values
        if len(types) == 1:
            # Only None is here
            return WDL.Type.Any(optional=True)
        if len(types) == 2:
            # None and something else
            for item in types:
                if item is not None:
                    # Return the type that's actually there, but make optional if not already.
                    return item.copy(optional=True)
            raise RuntimeError("Expected non-None in types could not be found")
        else:
            # Multiple types, and some nulls, so we need an optional Any.
            return WDL.Type.Any(optional=True)
    else:
        if len(types) == 1:
            # Only one type. It isn't None.
            the_type = types[0]
            assert the_type is not None
            return the_type
        else:
            # Multiple types (or none). Assume Any
            return WDL.Type.Any()


def for_each_node(root: WDL.Tree.WorkflowNode) -> Iterator[WDL.Tree.WorkflowNode]:
    """
    Iterate over all WDL workflow nodes in the given node, including inputs,
    internal nodes of conditionals and scatters, and gather nodes.
    """

    logger.debug('WorkflowNode: %s: %s %s', type(root), root, root.workflow_node_id)
    yield root
    for child_node in root.children:
        if isinstance(child_node, WDL.Tree.WorkflowNode):
            for result in for_each_node(child_node):
                yield result

def recursive_dependencies(root: WDL.Tree.WorkflowNode) -> Set[str]:
    """
    Get the combined workflow_node_dependencies of root and everything under
    it, which are not on anything in that subtree.

    Useful because section nodes can have internal nodes with dependencies not
    reflected in those of the section node itself.
    """

    # What are all dependencies?
    needed: Set[str] = set()
    # And what dependencies are provided internally?
    provided: Set[str] = set()

    for node in for_each_node(root):
        # Record everything each node needs
        needed |= node.workflow_node_dependencies
        # And the ID it makes
        provided.add(node.workflow_node_id)

    # And produce the diff
    return needed - provided

# We define a URI scheme kind of like but not actually compatible with the one
# we use for CWL. CWL brings along the file basename in its file type, but
# WDL.Value.File doesn't. So we need to make sure we stash that somewhere in
# the URI.
# TODO: We need to also make sure files from the same source directory end up
# in the same destination directory, when dealing with basename conflicts.

TOIL_URI_SCHEME = 'toilfile:'

def pack_toil_uri(file_id: FileID, file_basename: str) -> str:
    """
    Encode a Toil file ID and its source path in a URI that starts with the scheme in TOIL_URI_SCHEME.
    """

    # We urlencode everything, including any slashes. We need to use a slash to
    # set off the actual filename, so the WDL standard library basename
    # function works correctly.
    return f"{TOIL_URI_SCHEME}{quote(file_id.pack(), safe='')}/{quote(file_basename, safe='')}"

def unpack_toil_uri(toil_uri: str) -> Tuple[FileID, str]:
    """
    Unpack a URI made by make_toil_uri to retrieve the FileID and the basename
    (no path prefix) that the file is supposed to have.
    """

    # Split out scheme and rest of URL
    parts = toil_uri.split(':')
    if len(parts) != 2:
        raise ValueError(f"Wrong number of colons in URI: {toil_uri}")
    if parts[0] + ':' != TOIL_URI_SCHEME:
        raise ValueError(f"URI doesn't start with {TOIL_URI_SCHEME} and should: {toil_uri}")
    # Split encoded file ID from filename
    parts = parts[1].split('/')
    if len(parts) != 2:
        raise ValueError(f"Wrong number of path segments in URI: {toil_uri}")
    file_id = FileID.unpack(unquote(parts[0]))
    file_basename = unquote(parts[1])

    return file_id, file_basename

class NonDownloadingSize(WDL.StdLib._Size):
    """
    WDL size() implementation that avoids downloading files.

    MiniWDL's default size() implementation downloads the whole file to get its
    size. We want to be able to get file sizes from code running on the leader,
    where there may not be space to download the whole file. So we override the
    fancy class that implements it so that we can handle sizes for FileIDs
    using the FileID's stored size info.
    """

    def _call_eager(self, expr: "WDL.Expr.Apply", arguments: List[WDL.Value.Base]) -> WDL.Value.Base:
        """
        Replacement evaluation implementation that avoids downloads.
        """

        # Get all the URIs of files that actually are set.
        file_uris: List[str] = [f.value for f in arguments[0].coerce(WDL.Type.Array(WDL.Type.File(optional=True))).value if not isinstance(f, WDL.Value.Null)]

        total_size = 0.0
        for uri in file_uris:
            # Sum up the sizes of all the files, if any.
            if uri.startswith(TOIL_URI_SCHEME):
                # This is a Toil File ID we encoded; we have the size
                # available.
                file_id, _ = unpack_toil_uri(uri)
                # Use the encoded size
                total_size += file_id.size
            else:
                # We need to fetch it and get its size.
                total_size += os.path.getsize(self.stdlib._devirtualize_filename(uri))

        if len(arguments) > 1:
            # Need to convert units. See
            # <https://github.com/chanzuckerberg/miniwdl/blob/498dc98d08e3ea3055b34b5bec408ae51dae0f0f/WDL/StdLib.py#L735-L740>
            unit_name: str = arguments[1].coerce(WDL.Type.String()).value
            if unit_name not in byte_size_units:
                raise WDL.Error.EvalError(expr, "size(): invalid unit " + unit_name)
            # Divide down to the right unit
            total_size /= float(byte_size_units[unit_name])

        # Return the result as a WDL float value
        return WDL.Value.Float(total_size)


# Both the WDL code itself **and** the commands that it runs will deal in
# "virtualized" filenames.

# We have to guarantee that "When a WDL author uses a File input in their
# Command Section, the fully qualified, localized path to the file is
# substituted when that declaration is referenced in the command template."

# This has to be true even if the File is the result of a WDL function that is
# run *during* the evaluation of the command string, via a placeholder
# expression evaluation.

# Really there are 3 filename spaces in play: Toil filestore URLs,
# outside-the-container host filenames, and inside-the-container filenames. But
# the MiniWDL machinery only gives us 2 levels to work with: "virtualized"
# (visible to the workflow) and "devirtualized" (openable by this process).

# So we sneakily swap out what "virtualized" means. Usually (as provided by
# ToilWDLStdLibBase) a "virtualized" filename is the Toil filestore URL space.
# But when evaluating a task command, we switch things so that the
# "virtualized" space is the inside-the-container filename space (by
# devirtualizing and then host-to-container-mapping all the visible files, and
# then using ToilWDLStdLibTaskCommand for evaluating expressions, and then
# going back from container to host space after the command). At all times the
# "devirtualized" space is outside-the-container host filenames.

class ToilWDLStdLibBase(WDL.StdLib.Base):
    """
    Standard library implementation for WDL as run on Toil.
    """

    def __init__(self, file_store: AbstractFileStore):
        """
        Set up the standard library.
        """
        # TODO: Just always be the 1.2 standard library.
        wdl_version = "1.2"
        # Where should we be writing files that write_file() makes?
        write_dir = file_store.getLocalTempDir()
        # Set up miniwdl's implementation (which may be WDL.StdLib.TaskOutputs)
        super().__init__(wdl_version, write_dir)

        # Replace the MiniWDL size() implementation with one that doesn't need
        # to always download the file.
        self.size = NonDownloadingSize(self)

        # Keep the file store around so we can access files.
        self._file_store = file_store 

    def _is_url(self, filename: str, schemes: List[str] = ['http:', 'https:', 's3:', 'gs:', TOIL_URI_SCHEME]) -> bool:
        """
        Decide if a filename is a known kind of URL
        """
        for scheme in schemes:
            if filename.startswith(scheme):
                return True
        return False

    def _devirtualize_filename(self, filename: str) -> str:
        """
        'devirtualize' filename passed to a read_* function: return a filename that can be open()ed
        on the local host.
        """

        # TODO: Support people doing path operations (join, split, get parent directory) on the virtualized filenames.
        # TODO: For task inputs, we are supposed to make sure to put things in the same directory if they came from the same directory. See <https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#task-input-localization>
        if filename.startswith(TOIL_URI_SCHEME):
            # This is a reference to the Toil filestore.
            # Deserialize the FileID
            file_id, file_basename = unpack_toil_uri(filename)

            # Decide where it should be put
            file_dir = self._file_store.getLocalTempDir()
            dest_path = os.path.join(file_dir, file_basename)

            # And get a local path to the file
            result = self._file_store.readGlobalFile(file_id, dest_path)
        elif self._is_url(filename):
            # This is some other URL that we think Toil knows how to read.
            # Import into the job store from here and then download to the node.
            # TODO: Can we predict all the URLs that can be used up front and do them all on the leader, where imports are meant to happen?
            imported = self._file_store.import_file(filename)
            if imported is None:
                raise FileNotFoundError(f"Could not import URL {filename}")
            # And get a local path to the file
            result = self._file_store.readGlobalFile(imported)
        else:
            # This is a local file
            result = filename

        logger.debug('Devirtualized %s as openable file %s', filename, result)
        assert os.path.exists(result), f"Virtualized file {filename} looks like a local file but isn't!"
        return result

    def _virtualize_filename(self, filename: str) -> str:
        """
        from a local path in write_dir, 'virtualize' into the filename as it should present in a
        File value
        """


        if self._is_url(filename):
            # Already virtual
            logger.debug('Virtualized %s as WDL file %s', filename, filename)
            return filename

        # Otherwise this is a local file and we want to fake it as a Toil file store file
        file_id = self._file_store.writeGlobalFile(filename)
        result = pack_toil_uri(file_id, os.path.basename(filename))
        logger.debug('Virtualized %s as WDL file %s', filename, result)
        return result

class ToilWDLStdLibTaskCommand(ToilWDLStdLibBase):
    """
    Standard library implementation to use inside a WDL task command evaluation.

    Expects all the filenames in variable bindings to be container-side paths;
    these are the "virtualized" filenames, while the "devirtualized" filenames
    are host-side paths.
    """

    def __init__(self, file_store: AbstractFileStore, container: TaskContainer):
        """
        Set up the standard library for the task command section.
        """

        # TODO: Don't we want to make sure we don't actually use the file store?
        super().__init__(file_store)
        self.container = container

    def _devirtualize_filename(self, filename: str) -> str:
        """
        Go from a virtualized WDL-side filename to a local disk filename.

        Any WDL-side filenames which are paths will be paths in the container. 
        """
        if self._is_url(filename):
            # We shouldn't have to deal with URLs here; we want to have exactly
            # two nicely stacked/back-to-back layers of virtualization, joined
            # on the out-of-container paths.
            raise RuntimeError(f"File {filename} is a URL but should already be an in-container-virtualized filename")
        
        # If this is a local path it will be in the container. Make sure we
        # use the out-of-container equivalent.
        result = self.container.host_path(filename)

        if result is None:
            # We really shouldn't have files in here that we didn't virtualize.
            raise RuntimeError(f"File {filename} in container is not mounted from the host and can't be opened from the host")

        logger.debug('Devirtualized %s as out-of-container file %s', filename, result)
        return result


    def _virtualize_filename(self, filename: str) -> str:
        """
        From a local path in write_dir, 'virtualize' into the filename as it should present in a
        File value, when substituted into a command in the container.
        """

        if filename not in self.container.input_path_map:
            # Mount the file.
            self.container.add_paths([filename])

        result = self.container.input_path_map[filename]
        
        logger.debug('Virtualized %s as WDL file %s', filename, result)
        return result

class ToilWDLStdLibTaskOutputs(ToilWDLStdLibBase, WDL.StdLib.TaskOutputs):
    """
    Standard library implementation for WDL as run on Toil, with additional
    functions only allowed in task output sections.
    """

    def __init__(self, file_store: AbstractFileStore, stdout_path: str, stderr_path: str, current_directory_override: Optional[str] = None):
        """
        Set up the standard library for a task output section. Needs to know
        where standard output and error from the task have been stored.

        If current_directory_override is set, resolves relative paths and globs
        from there instead of from the real current directory.
        """

        # Just set up as ToilWDLStdLibBase, but it will call into
        # WDL.StdLib.TaskOutputs next.
        super().__init__(file_store)

        # Remember task putput files
        self._stdout_path = stdout_path
        self._stderr_path = stderr_path

        # Remember current directory
        self._current_directory_override = current_directory_override

        # We need to attach implementations for WDL's stdout(), stderr(), and glob().
        # TODO: Can we use the fancy decorators instead of this wizardry?
        setattr(
            self,
            "stdout",
            WDL.StdLib.StaticFunction("stdout", [], WDL.Type.File(), self._stdout),
        )
        setattr(
            self,
            "stderr",
            WDL.StdLib.StaticFunction("stderr", [], WDL.Type.File(), self._stderr),
        )
        setattr(
            self,
            "glob",
            WDL.StdLib.StaticFunction("glob", [WDL.Type.String()], WDL.Type.Array(WDL.Type.File()), self._glob),
        )

    def _stdout(self) -> WDL.Value.File:
        """
        Get the standard output of the command that ran, as a WDL File, outside the container.
        """
        return WDL.Value.File(self._stdout_path)

    def _stderr(self) -> WDL.Value.File:
        """
        Get the standard error of the command that ran, as a WDL File, outside the container.
        """
        return WDL.Value.File(self._stderr_path)

    def _glob(self, pattern: WDL.Value.String) -> WDL.Value.Array:
        """
        Get a WDL Array of WDL Files left behind by the job that ran, matching the given glob pattern, outside the container.
        """

        # Unwrap the pattern
        pattern_string = pattern.coerce(WDL.Type.String()).value

        # The spec says we really are supposed to invoke `bash` and pass it
        # `echo <the pattern>`, and that `bash` is allowed to be
        # "non-standard", so if you use a Docker image you could ship any code
        # you want as "bash" and we have to run it and then filter out the
        # directories.

        # Problem: `echo <the pattern>` just dumps space-delimited filenames which may themselves contain spaces, so we can't actually correctly recover them, if we need to allow for `echo <the pattern>` being able to do arbitrary things in the container's Bash other than interpreting the pattern
        # So we send a little Bash script that can delimit the files with something, and assume the Bash really is a Bash.

        # This needs to run in the work directory that the container used, if any.
        work_dir = '.' if not self._current_directory_override else self._current_directory_override

        # TODO: get this to run in the right container if there is one
        # Bash (now?) has a compgen builtin for shell completion that can evaluate a glob where the glob is in a quotes string that might have spaces in it. See <https://unix.stackexchange.com/a/616608>.
        # This will handle everything except newlines in the filenames.
        # TODO: Newlines in the filenames?
        # Since compgen will return 1 if nothing matches, we need to allow a failing exit code here.
        lines = subprocess.run(['bash', '-c', 'cd ' + shlex.quote(work_dir) + ' && compgen -G ' + shlex.quote(pattern_string)], stdout=subprocess.PIPE).stdout.decode('utf-8')

        # Get each name that is a file
        results = []
        for line in lines.split('\n'):
            if not line:
                continue
            if not line.startswith('/'):
                # Make sure to be working with absolute paths since the glob
                # might not share our current directory
                line = os.path.join(work_dir, line)
            if not os.path.isfile(line):
                continue
            results.append(line)

        # Just turn them all into WDL File objects with local disk out-of-container names.
        return WDL.Value.Array(WDL.Type.File(), [WDL.Value.File(x) for x in results])

    def _devirtualize_filename(self, filename: str) -> str:
        """
        Go from a virtualized WDL-side filename to a local disk filename.

        Any WDL-side filenames which are relative will be relative to the
        current directory override, if set.
        """
        if not self._is_url(filename) and not filename.startswith('/'):
            # We are getting a bare relative path from the WDL side.
            # Find a real path to it relative to the current directory override.
            work_dir = '.' if not self._current_directory_override else self._current_directory_override
            filename = os.path.join(work_dir, filename)

        return super()._devirtualize_filename(filename)

    def _virtualize_filename(self, filename: str) -> str:
        """
        Go from a local disk filename to a virtualized WDL-side filename.

        Any relative paths will be relative to the current directory override,
        if set, to account for how they might not be *real* devirtualized
        filenames.
        """

        if not self._is_url(filename) and not filename.startswith('/'):
            # We are getting a bare relative path the supposedly devirtualized side.
            # Find a real path to it relative to the current directory override.
            work_dir = '.' if not self._current_directory_override else self._current_directory_override
            filename = os.path.join(work_dir, filename)

        return super()._virtualize_filename(filename)

def evaluate_named_expression(context: Union[WDL.Error.SourceNode, WDL.Error.SourcePosition], name: str, expected_type: Optional[WDL.Type.Base], expression: Optional[WDL.Expr.Base], environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDL.Value.Base:
    """
    Evaluate an expression when we know the name of it.
    """

    if expression is None:
        if expected_type and expected_type.optional:
            # We can just leave the value as null
            value: WDL.Value.Base = WDL.Value.Null()
        else:
            raise WDL.Error.EvalError(context, "Cannot evaluate no expression for " + name)
    else:
        logger.debug("Evaluate expression for %s: %s", name, expression)
        try:
            if expected_type:
                # Make sure the types are allowed
                expression.typecheck(expected_type)

            # Do the actual evaluation
            value = expression.eval(environment, stdlib)
        except Exception:
            # If something goes wrong, dump.
            logger.exception("Expression evaluation failed for %s: %s", name, expression)
            log_bindings(logger.exception, "Expression was evaluated in:", [environment])
            raise

    if expected_type:
        # Coerce to the type it should be.
        value = value.coerce(expected_type)

    return value

def evaluate_decl(node: WDL.Tree.Decl, environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDL.Value.Base:
    """
    Evaluate the expression of a declaration node, or raise an error.
    """

    return evaluate_named_expression(node, node.name, node.type, node.expr, environment, stdlib)

def evaluate_call_inputs(context: Union[WDL.Error.SourceNode, WDL.Error.SourcePosition], expressions: Dict[str, WDL.Expr.Base], environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Evaluate a bunch of expressions with names, and make them into a fresh set of bindings.
    """

    new_bindings: WDLBindings = WDL.Env.Bindings()
    for k, v in expressions.items():
        # Add each binding in turn
        new_bindings = new_bindings.bind(k, evaluate_named_expression(context, k, None, v, environment, stdlib))
    return new_bindings

def evaluate_defaultable_decl(node: WDL.Tree.Decl, environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDL.Value.Base:
    """
    If the name of the declaration is already defined in the environment, return its value. Otherwise, return the evaluated expression.
    """

    try:
        if node.name in environment and not isinstance(environment[node.name], WDL.Value.Null):
            logger.debug('Name %s is already defined with a non-null value, not using default', node.name)
            return environment[node.name]
        else:
            if node.type is not None and not node.type.optional and node.expr is None:
                # We need a value for this but there isn't one.
                raise WDL.Error.EvalError(node, f"Value for {node.name} was not provided and no default value is available")
            logger.info('Defaulting %s to %s', node.name, node.expr)
            return evaluate_decl(node, environment, stdlib)
    except Exception:
        # If something goes wrong, dump.
        logger.exception("Evaluation failed for %s", node)
        log_bindings(logger.exception, "Statement was evaluated in:", [environment])
        raise

# TODO: make these stdlib methods???
def devirtualize_files(environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that are actually available to command line commands.
    """

    return map_over_files_in_bindings(environment, stdlib._devirtualize_filename)

def virtualize_files(environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that are usable from other machines.
    """

    return map_over_files_in_bindings(environment, stdlib._virtualize_filename)

def import_files(environment: WDLBindings, toil: Toil, path: Optional[List[str]] = None) -> WDLBindings:
    """
    Make sure all File values embedded in the given bindings are imported,
    using the given Toil object.

    :param path: If set, try resolving input location relative to the URLs or
                 directories in this list.
    """

    def import_file_from_uri(uri: str) -> str:
        """
        Import a file from a URI and return a virtualized filename for it.
        """

        tried = []
        for candidate_uri in potential_absolute_uris(uri, path if path is not None else []):
            # Try each place it could be according to WDL finding logic.
            tried.append(candidate_uri)
            try:
                # Try to import the file. Don't raise if we can't find it, just
                # return None!
                imported = toil.import_file(candidate_uri, check_existence=False)
            except UnimplementedURLException as e:
                # We can't find anything that can even support this URL scheme.
                # Report to the user, they are probably missing an extra.
                logger.critical('Error: ' + str(e))
                sys.exit(1)
            except Exception:
                # Something went wrong besides the file not being found. Maybe
                # we have no auth.
                logger.error("Something went wrong importing %s", candidate_uri)
                raise
            if imported is None:
                # Wasn't found there
                continue
            logger.info('Imported %s', candidate_uri)

            # Work out what the basename for the file was
            file_basename = os.path.basename(urlsplit(candidate_uri).path)

            if file_basename == "":
                # We can't have files with no basename because we need to
                # download them at that basename later.
                raise RuntimeError(f"File {candidate_uri} has no basename and so cannot be a WDL File")

            # Was actually found
            return pack_toil_uri(imported, file_basename)

        # If we get here we tried all the candidates
        raise RuntimeError(f"Could not find {uri} at any of: {tried}")

    return map_over_files_in_bindings(environment, import_file_from_uri)

def drop_missing_files(environment: WDLBindings, current_directory_override: Optional[str] = None) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that exist, or are null.

    Files must not be virtualized.
    """

    # Determine where to evaluate relative paths relative to
    work_dir = '.' if not current_directory_override else current_directory_override

    def drop_if_missing(value_type: WDL.Type.Base, filename: str) -> Optional[str]:
        """
        Return None if a file doesn't exist, or its path if it does.
        """
        effective_path = os.path.abspath(os.path.join(work_dir, filename))
        if os.path.exists(effective_path):
            return filename
        else:
            logger.debug('File %s with type %s does not actually exist at %s', filename, value_type, effective_path)
            return None

    return map_over_typed_files_in_bindings(environment, drop_if_missing)

def get_file_paths_in_bindings(environment: WDLBindings) -> List[str]:
    """
    Get the paths of all files in the bindings. Doesn't guarantee that
    duplicates are removed.

    TODO: Duplicative with WDL.runtime.task._fspaths, except that is internal
    and supports Direcotry objects.
    """

    paths = []
    map_over_files_in_bindings(environment, lambda x: paths.append(x))
    return paths

def map_over_typed_files_in_bindings(environment: WDLBindings, transform: Callable[[WDL.Type.Base, str], Optional[str]]) -> WDLBindings:
    """
    Run all File values embedded in the given bindings through the given
    transformation function.

    TODO: Replace with WDL.Value.rewrite_env_paths or WDL.Value.rewrite_files
    """

    return environment.map(lambda b: map_over_typed_files_in_binding(b, transform))

def map_over_files_in_bindings(bindings: WDLBindings, transform: Callable[[str], Optional[str]]) -> WDLBindings:
    """
    Run all File values' types and values embedded in the given bindings
    through the given transformation function.

    TODO: Replace with WDL.Value.rewrite_env_paths or WDL.Value.rewrite_files
    """

    return map_over_typed_files_in_bindings(bindings, lambda _, x: transform(x))


def map_over_typed_files_in_binding(binding: WDL.Env.Binding[WDL.Value.Base], transform: Callable[[WDL.Type.Base, str], Optional[str]]) -> WDL.Env.Binding[WDL.Value.Base]:
    """
    Run all File values' types and values embedded in the given binding's value through the given
    transformation function.
    """

    return WDL.Env.Binding(binding.name, map_over_typed_files_in_value(binding.value, transform), binding.info)

# TODO: We want to type this to say, for anything descended from a WDL type, we
# return something descended from the same WDL type or a null. But I can't
# quite do that with generics, since you could pass in some extended WDL value
# type we've never heard of and expect to get one of those out.
#
# For now we assume that any types extending the WDL value types will implement
# compatible constructors.
def map_over_typed_files_in_value(value: WDL.Value.Base, transform: Callable[[WDL.Type.Base, str], Optional[str]]) -> WDL.Value.Base:
    """
    Run all File values embedded in the given value through the given
    transformation function.

    If the transform returns None, the file value is changed to Null.

    The transform has access to the type information for the value, so it knows
    if it may return None, depending on if the value is optional or not.

    The transform is *allowed* to return None only if the mapping result won't
    actually be used, to allow for scans. So error checking needs to be part of
    the transform itself.
    """

    if isinstance(value, WDL.Value.File):
        # This is a file so we need to process it
        new_path = transform(value.type, value.value)
        if new_path is None:
            # Assume the transform checked types if we actually care about the
            # result.
            return WDL.Value.Null()
        else:
            # Make whatever the value is around the new path.
            # TODO: why does this need casting?
            return WDL.Value.File(new_path, value.expr)
    elif isinstance(value, WDL.Value.Array):
        # This is an array, so recurse on the items
        return WDL.Value.Array(value.type.item_type, [map_over_typed_files_in_value(v, transform) for v in value.value], value.expr)
    elif isinstance(value, WDL.Value.Map):
        # This is a map, so recurse on the members of the items, which are tuples (but not wrapped as WDL Pair objects)
        # TODO: Can we avoid a cast in a comprehension if we get MyPy to know that each pair is always a 2-element tuple?
        return WDL.Value.Map(value.type.item_type, [cast(Tuple[WDL.Value.Base, WDL.Value.Base], tuple((map_over_typed_files_in_value(v, transform) for v in pair))) for pair in value.value], value.expr)
    elif isinstance(value, WDL.Value.Pair):
        # This is a pair, so recurse on the left and right items
        return WDL.Value.Pair(value.type.left_type, value.type.right_type, cast(Tuple[WDL.Value.Base, WDL.Value.Base], tuple((map_over_typed_files_in_value(v, transform) for v in value.value))), value.expr)
    elif isinstance(value, WDL.Value.Struct):
        # This is a struct, so recurse on the values in the backing dict
        return WDL.Value.Struct(cast(Union[WDL.Type.StructInstance, WDL.Type.Object], value.type), {k: map_over_typed_files_in_value(v, transform) for k, v in value.value.items()}, value.expr)
    else:
        # All other kinds of value can be passed through unmodified.
        return value

class WDLBaseJob(Job):
    """
    Base job class for all WDL-related jobs.
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Make a WDL-related job.

        Makes sure the global recursive call limit is high enough to allow
        MiniWDL's extremely deep WDL structures to be pickled. We handle this
        in the constructor because it needs to happen in the leader and the
        worker before a job body containing MiniWDL structures can be saved.
        """

        # Default everything to being a local job
        if 'local' not in kwargs:
            kwargs['local'] = True

        super().__init__(**kwargs)

        # The jobs can't pickle under the default Python recursion limit of
        # 1000 because MiniWDL data structures are very deep.
        # TODO: Dynamically determine how high this needs to be to serialize the structures we actually have.
        # TODO: Make sure C-level stack size is also big enough for this.
        sys.setrecursionlimit(10000)

    # TODO: We're not allowed by MyPy to override a method and widen the return
    # type, so this has to be Any.
    def run(self, file_store: AbstractFileStore) -> Any:
        """
        Run a WDL-related job.
        """
        # Make sure that pickle is prepared to save our return values, which
        # might take a lot of recursive calls. TODO: This might be because
        # bindings are actually linked lists or something?
        sys.setrecursionlimit(10000)

class WDLTaskJob(WDLBaseJob):
    """
    Job that runs a WDL task.

    Responsible for evaluating the input declarations for unspecified inputs,
    evaluating the runtime section, re-scheduling if resources are not
    available, running any command, and evaluating the outputs.

    All bindings are in terms of task-internal names.
    """

    def __init__(self, task: WDL.Tree.Task, prev_node_results: Sequence[Promised[WDLBindings]], task_id: List[str], namespace: str, **kwargs: Any) -> None:
        """
        Make a new job to run a task.

        :param namespace: The namespace that the task's *contents* exist in.
               The caller has alredy added the task's own name.
        """

        # This job should not be local because it represents a real workflow task.
        # TODO: Instead of re-scheduling with more resources, add a local
        # "wrapper" job like CWL uses to determine the actual requirements.
        super().__init__(unitName=namespace, displayName=namespace, local=False, **kwargs)

        logger.info("Preparing to run task %s as %s", task.name, namespace)

        self._task = task
        self._prev_node_results = prev_node_results
        self._task_id = task_id
        self._namespace = namespace

    def can_fake_root(self) -> bool:
        """
        Determie if --fakeroot is likely to work for Singularity.
        """

        # We need to have an entry for our user in /etc/subuid to grant us a range of UIDs to use, for fakeroot to work.
        try:
            subuid_file = open('/etc/subuid')
        except OSError as e:
            logger.warning('Cannot open /etc/subuid due to %s; assuming no subuids available', e)
            return False
        username = get_user_name()
        for line in subuid_file:
            if line.split(':')[0].strip() == username:
                # We have a line assigning subuids
                return True
        # If there is no line, we have no subuids
        logger.warning('No subuids are assigned to %s; cannot fake root.', username)
        return False

    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually run the task.
        """
        super().run(file_store)
        logger.info("Running task %s (%s) called as %s", self._task.name, self._task_id, self._namespace)

        # Combine the bindings we get from previous jobs.
        # For a task we are only passed the inside-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)

        if self._task.inputs:
            logger.debug("Evaluating task inputs")
            for input_decl in self._task.inputs:
                # Evaluate all the inputs that aren't pre-set
                bindings = bindings.bind(input_decl.name, evaluate_defaultable_decl(input_decl, bindings, standard_library))
        for postinput_decl in self._task.postinputs:
            # Evaluate all the postinput decls
            bindings = bindings.bind(postinput_decl.name, evaluate_defaultable_decl(postinput_decl, bindings, standard_library))

        # Evaluate the runtime section
        runtime_bindings = evaluate_call_inputs(self._task, self._task.runtime, bindings, standard_library)

        # Fill these in with not-None if we need to bump up our resources from what we have available.
        # TODO: Can this break out into a function somehow?
        runtime_memory: Optional[int] = None
        runtime_cores: Optional[float] = None
        runtime_disk: Optional[int] = None
        runtime_accelerators: Optional[List[AcceleratorRequirement]] = None

        if runtime_bindings.has_binding('cpu'):
            cpu_spec: int = runtime_bindings.resolve('cpu').value
            if cpu_spec > self.cores:
                # We need to get more cores
                runtime_cores = float(cpu_spec)
                logger.info('Need to reschedule to get %s cores; have %s', runtime_cores, self.cores)

        if runtime_bindings.has_binding('memory'):
            # Get the memory requirement and convert to bytes
            memory_spec: Union[int, str] = runtime_bindings.resolve('memory').value
            if isinstance(memory_spec, str):
                memory_spec = human2bytes(memory_spec)

            if memory_spec > self.memory:
                # We need to go get more memory
                runtime_memory = memory_spec
                logger.info('Need to reschedule to get %s memory; have %s', runtime_memory, self.memory)

        if runtime_bindings.has_binding('disks'):
            # Miniwdl doesn't have this, but we need to be able to parse things like:
            # local-disk 5 SSD
            # which would mean we need 5 GB space. Cromwell docs for this are at https://cromwell.readthedocs.io/en/stable/RuntimeAttributes/#disks
            # We ignore all disk types, and complain if the mount point is not `local-disk`.
            disks_spec: str = runtime_bindings.resolve('disks').value
            all_specs = disks_spec.split(',')
            # Sum up the gigabytes in each disk specification
            total_gb = 0
            for spec in all_specs:
                # Split up each spec as space-separated. We assume no fields
                # are empty, and we want to allow people to use spaces after
                # their commas when separating the list, like in Cromwell's
                # examples, so we strip whitespace.
                spec_parts = spec.strip().split(' ')
                if len(spec_parts) != 3:
                    # TODO: Add a WDL line to this error
                    raise ValueError(f"Could not parse disks = {disks_spec} because {spec} does not have 3 space-separated parts")
                if spec_parts[0] != 'local-disk':
                    # TODO: Add a WDL line to this error
                    raise NotImplementedError(f"Could not provide disks = {disks_spec} because only the local-disks mount point is implemented")
                try:
                    total_gb += int(spec_parts[1])
                except:
                    # TODO: Add a WDL line to this error
                    raise ValueError(f"Could not parse disks = {disks_spec} because {spec_parts[1]} is not an integer")
                # TODO: we always ignore the disk type and assume we have the right one.
                # TODO: Cromwell rounds LOCAL disks up to the nearest 375 GB. I
                # can't imagine that ever being standardized; just leave it
                # alone so that the workflow doesn't rely on this weird and
                # likely-to-change Cromwell detail.
                if spec_parts[2] == 'LOCAL':
                    logger.warning('Not rounding LOCAL disk to the nearest 375 GB; workflow execution will differ from Cromwell!')
            total_bytes: float = convert_units(total_gb, 'GB')
            if total_bytes > self.disk:
                runtime_disk = int(total_bytes)
                logger.info('Need to reschedule to get %s disk, have %s', runtime_disk, self.disk)

        if runtime_bindings.has_binding('gpuType') or runtime_bindings.has_binding('gpuCount') or runtime_bindings.has_binding('nvidiaDriverVersion'):
            # We want to have GPUs
            # TODO: actually coerce types here instead of casting to detect user mistakes
            # Get the GPU count if set, or 1 if not,
            gpu_count: int = cast(WDL.Value.Int, runtime_bindings.get('gpuCount', WDL.Value.Int(1))).value
            # Get the GPU model constraint if set, or None if not
            gpu_model: Optional[str] = cast(Union[WDL.Value.String, WDL.Value.Null], runtime_bindings.get('gpuType', WDL.Value.Null())).value
            # We can't enforce a driver version, but if an nvidia driver
            # version is set, manually set nvidia brand
            gpu_brand: Optional[str] = 'nvidia' if runtime_bindings.has_binding('nvidiaDriverVersion') else None
            # Make a dict from this
            accelerator_spec: Dict[str, Union[str, int]] = {'kind': 'gpu', 'count': gpu_count}
            if gpu_model is not None:
                accelerator_spec['model'] = gpu_model
            if gpu_brand is not None:
                accelerator_spec['brand'] = gpu_brand

            accelerator_requirement = parse_accelerator(accelerator_spec)
            if not accelerators_fully_satisfy(self.accelerators, accelerator_requirement, ignore=['model']):
                # We don't meet the accelerator requirement.
                # We are loose on the model here since, really, we *should*
                # have either no accelerators or the accelerators we asked for.
                # If the batch system is ignoring the model, we don't want to
                # loop forever trying for the right model.
                # TODO: Change models overall to a hint???
                runtime_accelerators = [accelerator_requirement]
                logger.info('Need to reschedule to get %s accelerators, have %s', runtime_accelerators, self.accelerators)

        if runtime_cores or runtime_memory or runtime_disk or runtime_accelerators:
            # We need to reschedule.
            logger.info('Rescheduling %s with more resources', self)
            # Make the new copy of this job with more resources.
            # TODO: We don't pass along the input or runtime bindings, so they
            # need to get re-evaluated. If we did pass them, we'd have to make
            # sure to upload local files made by WDL code in the inputs/runtime
            # sections and pass along that environment. Right now we just
            # re-evaluate that whole section once we have the requested
            # resources.
            # TODO: What if the runtime section says we need a lot of disk to
            # hold the large files that the inputs section is going to write???
            rescheduled = WDLTaskJob(self._task, self._prev_node_results, self._task_id, self._namespace, cores=runtime_cores or self.cores, memory=runtime_memory or self.memory, disk=runtime_disk or self.disk, accelerators=runtime_accelerators or self.accelerators)
            # Run that as a child
            self.addChild(rescheduled)
            # And return its result.
            return rescheduled.rv()

        # If we get here we have all the resources we need, so run the task

        if shutil.which('singularity'):

            # Prepare to use Singularity. We will need plenty of space to
            # download images.
            if 'SINGULARITY_CACHEDIR' not in os.environ:
                # Cache Singularity's layers somehwere known to have space, not in home
                os.environ['SINGULARITY_CACHEDIR'] = os.path.join(file_store.workflow_dir, 'singularity_cache')
            # Make sure it exists.
            os.makedirs(os.environ['SINGULARITY_CACHEDIR'], exist_ok=True)

            if 'MINIWDL__SINGULARITY__IMAGE_CACHE' not in os.environ:
                # Cache Singularity images for the workflow on this machine.
                # Since MiniWDL does only within-process synchronization for pulls,
                # we also will need to pre-pull one image into here at a time.
                os.environ['MINIWDL__SINGULARITY__IMAGE_CACHE'] = os.path.join(file_store.workflow_dir, 'miniwdl_sif_cache')
            # Make sure it exists.
            os.makedirs(os.environ['MINIWDL__SINGULARITY__IMAGE_CACHE'], exist_ok=True)

            # Run containers with Singularity
            TaskContainerImplementation: Type[TaskContainer]  = SingularityContainer
        else:
            # Run containers with Docker
            TaskContainerImplementation = SwarmContainer
            if runtime_accelerators:
                # Complain to the user that this is unlikely to work.
                logger.warning("Running job that needs accelerators with Docker, because "
                               "Singularity is not available. Accelerator and GPU support "
                               "is not yet implemented in the MiniWDL Docker "
                               "containerization implementation.")

        # Set up the MiniWDL container running stuff
        miniwdl_logger = logging.getLogger("MiniWDLContainers")
        miniwdl_config = WDL.runtime.config.Loader(miniwdl_logger)
        if not getattr(TaskContainerImplementation, 'toil_initialized__', False):
            # Initialize the cointainer system
            TaskContainerImplementation.global_init(miniwdl_config, miniwdl_logger)

            # TODO: We don't want to use MiniWDL's resource limit logic, but
            # we'd have to get at the _SubprocessScheduler that is internal to
            # the WDL.runtime.backend.cli_subprocess.SubprocessBase class to
            # hack it out of e.g. SingularityContainer, so for now we bring it
            # up. If we don't do this, we error out trying to make
            # _SubprocessScheduler instances because its class-level condition
            # variable doesn't exist.
            TaskContainerImplementation.detect_resource_limits(miniwdl_config, miniwdl_logger)

            # And remember we did it
            setattr(TaskContainerImplementation, 'toil_initialized__', True)
            # TODO: not thread safe!

        # Records, if we use a container, where its workdir is on our
        # filesystem, so we can interpret file anmes and globs relative to
        # there.
        workdir_in_container: Optional[str] = None

        if self._task.command:
            # When the command string references a File, we need to get a path to the file on a local disk, which the commnad will be able to actually use, accounting for e.g. containers.
            # TODO: Figure out whan the command template actually uses File values and lazily download them.
            # For now we just grab all the File values in the inside-the-task environment, since any of them *might* be used.
            # TODO: MiniWDL can parallelize the fetch
            bindings = devirtualize_files(bindings, standard_library)

            # Make the container object
            # TODO: What is this?
            run_id = str(uuid.uuid4())
            # Directory on the host where the conteiner is allowed to put files.
            host_dir = os.path.abspath('.')
            # Container working directory is guaranteed (?) to be at "work" inside there
            workdir_in_container = os.path.join(host_dir, "work")
            task_container = TaskContainerImplementation(miniwdl_config, run_id, host_dir)

            if isinstance(task_container, SingularityContainer):
                # We need to patch the Singularity container run invocation

                # We might need to send GPUs and the current miniwdl doesn't do
                # that for Singularity. And we might need to *not* try and use
                # --fakeroot if we lack sub-UIDs. So we sneakily monkey patch it
                # here.
                original_run_invocation = task_container._run_invocation
                def patched_run_invocation(*args: Any, **kwargs: Any) -> List[str]:
                    """
                    Invoke the original _run_invocation to get a base Singularity
                    command line, and then adjust the result to pass GPUs and not
                    fake root if needed.
                    """
                    command_line: List[str] = original_run_invocation(*args, **kwargs)

                    logger.debug('MiniWDL wants to run command line: %s', command_line)

                    # "exec" can be at index 1 or 2 depending on if we have a --verbose.
                    subcommand_index = 2 if command_line[1] == "--verbose" else 1

                    if '--fakeroot' in command_line and not self.can_fake_root():
                        # We can't fake root so don't try.
                        command_line.remove('--fakeroot')

                    extra_flags: Set[str] = set()
                    accelerators_needed: Optional[List[AcceleratorRequirement]] = self.accelerators
                    if accelerators_needed is not None:
                        for accelerator in accelerators_needed:
                            if accelerator['kind'] == 'gpu':
                                if accelerator['brand'] == 'nvidia':
                                    # Tell Singularity to expose nvidia GPUs
                                    extra_flags.add('--nv')
                                elif accelerator['api'] == 'rocm':
                                    # Tell Singularity to expose ROCm GPUs
                                    extra_flags.add('--nv')
                                else:
                                    raise RuntimeError('Cannot expose allocated accelerator %s to Singularity job', accelerator)

                    for flag in extra_flags:
                        # Put in all those flags
                        command_line.insert(subcommand_index + 1, flag)

                    logger.debug('Amended command line to: %s', command_line)

                    # Return the modified command line
                    return command_line

                # Apply the patch
                task_container._run_invocation = patched_run_invocation #  type: ignore

            # Show the runtime info to the container
            task_container.process_runtime(miniwdl_logger, {binding.name: binding.value for binding in runtime_bindings})

            # Tell the container to take up all these files. It will assign
            # them all new paths in task_container.input_path_map which we can
            # read. We also get a task_container.host_path() to go the other way.
            task_container.add_paths(get_file_paths_in_bindings(bindings))
            logger.debug("Using container path map: %s", task_container.input_path_map)

            # Replace everything with in-container paths for the command.
            # TODO: MiniWDL deals with directory paths specially here.
            contained_bindings = map_over_files_in_bindings(bindings, lambda path: task_container.input_path_map[path])

            # Make a new standard library for evaluating the command specifically, which only deals with in-container paths and out-of-container paths.
            command_library = ToilWDLStdLibTaskCommand(file_store, task_container)

            # Work out the command string, and unwrap it
            command_string: str = evaluate_named_expression(self._task, "command", WDL.Type.String(), self._task.command, contained_bindings, command_library).coerce(WDL.Type.String()).value

            # Grab the standard out and error paths. MyPy complains if we call
            # them because in the current MiniWDL version they are untyped.
            # TODO: MyPy will complain if we accomodate this and they later
            # become typed.
            host_stdout_txt: str = task_container.host_stdout_txt() #  type: ignore
            host_stderr_txt: str = task_container.host_stderr_txt() #  type: ignore

            if isinstance(task_container, SingularityContainer):
                # Before running the command, we need to make sure the container's
                # image is already pulled, so MiniWDL doesn't try and pull it.
                # MiniWDL only locks its cache directory within a process, and we
                # need to coordinate with other processes sharing the cache.
                with global_mutex(os.environ['MINIWDL__SINGULARITY__IMAGE_CACHE'], 'toil_miniwdl_sif_cache_mutex'):
                    # Also lock the Singularity layer cache in case it is shared with a different set of hosts
                    # TODO: Will these locks work well across machines???
                    with global_mutex(os.environ['SINGULARITY_CACHEDIR'], 'toil_singularity_cache_mutex'):
                        with ExitStack() as cleanup:
                            task_container._pull(miniwdl_logger, cleanup)

            # Run the command in the container
            logger.info('Executing command in %s: %s', task_container, command_string)
            try:
                task_container.run(miniwdl_logger, command_string)
            finally:
                if os.path.exists(host_stderr_txt):
                    logger.info('Standard error at %s: %s', host_stderr_txt, open(host_stderr_txt).read())
                if os.path.exists(host_stdout_txt):
                    logger.info('Standard output at %s: %s', host_stdout_txt, open(host_stdout_txt).read())

        else:
            # We need to fake stdout and stderr, since nothing ran but the
            # standard library lets you grab them. TODO: Can these be None?
            host_stdout_txt = "/dev/null"
            host_stderr_txt = "/dev/null"

        # Evaluate all the outputs in their special library context
        # We need to evaluate globs and relative paths relative to the
        # container's workdir if any, but everything else doesn't need to seem
        # to run in the container; there's no way to go from
        # container-determined strings that are absolute paths to WDL File
        # objects, and like MiniWDL we can say we only support
        # working-directory-based relative paths for globs.
        outputs_library = ToilWDLStdLibTaskOutputs(file_store, host_stdout_txt, host_stderr_txt, current_directory_override=workdir_in_container)
        output_bindings: WDLBindings = WDL.Env.Bindings()
        for output_decl in self._task.outputs:
            output_bindings = output_bindings.bind(output_decl.name, evaluate_decl(output_decl, bindings, outputs_library))

        # Drop any files from the output which don't actually exist
        output_bindings = drop_missing_files(output_bindings, current_directory_override=workdir_in_container)

        # TODO: Check the output bindings against the types of the decls so we
        # can tell if we have a null in a value that is supposed to not be
        # nullable. We can't just look at the types on the values themselves
        # because those are all the non-nullable versions.

        # Upload any files in the outputs if not uploaded already. Accounts for how relative paths may still need to be container-relative.
        output_bindings = virtualize_files(output_bindings, outputs_library)

        return output_bindings

class WDLWorkflowNodeJob(WDLBaseJob):
    """
    Job that evaluates a WDL workflow node.
    """

    def __init__(self, node: WDL.Tree.WorkflowNode, prev_node_results: Sequence[Promised[WDLBindings]], namespace: str, **kwargs: Any) -> None:
        """
        Make a new job to run a workflow node to completion.
        """
        super().__init__(unitName=node.workflow_node_id, displayName=node.workflow_node_id, **kwargs)

        self._node = node
        self._prev_node_results = prev_node_results
        self._namespace = namespace

        if isinstance(self._node, WDL.Tree.Call):
            logger.debug("Preparing job for call node %s", self._node.workflow_node_id)

    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually execute the workflow node.
        """
        super().run(file_store)
        logger.info("Running node %s", self._node.workflow_node_id)

        # Combine the bindings we get from previous jobs
        incoming_bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)

        if isinstance(self._node, WDL.Tree.Decl):
            # This is a variable assignment
            logger.info('Setting %s to %s', self._node.name, self._node.expr)
            value = evaluate_decl(self._node, incoming_bindings, standard_library)
            return incoming_bindings.bind(self._node.name, value)
        elif isinstance(self._node, WDL.Tree.Call):
            # This is a call of a task or workflow

            # Fetch all the inputs we are passing and bind them.
            # The call is only allowed to use these.
            logger.debug("Evaluating step inputs")
            input_bindings = evaluate_call_inputs(self._node, self._node.inputs, incoming_bindings, standard_library)

            # Bindings may also be added in from the enclosing workflow inputs
            # TODO: this is letting us also inject them from the workflow body.
            # TODO: Can this result in picking up non-namespaced values that
            # aren't meant to be inputs, by not changing their names?
            passed_down_bindings = incoming_bindings.enter_namespace(self._node.name)

            if isinstance(self._node.callee, WDL.Tree.Workflow):
                # This is a call of a workflow
                subjob: Job = WDLWorkflowJob(self._node.callee, [input_bindings, passed_down_bindings], self._node.callee_id, f'{self._namespace}.{self._node.name}')
                self.addChild(subjob)
            elif isinstance(self._node.callee, WDL.Tree.Task):
                # This is a call of a task
                subjob = WDLTaskJob(self._node.callee, [input_bindings, passed_down_bindings], self._node.callee_id, f'{self._namespace}.{self._node.name}')
                self.addChild(subjob)
            else:
                raise WDL.Error.InvalidType(self._node, "Cannot call a " + str(type(self._node.callee)))

            # We need to agregate outputs namespaced with our node name, and existing bindings
            namespace_job = WDLNamespaceBindingsJob(self._node.name, [subjob.rv()])
            subjob.addFollowOn(namespace_job)
            self.addChild(namespace_job)

            combine_job = WDLCombineBindingsJob([namespace_job.rv(), incoming_bindings])
            namespace_job.addFollowOn(combine_job)
            self.addChild(combine_job)

            return combine_job.rv()
        elif isinstance(self._node, WDL.Tree.Scatter):
            subjob = WDLScatterJob(self._node, [incoming_bindings], self._namespace)
            self.addChild(subjob)
            # Scatters don't really make a namespace, just kind of a scope?
            # TODO: Let stuff leave scope!
            return subjob.rv()
        elif isinstance(self._node, WDL.Tree.Conditional):
            subjob = WDLConditionalJob(self._node, [incoming_bindings], self._namespace)
            self.addChild(subjob)
            # Conditionals don't really make a namespace, just kind of a scope?
            # TODO: Let stuff leave scope!
            return subjob.rv()
        else:
            raise WDL.Error.InvalidType(self._node, "Unimplemented WorkflowNode: " + str(type(self._node)))

class WDLCombineBindingsJob(WDLBaseJob):
    """
    Job that collects the results from WDL workflow nodes and combines their
    environment changes.
    """

    def __init__(self, prev_node_results: Sequence[Promised[WDLBindings]], underlay: Optional[Promised[WDLBindings]] = None, remove: Optional[Promised[WDLBindings]] = None, **kwargs: Any) -> None:
        """
        Make a new job to combine the results of previous jobs.

        If underlay is set, those bindings will be injected to be overridden by other bindings.

        If remove is set, bindings there will be subtracted out of the result.
        """
        super().__init__(**kwargs)

        self._prev_node_results = prev_node_results
        self._underlay = underlay
        self._remove = remove

    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Aggregate incoming results.
        """
        super().run(file_store)
        combined = combine_bindings(unwrap_all(self._prev_node_results))
        if self._underlay is not None:
            # Fill in from the underlay anything not defined in anything else.
            combined = combine_bindings([combined, unwrap(self._underlay).subtract(combined)])
        if self._remove is not None:
            # We need to take stuff out of scope
            combined = combined.subtract(unwrap(self._remove))
        return combined

class WDLNamespaceBindingsJob(WDLBaseJob):
    """
    Job that puts a set of bindings into a namespace.
    """

    def __init__(self, namespace: str, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        """
        Make a new job to namespace results.
        """
        super().__init__(**kwargs)

        self._namespace = namespace
        self._prev_node_results = prev_node_results

    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Apply the namespace
        """
        super().run(file_store)
        return combine_bindings(unwrap_all(self._prev_node_results)).wrap_namespace(self._namespace)

class WDLSectionJob(WDLBaseJob):
    """
    Job that can create more graph for a section of the wrokflow.
    """

    def __init__(self, namespace: str, **kwargs: Any) -> None:
        """
        Make a WDLSectionJob where the interior runs in the given namespace,
        starting with the root workflow.
        """
        super().__init__(**kwargs)
        self._namespace = namespace

    def create_subgraph(self, nodes: Sequence[WDL.Tree.WorkflowNode], gather_nodes: Sequence[WDL.Tree.Gather], environment: WDLBindings, local_environment: Optional[WDLBindings] = None) -> Job:
        """
        Make a Toil job to evaluate a subgraph inside a workflow or workflow
        section.

        :returns: a child Job that will return the aggregated environment
                  after running all the things in the section.

        :param gather_nodes: Names exposed by these will always be defined
               with something, even if the code that defines them does
               not actually run.
        :param environment: Bindings in this environment will be used
               to evaluate the subgraph and will be passed through.
        :param local_environment: Bindings in this environment will be
               used to evaluate the subgraph but will go out of scope
               at the end of the section.
        """

        # We need to track the dependency universe; some of our child nodes may
        # depend on nodes that are e.g. inputs to the workflow that encloses
        # the section that encloses this section, and we need to just assume
        # those are already available, even though we don't have access to the
        # complete list. So we make a set of everything we actually do need to
        # care about resolving, instead.
        dependabes: Set[str] = set()

        if local_environment is not None:
            # Bring local environment into scope
            environment = combine_bindings([environment, local_environment])

        # What nodes exist, under their IDs?
        wdl_id_to_wdl_node: Dict[str, WDL.Tree.WorkflowNode] = {node.workflow_node_id: node for node in nodes if isinstance(node, WDL.Tree.WorkflowNode)}
        dependabes |= set(wdl_id_to_wdl_node.keys())

        # That doesn't include gather nodes, which in the Toil interpreter we
        # handle as part of their enclosing section, without individual Toil
        # jobs for each. So make a map from gather ID to the section node ID.
        gather_to_section: Dict[str, str] = {}
        for node in nodes:
            if isinstance(node, WDL.Tree.WorkflowSection):
                for gather_node in node.gathers.values():
                    gather_to_section[gather_node.workflow_node_id] = node.workflow_node_id
        dependabes |= set(gather_to_section.keys())

        # To make Toil jobs, we need all the jobs they depend on made so we can
        # call .rv(). So we need to solve the workflow DAG ourselves to set it up
        # properly.

        # We also need to make sure to bubble up dependencies from inside
        # sections. A conditional might only appear to depend on the variables
        # in the conditional expression, but its body can depend on other
        # stuff, and we need to make sure that that stuff has finished and
        # updated the environment before the conditional body runs. TODO: This
        # is because Toil can't go and get and add successors to the relevant
        # jobs later, while MiniWDL's engine apparently can. This ends up
        # reducing parallelism more than would strictly be necessary; nothing
        # in the conditional can start until the dependencies of everything in
        # the conditional are ready.

        # What are the dependencies of all the body nodes on other body nodes?
        # Nodes can depend on other nodes actually in the tree, or on gathers
        # that belong to other nodes, but we rewrite the gather dependencies
        # through to the enclosing section node. Skip any dependencies on
        # anything not provided by another body node (such as on an input, or
        # something outside of the current section). TODO: This will need to
        # change if we let parallelism transcend sections.
        wdl_id_to_dependency_ids = {node_id: list({gather_to_section[dep] if dep in gather_to_section else dep for dep in recursive_dependencies(node) if dep in dependabes}) for node_id, node in wdl_id_to_wdl_node.items()}

        # Which of those are outstanding?
        wdl_id_to_outstanding_dependency_ids = copy.deepcopy(wdl_id_to_dependency_ids)

        # What nodes depend on each node?
        wdl_id_to_dependent_ids: Dict[str, Set[str]] = collections.defaultdict(set)
        for node_id, dependencies in wdl_id_to_dependency_ids.items():
            for dependency_id in dependencies:
                # Invert the dependency edges
                wdl_id_to_dependent_ids[dependency_id].add(node_id)

        # This will hold all the Toil jobs by WDL node ID
        wdl_id_to_toil_job: Dict[str, Job] = {}

        # And collect IDs of jobs with no successors to add a final sink job
        leaf_ids: Set[str] = set()

        # What nodes are ready?
        ready_node_ids = {node_id for node_id, dependencies in wdl_id_to_outstanding_dependency_ids.items() if len(dependencies) == 0}

        while len(wdl_id_to_outstanding_dependency_ids) > 0:
            logger.debug('Ready nodes: %s', ready_node_ids)
            logger.debug('Waiting nodes: %s', wdl_id_to_outstanding_dependency_ids)

            # Find a node that we can do now
            node_id = next(iter(ready_node_ids))

            # Say we are doing it
            ready_node_ids.remove(node_id)
            del wdl_id_to_outstanding_dependency_ids[node_id]
            logger.debug('Make Toil job for %s', node_id)

            # Collect the return values from previous jobs. Some nodes may have been inputs, without jobs.
            prev_jobs = [wdl_id_to_toil_job[prev_node_id] for prev_node_id in wdl_id_to_dependency_ids[node_id] if prev_node_id in wdl_id_to_toil_job]
            rvs: List[Union[WDLBindings, Promise]] = [prev_job.rv() for prev_job in prev_jobs]
            # We also need access to section-level bindings like inputs
            rvs.append(environment)

            # Use them to make a new job
            job = WDLWorkflowNodeJob(wdl_id_to_wdl_node[node_id], rvs, self._namespace)
            for prev_job in prev_jobs:
                # Connect up the happens-after relationships to make sure the
                # return values are available.
                # We have a graph that only needs one kind of happens-after
                # relationship, so we always use follow-ons.
                prev_job.addFollowOn(job)

            if len(prev_jobs) == 0:
                # Nothing came before this job, so connect it to the workflow.
                self.addChild(job)

            # Save the job
            wdl_id_to_toil_job[node_id] = job

            if len(wdl_id_to_dependent_ids[node_id]) == 0:
                # Nothing comes after this job, so connect it to sink
                leaf_ids.add(node_id)
            else:
                for dependent_id in wdl_id_to_dependent_ids[node_id]:
                    # For each job that waits on this job
                    wdl_id_to_outstanding_dependency_ids[dependent_id].remove(node_id)
                    logger.debug('Dependent %s no longer needs to wait on %s', dependent_id, node_id)
                    if len(wdl_id_to_outstanding_dependency_ids[dependent_id]) == 0:
                        # We were the last thing blocking them.
                        ready_node_ids.add(dependent_id)
                        logger.debug('Dependent %s is now ready', dependent_id)

        # Make the sink job
        leaf_rvs: List[Union[WDLBindings, Promise]] = [wdl_id_to_toil_job[node_id].rv() for node_id in leaf_ids]
        # Make sure to also send the section-level bindings
        leaf_rvs.append(environment)
        # And to fill in bindings from code not executed in this instantiation
        # with Null, and filter out stuff that should leave scope.
        sink = WDLCombineBindingsJob(
            leaf_rvs,
            underlay=self.make_gather_bindings(gather_nodes, WDL.Value.Null()),
            remove=local_environment
        )
        # It runs inside us
        self.addChild(sink)
        for node_id in leaf_ids:
            # And after all the leaf jobs.
            wdl_id_to_toil_job[node_id].addFollowOn(sink)

        return sink

    def make_gather_bindings(self, gathers: Sequence[WDL.Tree.Gather], undefined: WDL.Value.Base) -> WDLBindings:
        """
        Given a collection of Gathers, create bindings from every identifier
        gathered, to the given "undefined" placeholder (which would be Null for
        a single execution of the body, or an empty array for a completely
        unexecuted scatter).

        These bindings can be overlaid with bindings from the actual execution,
        so that references to names defined in unexecuted code get a proper
        default undefined value, and not a KeyError at runtime.

        The information to do this comes from MiniWDL's "gathers" system:
        <https://miniwdl.readthedocs.io/en/latest/WDL.html#WDL.Tree.WorkflowSection.gathers>

        TODO: This approach will scale O(n^2) when run on n nested
        conditionals, because generating these bindings for the outer
        conditional will visit all the bindings from the inner ones.
        """

        # We can just directly compose our bindings.
        new_bindings: WDLBindings = WDL.Env.Bindings()

        for gather_node in gathers:
            bindings_source = gather_node.final_referee
            # Since there's no namespacing to be done at intermediate Gather
            # nodes (we can't refer via a gather referee chain to the inside of
            # a Call), we can just jump to the end here.
            bindings_source = gather_node.final_referee
            if isinstance(bindings_source, WDL.Tree.Decl):
                # Bind the decl's name
                new_bindings = new_bindings.bind(bindings_source.name, undefined)
            elif isinstance(bindings_source, WDL.Tree.Call):
                # Bind each of the call's outputs, namespaced with the call.
                # The call already has a bindings for these to expressions.
                for call_binding in bindings_source.effective_outputs:
                    # TODO: We could try and map here instead
                    new_bindings = new_bindings.bind(call_binding.name, undefined)
            else:
                # Either something unrecognized or final_referee lied and gave us a Gather.
                raise TypeError(f"Cannot generate bindings for a gather over a {type(bindings_source)}")

        return new_bindings

class WDLScatterJob(WDLSectionJob):
    """
    Job that evaluates a scatter in a WDL workflow. Runs the body for each
    value in an array, and makes arrays of the new bindings created in each
    instance of the body. If an instance of the body doesn't create a binding,
    it gets a null value in the corresponding array.
    """
    def __init__(self, scatter: WDL.Tree.Scatter, prev_node_results: Sequence[Promised[WDLBindings]], namespace: str, **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL scatter. The scatter itself and the contents live in the given namespace.
        """
        super().__init__(namespace, **kwargs, unitName=scatter.workflow_node_id, displayName=scatter.workflow_node_id)

        # Because we need to return the return value of the workflow, we need
        # to return a Toil promise for the last/sink job in the workflow's
        # graph. But we can't save either a job that takes promises, or a
        # promise, in ourselves, because of the way that Toil resolves promises
        # at deserialization. So we need to do the actual building-out of the
        # workflow in run().

        logger.info("Preparing to run scatter on %s", scatter.variable)

        self._scatter = scatter
        self._prev_node_results = prev_node_results

    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Run the scatter.
        """
        super().run(file_store)

        logger.info("Running scatter on %s", self._scatter.variable)

        # Combine the bindings we get from previous jobs.
        # For a task we only see the inside-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)

        # Get what to scatter over
        scatter_value = evaluate_named_expression(self._scatter, self._scatter.variable, None, self._scatter.expr, bindings, standard_library)

        assert isinstance(scatter_value, WDL.Value.Array)

        scatter_jobs = []
        for item in scatter_value.value:
            # Make an instantiation of our subgraph for each possible value of
            # the variable. Make sure the variable is bound only for the
            # duration of the body.
            local_bindings: WDLBindings = WDL.Env.Bindings()
            local_bindings = local_bindings.bind(self._scatter.variable, item)
            # TODO: We need to turn values() into a list because MyPy seems to
            # think a dict_values isn't a Sequence. This is a waste of time to
            # appease MyPy but probably better than a cast?
            scatter_jobs.append(self.create_subgraph(self._scatter.body, list(self._scatter.gathers.values()), bindings, local_bindings))

        if len(scatter_jobs) == 0:
            # No scattering is needed. We just need to bind all the names.

            logger.info("No scattering is needed. Binding all scatter results to [].")

            # Define the value that should be seen for a name bound in the scatter
            # if nothing in the scatter actually runs. This should be some kind of
            # empty array.
            empty_array = WDL.Value.Array(WDL.Type.Any(optional=True, null=True), [])
            return self.make_gather_bindings(list(self._scatter.gathers.values()), empty_array)

        # Otherwise we actually have some scatter jobs.

        # Make a job at the end to aggregate.
        # Turn all the bindings created inside the scatter bodies into arrays
        # of maybe-optional values. Each body execution will define names it
        # doesn't make as nulls, so we don't have to worry about
        # totally-missing names.
        gather_job = WDLArrayBindingsJob([j.rv() for j in scatter_jobs], bindings)
        self.addChild(gather_job)
        for j in scatter_jobs:
            j.addFollowOn(gather_job)
        return gather_job.rv()

class WDLArrayBindingsJob(WDLBaseJob):
    """
    Job that takes all new bindings created in an array of input environments,
    relative to a base environment, and produces bindings where each new
    binding name is bound to an array of the values in all the input
    environments.

    Useful for producing the results of a scatter.
    """

    def __init__(self, input_bindings: Sequence[Promised[WDLBindings]], base_bindings: WDLBindings, **kwargs: Any) -> None:
        """
        Make a new job to array-ify the given input bindings.

        :param input_bindings: bindings visible to each evaluated iteration.
        :param base_bindings: bindings visible to *all* evaluated iterations,
               which should be constant across all of them and not made into
               arrays but instead passed through unchanged.
        """
        super().__init__(**kwargs)

        self._input_bindings = input_bindings
        self._base_bindings = base_bindings

    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Actually produce the array-ified bindings now that promised values are available.
        """
        super().run(file_store)

        # Subtract base bindings to get just the new bindings created in each input
        new_bindings = [env.subtract(self._base_bindings) for env in unwrap_all(self._input_bindings)]
        # Make a set of all the new names.
        # TODO: They ought to maybe have types? Spec just says "any scalar
        # outputs of these tasks is now an array", with no hint on what to do
        # if some tasks output nothing, some tasks output things of
        # incompatible types, etc.
        new_names = {b.name for env in new_bindings for b in env}

        result = self._base_bindings
        for name in new_names:
            # Determine the set of all types bound to the name, or None if a result is null.
            # Problem: the WDL type types are not hashable, so we need to do bad N^2 deduplication
            observed_types = []
            for env in new_bindings:
                binding_type = env.resolve(name).type if env.has_binding(name) else None
                if binding_type not in observed_types:
                    observed_types.append(binding_type)
            # Get the supertype of those types
            supertype: WDL.Type.Base = get_supertype(observed_types)
            # Bind an array of the values
            # TODO: We should be able to assume the binding is always there if this is a scatter, because we create and underlay bindings based on the gathers.
            result = result.bind(name, WDL.Value.Array(supertype, [env.resolve(name) if env.has_binding(name) else WDL.Value.Null() for env in new_bindings]))

        # Base bindings are already included so return the result
        return result

class WDLConditionalJob(WDLSectionJob):
    """
    Job that evaluates a conditional in a WDL workflow.
    """
    def __init__(self, conditional: WDL.Tree.Conditional, prev_node_results: Sequence[Promised[WDLBindings]], namespace: str, **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL conditional. The conditional itself and its contents live in the given namespace.
        """
        super().__init__(namespace, **kwargs, unitName=conditional.workflow_node_id, displayName=conditional.workflow_node_id)

        # Once again we need to ship the whole body template to be instantiated
        # into Toil jobs only if it will actually run.

        logger.info("Preparing to run conditional on %s", conditional.expr)

        self._conditional = conditional
        self._prev_node_results = prev_node_results

    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Run the conditional.
        """
        super().run(file_store)

        logger.info("Checking condition for %s: %s", self._conditional.workflow_node_id, self._conditional.expr)

        # Combine the bindings we get from previous jobs.
        # For a task we only see the insode-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)

        # Get the expression value. Fake a name.
        expr_value = evaluate_named_expression(self._conditional, "<conditional expression>", WDL.Type.Boolean(), self._conditional.expr, bindings, standard_library)

        if expr_value.value:
            # Evaluated to true!
            logger.info('Condition is true')
            # Run the body and return its effects
            body_job = self.create_subgraph(self._conditional.body, list(self._conditional.gathers.values()), bindings)
            return body_job.rv()
        else:
            logger.info('Condition is false')
            # Return the input bindings and null bindings for all our gathers.
            # Should not collide at all.
            gather_bindings = self.make_gather_bindings(list(self._conditional.gathers.values()), WDL.Value.Null())
            return combine_bindings([bindings, gather_bindings])

class WDLWorkflowJob(WDLSectionJob):
    """
    Job that evaluates an entire WDL workflow.
    """

    def __init__(self, workflow: WDL.Tree.Workflow, prev_node_results: Sequence[Promised[WDLBindings]], workflow_id: List[str], namespace: str, **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL workflow. The job returns the
        return value of the workflow.

        :param namespace: the namespace that the workflow's *contents* will be
               in. Caller has already added the workflow's own name.
        """
        super().__init__(namespace, **kwargs)

        # Because we need to return the return value of the workflow, we need
        # to return a Toil promise for the last/sink job in the workflow's
        # graph. But we can't save either a job that takes promises, or a
        # promise, in ourselves, because of the way that Toil resolves promises
        # at deserialization. So we need to do the actual building-out of the
        # workflow in run().

        logger.debug("Preparing to run workflow %s", workflow.name)


        self._workflow = workflow
        self._prev_node_results = prev_node_results
        self._workflow_id = workflow_id
        self._namespace = namespace

    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Run the workflow. Return the result of the workflow.
        """
        super().run(file_store)

        logger.info("Running workflow %s (%s) called as %s", self._workflow.name, self._workflow_id, self._namespace)

        # Combine the bindings we get from previous jobs.
        # For a task we only see the insode-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)

        if self._workflow.inputs:
            for input_decl in self._workflow.inputs:
                # Evaluate all the inputs that aren't pre-set
                bindings = bindings.bind(input_decl.name, evaluate_defaultable_decl(input_decl, bindings, standard_library))

        # Make jobs to run all the parts of the workflow
        sink = self.create_subgraph(self._workflow.body, [], bindings)

        if self._workflow.outputs:
            # Add evaluating the outputs after the sink
            outputs_job = WDLOutputsJob(self._workflow.outputs, sink.rv())
            sink.addFollowOn(outputs_job)
            # Caller takes care of namespacing the result
            return outputs_job.rv()
        else:
            # No outputs from this workflow.
            return WDL.Env.Bindings()

class WDLOutputsJob(WDLBaseJob):
    """
    Job which evaluates an outputs section (such as for a workflow).

    Returns an environment with just the outputs bound, in no namespace.
    """

    def __init__(self, outputs: List[WDL.Tree.Decl], bindings: Promised[WDLBindings], **kwargs: Any):
        """
        Make a new WDLWorkflowOutputsJob for the given workflow, with the given set of bindings after its body runs.
        """
        super().__init__(**kwargs)

        self._outputs = outputs
        self._bindings = bindings

    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Make bindings for the outputs.
        """
        super().run(file_store)

        # Evaluate all the outputs in the normal, non-task-outputs library context
        standard_library = ToilWDLStdLibBase(file_store)
        output_bindings: WDL.Env.Bindings[WDL.Value.Base] = WDL.Env.Bindings()
        for output_decl in self._outputs:
            output_bindings = output_bindings.bind(output_decl.name, evaluate_decl(output_decl, unwrap(self._bindings), standard_library))

        return output_bindings

class WDLRootJob(WDLSectionJob):
    """
    Job that evaluates an entire WDL workflow, and returns the workflow outputs
    namespaced with the workflow name. Inputs may or may not be namespaced with
    the workflow name; both forms are accepted.
    """

    def __init__(self, workflow: WDL.Tree.Workflow, inputs: WDLBindings, **kwargs: Any) -> None:
        """
        Create a subtree to run the workflow and namespace the outputs.
        """

        # The root workflow names the root namespace
        super().__init__(workflow.name, **kwargs)

        self._workflow = workflow
        self._inputs = inputs

    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually build the subgraph.
        """
        super().run(file_store)

        # Run the workflow. We rely in this to handle entering the input
        # namespace if needed, or handling free-floating inputs.
        workflow_job = WDLWorkflowJob(self._workflow, [self._inputs], [self._workflow.name], self._namespace)
        self.addChild(workflow_job)

        # And namespace its outputs
        namespace_job = WDLNamespaceBindingsJob(self._namespace, [workflow_job.rv()])
        workflow_job.addFollowOn(namespace_job)

        return namespace_job.rv()

def main() -> None:
    """
    A Toil workflow to interpret WDL input files.
    """

    parser = argparse.ArgumentParser(description='Runs WDL files with toil.')
    addOptions(parser, jobstore_as_flag=True)

    parser.add_argument("wdl_uri", type=str,
                        help="WDL document URI")
    parser.add_argument("inputs_uri", type=str, nargs='?',
                        help="WDL input JSON URI")
    parser.add_argument("--input", "-i", dest="inputs_uri", type=str,
                        help="WDL input JSON URI")
    parser.add_argument("--outputDialect", dest="output_dialect", type=str, default='cromwell', choices=['cromwell', 'miniwdl'],
                        help=("JSON output format dialect. 'cromwell' just returns the workflow's output"
                              "values as JSON, while 'miniwdl' nests that under an 'outputs' key, and "
                              "includes a 'dir' key where files are written."))
    parser.add_argument("--outputDirectory", "-o", dest="output_directory", type=str, default=None,
                        help=("Directory in which to save output files. By default a new directory is created in the current directory."))
    parser.add_argument("--outputFile", "-m", dest="output_file", type=argparse.FileType('w'), default=sys.stdout,
                        help="File to save output JSON to.")

    options = parser.parse_args(sys.argv[1:])

    # Make sure we have a jobStore
    if options.jobStore is None:
        # TODO: Move cwltoil's generate_default_job_store where we can use it
        options.jobStore = os.path.join(tempfile.mkdtemp(), 'tree')

    # Make sure we have an output directory and we don't need to ever worry
    # about a None, and MyPy knows it.
    # If we don't have a directory assigned, make one in the current directory.
    output_directory: str = options.output_directory if options.output_directory else tempfile.mkdtemp(prefix='wdl-out-', dir=os.getcwd())
    if not os.path.isdir(output_directory):
        # Make sure it exists
        os.mkdir(output_directory)


    with Toil(options) as toil:
        if options.restart:
            output_bindings = toil.restart()
        else:
            # Load the WDL document
            document: WDL.Tree.Document = WDL.load(options.wdl_uri, read_source=toil_read_source)

            if document.workflow is None:
                logger.critical("No workflow in document!")
                sys.exit(1)

            if options.inputs_uri:
                # Load the inputs. Use the same loading mechanism, which means we
                # have to break into async temporarily.
                downloaded = asyncio.run(toil_read_source(options.inputs_uri, [], None))
                try:
                    inputs = json.loads(downloaded.source_text)
                except json.JSONDecodeError as e:
                    logger.critical('Cannot parse JSON at %s: %s', downloaded.abspath, e)
                    sys.exit(1)
            else:
                inputs = {}
            # Parse out the available and required inputs. Each key in the
            # JSON ought to start with the workflow's name and then a .
            # TODO: WDL's Bindings[] isn't variant in the right way, so we
            # have to cast from more specific to less specific ones here.
            # The miniwld values_from_json function can evaluate
            # expressions in the inputs or something.
            WDLTypeDeclBindings = WDL.Env.Bindings[Union[WDL.Tree.Decl, WDL.Type.Base]]
            input_bindings = WDL.values_from_json(
                inputs,
                cast(WDLTypeDeclBindings, document.workflow.available_inputs),
                cast(Optional[WDLTypeDeclBindings], document.workflow.required_inputs),
                document.workflow.name
            )

            # Determine where to look for files referenced in the inputs, in addition to here.
            inputs_search_path = []
            if options.inputs_uri:
                inputs_search_path.append(options.inputs_uri)
                match = re.match('https://raw\.githubusercontent\.com/[^/]*/[^/]*/[^/]*/', options.inputs_uri)
                if match:
                    # Special magic for Github repos to make e.g.
                    # https://raw.githubusercontent.com/vgteam/vg_wdl/44a03d9664db3f6d041a2f4a69bbc4f65c79533f/params/giraffe.json
                    # work when it references things relative to repo root.
                    logger.info("Inputs appear to come from a Github repository; adding repository root to file search path")
                    inputs_search_path.append(match.group(0))

            # Import any files in the bindings
            input_bindings = import_files(input_bindings, toil, inputs_search_path)

            # TODO: Automatically set a good MINIWDL__SINGULARITY__IMAGE_CACHE ?

            # Run the workflow and get its outputs namespaced with the workflow name.
            root_job = WDLRootJob(document.workflow, input_bindings)
            output_bindings = toil.start(root_job)
        assert isinstance(output_bindings, WDL.Env.Bindings)

        # Fetch all the output files
        # TODO: deduplicate with _devirtualize_filename
        def devirtualize_output(filename: str) -> str:
            """
            'devirtualize' a file using the "toil" object instead of a filestore.
            Returns its local path.
            """
            if filename.startswith(TOIL_URI_SCHEME):
                # This is a reference to the Toil filestore.
                # Deserialize the FileID and required basename
                file_id, file_basename = unpack_toil_uri(filename)
                # Figure out where it should go.
                # TODO: Deal with name collisions
                dest_name = os.path.join(output_directory, file_basename)
                # Export the file
                toil.exportFile(file_id, dest_name)
                # And return where we put it
                return dest_name
            elif filename.startswith('http:') or filename.startswith('https:') or filename.startswith('s3:') or filename.startswith('gs:'):
                # This is a URL that we think Toil knows how to read.
                imported = toil.import_file(filename)
                if imported is None:
                    raise FileNotFoundError(f"Could not import URL {filename}")
                # Get a basename from the URL.
                # TODO: Deal with name collisions
                file_basename = os.path.basename(urlsplit(filename).path)
                # Do the same as we do for files we actually made.
                dest_name = os.path.join(output_directory, file_basename)
                toil.exportFile(imported, dest_name)
                return dest_name
            else:
                # Not a fancy file
                return filename

        # Make all the files local files
        output_bindings = map_over_files_in_bindings(output_bindings, devirtualize_output)

        # Report the result in the right format
        outputs = WDL.values_to_json(output_bindings)
        if options.output_dialect == 'miniwdl':
            outputs = {'dir': output_directory, 'outputs': outputs}
        options.output_file.write(json.dumps(outputs))
        options.output_file.write('\n')


if __name__ == "__main__":
    main()





