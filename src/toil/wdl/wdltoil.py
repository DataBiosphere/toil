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
from __future__ import annotations

import asyncio
import errno
import io
import json
import logging
import os
import platform
import re
import shlex
import shutil
import stat
import subprocess
import sys
import textwrap
import uuid
from contextlib import ExitStack, contextmanager
from graphlib import TopologicalSorter
from tempfile import mkstemp, gettempdir
from typing import (Any,
                    Callable,
                    Dict,
                    Generator,
                    Iterable,
                    Iterator,
                    List,
                    Optional,
                    Sequence,
                    Set,
                    Tuple,
                    Type,
                    TypeVar,
                    Union,
                    cast)
from mypy_extensions import Arg, DefaultArg
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urljoin, urlsplit
from functools import partial

import WDL.Error
import WDL.runtime.config
from configargparse import ArgParser
from WDL._util import byte_size_units
from WDL.Tree import ReadSourceResult
from WDL.CLI import print_error
from WDL.runtime.backend.docker_swarm import SwarmContainer
from WDL.runtime.backend.singularity import SingularityContainer
from WDL.runtime.task_container import TaskContainer

from toil.batchSystems.abstractBatchSystem import InsufficientSystemResources
from toil.common import Toil, addOptions
from toil.exceptions import FailedJobsException
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.job import (AcceleratorRequirement,
                      Job,
                      Promise,
                      Promised,
                      TemporaryID,
                      parse_accelerator,
                      unwrap,
                      unwrap_all)
from toil.jobStores.abstractJobStore import (AbstractJobStore, UnimplementedURLException,
                                             InvalidImportExportUrlException, LocatorException)
from toil.lib.accelerators import get_individual_local_accelerators
from toil.lib.conversions import convert_units, human2bytes
from toil.lib.io import mkdtemp
from toil.lib.memoize import memoize
from toil.lib.misc import get_user_name
from toil.lib.resources import ResourceMonitor
from toil.lib.threading import global_mutex
from toil.provisioners.clusterScaler import JobTooBigError


logger = logging.getLogger(__name__)


@contextmanager
def wdl_error_reporter(task: str, exit: bool = False, log: Callable[[str], None] = logger.critical) -> Generator[None, None, None]:
    """
    Run code in a context where WDL errors will be reported with pretty formatting.
    """

    try:
        yield
    except (
        WDL.Error.EvalError,
        WDL.Error.SyntaxError,
        WDL.Error.ImportError,
        WDL.Error.ValidationError,
        WDL.Error.MultipleValidationErrors,
        FileNotFoundError,
        InsufficientSystemResources,
        LocatorException,
        InvalidImportExportUrlException,
        UnimplementedURLException,
        JobTooBigError
    ) as e:
        # Don't expose tracebacks to the user for exceptions that may be expected
        log("Could not " + task + " because:")

        # These are the errors that MiniWDL's parser can raise and its reporter
        # can report (plus some extras). See
        # https://github.com/chanzuckerberg/miniwdl/blob/a780b1bf2db61f18de37616068968b2bb4c2d21c/WDL/CLI.py#L91-L97.
        #
        # We are going to use MiniWDL's pretty printer to print them.
        # Make the MiniWDL stuff on stderr loud so people see it
        sys.stderr.write("\n" + "🚨" * 3 + "\n")
        print_error(e)
        sys.stderr.write("🚨" * 3 + "\n\n")
        if exit:
            # Stop right now
            sys.exit(1)
        else:
            # Reraise the exception to stop
            raise

F = TypeVar('F', bound=Callable[..., Any])
def report_wdl_errors(task: str, exit: bool = False, log: Callable[[str], None] = logger.critical) -> Callable[[F], F]:
    """
    Create a decorator to report WDL errors with the given task message.

    Decorator can then be applied to a function, and if a WDL error happens it
    will say that it could not {task}.
    """
    def decorator(decoratee: F) -> F:
        """
        Decorate a function with WDL error reporting.
        """
        def decorated(*args: Any, **kwargs: Any) -> Any:
            """
            Run the decoratee and handle WDL errors.
            """
            with wdl_error_reporter(task, exit=exit, log=log):
                return decoratee(*args, **kwargs)
        return cast(F, decorated)
    return decorator

def remove_common_leading_whitespace(expression: WDL.Expr.String, tolerate_blanks: bool = True, tolerate_dedents: bool = False, tolerate_all_whitespace: bool = True, debug: bool = False) -> WDL.Expr.String:
    """
    Remove "common leading whitespace" as defined in the WDL 1.1 spec.

    See <https://github.com/openwdl/wdl/blob/main/versions/1.1/SPEC.md#stripping-leading-whitespace>.

    Operates on a WDL.Expr.String expression that has already been parsed.

    :param tolerate_blanks: If True, don't allow totally blank lines to zero
        the common whitespace.

    :param tolerate_dedents: If True, remove as much of the whitespace on the
        first indented line as is found on subesquent lines, regardless of
        whether later lines are out-dented relative to it.

    :param tolerate_all_whitespace: If True, don't allow all-whitespace lines
        to reduce the common whitespace prefix.

    :param debug: If True, the function will show its work by logging at debug
        level.
    """

    # The expression has a "parts" list consisting of interleaved string
    # literals and placeholder expressions.
    #
    # TODO: We assume that there are no newlines in the placeholders.
    #
    # TODO: Look at the placeholders and their line and end_line values and try
    # and guess if they should reduce the amount of common whitespace.

    if debug:
        logger.debug("Parts: %s", expression.parts)

    # We split the parts list into lines, which are also interleaved string
    # literals and placeholder expressions.
    lines: List[List[Union[str, WDL.Expr.Placeholder]]] = [[]]
    for part in expression.parts:
        if isinstance(part, str):
            # It's a string. Split it into lines.
            part_lines = part.split("\n")
            # Part before any newline goes at the end of the current line
            lines[-1].append(part_lines[0])
            for part_line in part_lines[1:]:
                # Any part after a newline starts a new line
                lines.append([part_line])
        else:
            # It's a placeholder. Put it at the end of the current line.
            lines[-1].append(part)

    if debug:
        logger.debug("Lines: %s", lines)

    # Then we compute the common amount of leading whitespace on all the lines,
    # looking at the first string literal.
    # This will be the longest common whitespace prefix, or None if not yet detected.
    common_whitespace_prefix: Optional[str] = None
    for line in lines:
        if len(line) == 0:
            # TODO: how should totally empty lines be handled? Not in the spec!
            if not tolerate_blanks:
                # There's no leading whitespace here!
                common_whitespace_prefix = ""
            continue
        elif isinstance(line[0], WDL.Expr.Placeholder):
            # TODO: How can we convert MiniWDL's column numbers into space/tab counts or sequences?
            #
            # For now just skip these too.
            continue
        else:
            # The line starts with a string
            assert isinstance(line[0], str)
            if len(line[0]) == 0:
                # Still totally empty though!
                if not tolerate_blanks:
                    # There's no leading whitespace here!
                    common_whitespace_prefix = ""
                continue
            if len(line) == 1 and tolerate_all_whitespace and all(x in (' ', '\t') for x in line[0]):
                # All-whitespace lines shouldn't count
                continue
            # TODO: There are good algorithms for common prefixes. This is a bad one.
            # Find the number of leading whitespace characters
            line_whitespace_end = 0
            while line_whitespace_end < len(line[0]) and line[0][line_whitespace_end] in (' ', '\t'):
                line_whitespace_end += 1
            # Find the string of leading whitespace characters
            line_whitespace_prefix = line[0][:line_whitespace_end]

            if ' ' in line_whitespace_prefix and '\t' in line_whitespace_prefix:
                # Warn and don't change anything if spaces and tabs are mixed, per the spec.
                logger.warning("Line in command at %s mixes leading spaces and tabs! Not removing leading whitespace!", expression.pos)
                return expression

            if common_whitespace_prefix is None:
                # This is the first line we found, so it automatically has the common prefic
                common_whitespace_prefix = line_whitespace_prefix
            elif not tolerate_dedents:
                # Trim the common prefix down to what we have for this line
                if not line_whitespace_prefix.startswith(common_whitespace_prefix):
                    # Shorten to the real shared prefix.
                    # Hackily make os.path do it for us,
                    # character-by-character. See
                    # <https://stackoverflow.com/a/6718435>
                    common_whitespace_prefix = os.path.commonprefix([common_whitespace_prefix, line_whitespace_prefix])

    if common_whitespace_prefix is None:
        common_whitespace_prefix = ""

    if debug:
        logger.debug("Common Prefix: '%s'", common_whitespace_prefix)

    # Then we trim that much whitespace off all the leading strings.
    # We tolerate the common prefix not *actually* being common and remove as
    # much of it as is there, to support tolerate_dedents.

    def first_mismatch(prefix: str, value: str) -> int:
        """
        Get the index of the first character in value that does not match the corresponding character in prefix, or the length of the shorter string.
        """
        for n, (c1, c2) in enumerate(zip(prefix, value)):
            if c1 != c2:
                return n
        return min(len(prefix), len(value))

    # Trim up to the first mismatch vs. the common prefix if the line starts with a string literal.
    stripped_lines = [
        (
            (
                cast(
                    List[Union[str, WDL.Expr.Placeholder]],
                    [line[0][first_mismatch(common_whitespace_prefix, line[0]):]]
                ) + line[1:]
            )
            if len(line) > 0 and isinstance(line[0], str) else
            line
        )
        for line in lines
    ]
    if debug:
        logger.debug("Stripped Lines: %s", stripped_lines)

    # Then we reassemble the parts and make a new expression.
    # Build lists and turn the lists into strings later
    new_parts: List[Union[List[str], WDL.Expr.Placeholder]] = []
    for i, line in enumerate(stripped_lines):
        if i > 0:
            # This is a second line, so we need to tack on a newline.
            if len(new_parts) > 0 and isinstance(new_parts[-1], list):
                # Tack on to existing string collection
                new_parts[-1].append("\n")
            else:
                # Make a new string collection
                new_parts.append(["\n"])
        if len(line) > 0 and isinstance(line[0], str) and i > 0:
            # Line starts with a string we need to merge with the last string.
            # We know the previous line now ends with a string collection, so tack it on.
            assert isinstance(new_parts[-1], list)
            new_parts[-1].append(line[0])
            # Make all the strings into string collections in the rest of the line
            new_parts += [([x] if isinstance(x, str) else x) for x in line[1:]]
        else:
            # No string merge necessary
            # Make all the strings into string collections in the whole line
            new_parts += [([x] if isinstance(x, str) else x) for x in line]

    if debug:
        logger.debug("New Parts: %s", new_parts)

    # Now go back to the alternating strings and placeholders that MiniWDL wants
    new_parts_merged: List[Union[str, WDL.Expr.Placeholder]] = [("".join(x) if isinstance(x, list) else x) for x in new_parts]

    if debug:
        logger.debug("New Parts Merged: %s", new_parts_merged)

    modified = WDL.Expr.String(expression.pos, new_parts_merged, expression.command)
    # Fake the type checking of the modified expression.
    # TODO: Make MiniWDL expose a real way to do this?
    modified._type = expression._type
    return modified


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

async def toil_read_source(uri: str, path: List[str], importer: Optional[WDL.Tree.Document]) -> ReadSourceResult:
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
        return ReadSourceResult(string_data, candidate_uri)

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

def get_supertype(types: Sequence[WDL.Type.Base]) -> WDL.Type.Base:
    """
    Get the supertype that can hold values of all the given types.
    """
    supertype = None
    optional = False
    for typ in types:
        if isinstance(typ, WDL.Type.Any):
            # ignore an Any type, as we represent a bottom type as Any. See https://miniwdl.readthedocs.io/en/latest/WDL.html#WDL.Type.Any
            # and https://github.com/openwdl/wdl/blob/e43e042104b728df1f1ad6e6145945d2b32331a6/SPEC.md?plain=1#L1484
            optional = optional or typ.optional
        elif supertype is None:
            supertype = typ
            optional = optional or typ.optional
        else:
            # We have conflicting types
            raise RuntimeError(f"Cannot generate a supertype from conflicting types: {types}")
    if supertype is None:
        return WDL.Type.Any(null=optional)  # optional flag isn't used in Any
    return supertype.copy(optional=optional)


def for_each_node(root: WDL.Tree.WorkflowNode) -> Iterator[WDL.Tree.WorkflowNode]:
    """
    Iterate over all WDL workflow nodes in the given node, including inputs,
    internal nodes of conditionals and scatters, and gather nodes.
    """

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

# We always virtualize any file into a URI. However, when coercing from string to file,
# it is not necessary that the file needs to exist. See https://github.com/openwdl/wdl/issues/667
# So use a sentinel to indicate nonexistent files instead of immediately raising an error
# This is done instead of not virtualizing, using the string as a filepath, and coercing to None/null at use.
# This is because the File must represent some location on its corresponding machine.
# If a task runs on a node where a file does not exist, and passes that file as an input into another task,
# we need to remember that the file does not exist from the original node
# ex:
# Task T1 runs on node N1 with file F at path P, but P does not exist on node N1
# Task T1 passes file F to task T2 to run on node N2
# Task T2 runs on node N2, P exists on node N2, but file F cannot exist
# We also want to store the filename even if it does not exist, so use a sentinel URI scheme (can be useful in error messages)
TOIL_NONEXISTENT_URI_SCHEME = 'nonexistent:'

def pack_toil_uri(file_id: FileID, task_path: str, dir_id: uuid.UUID, file_basename: str) -> str:
    """
    Encode a Toil file ID and metadata about who wrote it as a URI.

    The URI will start with the scheme in TOIL_URI_SCHEME.
    """

    # We urlencode everything, including any slashes. We need to use a slash to
    # set off the actual filename, so the WDL standard library basename
    # function works correctly.
    return TOIL_URI_SCHEME + "/".join([
        quote(file_id.pack(), safe=''),
        quote(task_path, safe=''),
        quote(str(dir_id)),
        quote(file_basename, safe='')
    ])

def unpack_toil_uri(toil_uri: str) -> Tuple[FileID, str, str, str]:
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
    if len(parts) != 4:
        raise ValueError(f"Wrong number of path segments in URI: {toil_uri}")
    file_id = FileID.unpack(unquote(parts[0]))
    task_path = unquote(parts[1])
    parent_id = unquote(parts[2])
    file_basename = unquote(parts[3])

    return file_id, task_path, parent_id, file_basename


DirectoryNamingStateDict = Dict[str, Tuple[Dict[str, str], Set[str]]]
def choose_human_readable_directory(root_dir: str, source_task_path: str, parent_id: str, state: DirectoryNamingStateDict) -> str:
    """
    Select a good directory to save files from a task and source directory in.

    The directories involved may not exist.

    :param root_dir: Directory that the path will be under
    :param source_task_path: The dotted WDL name of whatever generated the
        file. We assume this is an acceptable filename component.
    :param parent_id: UUID of the directory that the file came from. All files
        with the same parent ID will be placed as siblings files in a shared
        parent directory.
    :param state: A state dict that must be passed to repeated calls.
    """

    # We need to always put things as siblings if they come from the same UUID
    # even if different tasks generated them. So the first task we download
    # from will get to name the directory for a parent ID.

    # Get the state info for this root directory.
    #
    # For each parent ID, we need the directory we are using for it (dict).
    #
    # For each local directory, we need to know if we used it for a parent ID already (set).
    id_to_dir, used_dirs = state.setdefault(root_dir, ({}, set()))
    logger.debug("Pick location for parent %s source %s root %s against id map %s and used set %s", parent_id, source_task_path, root_dir, id_to_dir, used_dirs)
    if parent_id not in id_to_dir:
        # Make a path for this parent named after this source task

        # Problem: If we put any files right at the root of the source task
        # directory, then we can't put any directories with guessable names in
        # it, because we might later come across a file with that name that
        # must be sibling to an existing file. So if a task uploads from
        # multiple sources or otherwise manages to collide with our numbering,
        # we will make multiple directories for it.

        candidate = source_task_path
        deduplicator = len(used_dirs)
        while candidate in used_dirs:
            # We use one run of deduplicating numbers across all the names.
            candidate = f"{source_task_path}-{deduplicator}"
            deduplicator += 1

        id_to_dir[parent_id] = candidate
        used_dirs.add(candidate)

    result = os.path.join(root_dir, id_to_dir[parent_id])
    logger.debug("Picked path %s", result)
    return result


def evaluate_output_decls(output_decls: List[WDL.Tree.Decl], all_bindings: WDL.Env.Bindings[WDL.Value.Base], standard_library: ToilWDLStdLibBase) -> WDL.Env.Bindings[WDL.Value.Base]:
    """
    Evaluate output decls with a given bindings environment and standard library.
    Creates a new bindings object that only contains the bindings from the given decls.
    Guarantees that each decl in `output_decls` can access the variables defined by the previous ones.
    :param all_bindings: Environment to use when evaluating decls
    :param output_decls: Decls to evaluate
    :param standard_library: Standard library
    :return: New bindings object with only the output_decls
    """
    # all_bindings contains output + previous bindings so that the output can reference its own declarations
    # output_bindings only contains the output bindings themselves so that bindings from sections such as the input aren't included
    output_bindings: WDL.Env.Bindings[WDL.Value.Base] = WDL.Env.Bindings()
    for output_decl in output_decls:
        output_value = evaluate_decl(output_decl, all_bindings, standard_library)
        drop_if_missing_with_workdir = partial(drop_if_missing, work_dir=standard_library.execution_dir)
        output_value = map_over_typed_files_in_value(output_value, drop_if_missing_with_workdir)
        all_bindings = all_bindings.bind(output_decl.name, output_value)
        output_bindings = output_bindings.bind(output_decl.name, output_value)
    return output_bindings

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
            if is_url(uri):
                if uri.startswith(TOIL_URI_SCHEME):
                    # This is a Toil File ID we encoded; we have the size
                    # available.
                    file_id = unpack_toil_uri(uri)[0]
                    # Use the encoded size
                    total_size += file_id.size
                else:
                    # This is some other kind of remote file.
                    # We need to get its size from the URI.
                    item_size = AbstractJobStore.get_size(uri)
                    if item_size is None:
                        # User asked for the size and we can't figure it out efficiently, so bail out.
                        raise RuntimeError(f"Attempt to check the size of {uri} failed")
                    total_size += item_size
            else:
                # This is actually a file we can use locally.
                local_path = self.stdlib._devirtualize_filename(uri)
                total_size += os.path.getsize(local_path)

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

def is_url(filename: str, schemes: List[str] = ['http:', 'https:', 's3:', 'gs:', TOIL_URI_SCHEME, TOIL_NONEXISTENT_URI_SCHEME]) -> bool:
        """
        Decide if a filename is a known kind of URL
        """
        for scheme in schemes:
            if filename.startswith(scheme):
                return True
        return False

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
    def __init__(
        self,
        file_store: AbstractFileStore,
        task_path: str,
        execution_dir: Optional[str] = None,
        enforce_existence: bool = True,
        share_files_with: Optional["ToilWDLStdLibBase"] = None
    ) -> None:
        """
        Set up the standard library.

        :param task_path: Dotted WDL name of the part of the workflow this library is working for.
        :param execution_dir: Directory to use as the working directory for workflow code.
        :param enforce_existence: If true, then if a file is detected as
            nonexistent, raise an error. Else, let it pass through
        :param share_files_with: If set to an existing standard library
            instance, use the same file upload and download paths as it.
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

        # Save the task path to tag uploads
        self._task_path = task_path

        # Keep the file store around so we can access files.
        self._file_store = file_store

        self._execution_dir = execution_dir or gettempdir()

        self._enforce_existence = enforce_existence

        if share_files_with is None:
            # We get fresh file download/upload state

            # Map forward from virtualized files to absolute devirtualized ones.
            self._virtualized_to_devirtualized: Dict[str, str] = {}
            # Allow mapping back from absolute devirtualized files to virtualized
            # paths, to save re-uploads.
            self._devirtualized_to_virtualized: Dict[str, str] = {}
            # State we need for choosing good names for devirtualized files
            self._devirtualization_state: DirectoryNamingStateDict = {}
            # UUID to differentiate which node files are virtualized from
            self._parent_dir_to_ids: Dict[str, uuid.UUID] = dict()
        else:
            # Share file download/upload state
            self._virtualized_to_devirtualized = share_files_with._virtualized_to_devirtualized
            self._devirtualized_to_virtualized = share_files_with._devirtualized_to_virtualized
            self._devirtualization_state = share_files_with._devirtualization_state
            self._parent_dir_to_ids = share_files_with._parent_dir_to_ids

    @property
    def execution_dir(self) -> str:
        return self._execution_dir

    def get_local_paths(self) -> List[str]:
        """
        Get all the local paths of files devirtualized (or virtualized) through the stdlib.
        """

        return list(self._virtualized_to_devirtualized.values())

    @memoize
    def _devirtualize_filename(self, filename: str) -> str:
        """
        'devirtualize' filename passed to a read_* function: return a filename that can be open()ed
        on the local host.
        """

        result = self.devirtualize_to(
            filename,
            self._file_store.localTempDir,
            self._file_store,
            self._execution_dir,
            self._devirtualization_state,
            self._devirtualized_to_virtualized,
            self._virtualized_to_devirtualized,
            self._enforce_existence
        )
        return result

    @staticmethod
    def devirtualize_to(
        filename: str,
        dest_dir: str,
        file_source: Union[AbstractFileStore, Toil],
        execution_dir: Optional[str],
        state: DirectoryNamingStateDict,
        devirtualized_to_virtualized: Optional[Dict[str, str]] = None,
        virtualized_to_devirtualized: Optional[Dict[str, str]] = None,
        enforce_existence: bool = True
    ) -> str:
        """
        Download or export a WDL virtualized filename/URL to the given directory.

        The destination directory must already exist.

        Makes sure sibling files stay siblings and files with the same name
        don't clobber each other. Called from within this class for tasks, and
        statically at the end of the workflow for outputs.

        Returns the local path to the file. If it already had a local path
        elsewhere, it might not actually be put in dest_dir.

        The input filename could already be devirtualized. In this case, the filename
        should not be added to the cache

        :param state: State dict which must be shared among successive calls into a dest_dir.
        :param enforce_existence: Raise an error if the file is nonexistent. Else, let it pass through.
        """

        if not os.path.isdir(dest_dir):
            # os.mkdir fails saying the directory *being made* caused a
            # FileNotFoundError. So check the dest_dir before trying to make
            # directories under it.
            raise RuntimeError(f"Cannot devirtualize {filename} into nonexistent directory {dest_dir}")

        # TODO: Support people doing path operations (join, split, get parent directory) on the virtualized filenames.
        # TODO: For task inputs, we are supposed to make sure to put things in the same directory if they came from the same directory. See <https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#task-input-localization>
        if is_url(filename):
            if virtualized_to_devirtualized is not None and filename in virtualized_to_devirtualized:
                # The virtualized file is in the cache, so grab the already devirtualized result
                result = virtualized_to_devirtualized[filename]
                logger.debug("Found virtualized %s in cache with devirtualized path %s", filename, result)
                return result
            if filename.startswith(TOIL_URI_SCHEME):
                # This is a reference to the Toil filestore.
                # Deserialize the FileID
                file_id, task_path, parent_id, file_basename = unpack_toil_uri(filename)

                # Decide where it should be put.
                dir_path = choose_human_readable_directory(dest_dir, task_path, parent_id, state)
            elif filename.startswith(TOIL_NONEXISTENT_URI_SCHEME):
                if enforce_existence:
                    raise FileNotFoundError(f"File {filename[len(TOIL_NONEXISTENT_URI_SCHEME):]} was not available when virtualized!")
                else:
                    return filename
            else:
                # Parse the URL and extract the basename
                file_basename = os.path.basename(urlsplit(filename).path)
                # Get the URL to the directory this thing came from. Remember
                # URLs are interpreted relative to the directory the thing is
                # in, not relative to the thing.
                parent_url = urljoin(filename, ".")
                # Turn it into a string we can make a directory for
                dir_path = os.path.join(dest_dir, quote(parent_url, safe=''))

            if not os.path.exists(dir_path):
                # Make sure the chosen directory exists
                os.mkdir(dir_path)
            # And decide the file goes in it.
            dest_path = os.path.join(dir_path, file_basename)

            if filename.startswith(TOIL_URI_SCHEME):
                # Get a local path to the file
                if isinstance(file_source, AbstractFileStore):
                    # Read from the file store.
                    # File is not allowed to be modified by the task. See
                    # <https://github.com/openwdl/wdl/issues/495>.
                    # We try to get away with symlinks and hope the task
                    # container can mount the destination file.
                    result = file_source.readGlobalFile(file_id, dest_path, mutable=False, symlink=True)
                elif isinstance(file_source, Toil):
                    # Read from the Toil context
                    file_source.export_file(file_id, dest_path)
                    result = dest_path
            else:
                # Download to a local file with the right name and execute bit.
                # Open it exclusively
                with open(dest_path, 'xb') as dest_file:
                    # And save to it
                    size, executable = AbstractJobStore.read_from_url(filename, dest_file)
                    if executable:
                        # Set the execute bit in the file's permissions
                        os.chmod(dest_path, os.stat(dest_path).st_mode | stat.S_IXUSR)

                result = dest_path
            if devirtualized_to_virtualized is not None:
                # Store the back mapping
                devirtualized_to_virtualized[result] = filename
            if virtualized_to_devirtualized is not None:
                # And the other way
                virtualized_to_devirtualized[filename] = result
            logger.debug('Devirtualized %s as openable file %s', filename, result)
        else:
            # This is a local file
            # To support relative paths, join the execution dir and filename
            # if filename is already an abs path, join() will do nothing
            if execution_dir is not None:
                result = os.path.join(execution_dir, filename)
            else:
                result = filename
            logger.debug("Virtualized file %s is already a local path", filename)

        if not os.path.exists(result):
            # Catch if something made it through without going through the proper virtualization/devirtualization steps
            raise RuntimeError(f"Virtualized file {filename} looks like a local file but isn't!")

        return result

    @memoize
    def _virtualize_filename(self, filename: str) -> str:
        """
        from a local path in write_dir, 'virtualize' into the filename as it should present in a
        File value
        """

        if is_url(filename):
            # Already virtual
            logger.debug('Already virtual: %s', filename)
            return filename

        # Otherwise this is a local file and we want to fake it as a Toil file store file

        # Make it an absolute path
        if self._execution_dir is not None:
            # To support relative paths from execution directory, join the execution dir and filename
            # If filename is already an abs path, join() will not do anything
            abs_filename = os.path.join(self._execution_dir, filename)
        else:
            abs_filename = os.path.abspath(filename)

        if abs_filename in self._devirtualized_to_virtualized:
            # This is a previously devirtualized thing so we can just use the
            # virtual version we remembered instead of reuploading it.
            result = self._devirtualized_to_virtualized[abs_filename]
            logger.debug("Re-using virtualized WDL file %s for %s", result, filename)
            return result

        file_id = self._file_store.writeGlobalFile(abs_filename)

        file_dir = os.path.dirname(abs_filename)
        parent_id = self._parent_dir_to_ids.setdefault(file_dir, uuid.uuid4())
        result = pack_toil_uri(file_id, self._task_path, parent_id, os.path.basename(abs_filename))
        logger.debug('Virtualized %s as WDL file %s', filename, result)
        # Remember the upload in case we share a cache
        self._devirtualized_to_virtualized[abs_filename] = result
        # And remember the local path in case we want a redownload
        self._virtualized_to_devirtualized[result] = abs_filename
        return result

class ToilWDLStdLibTaskCommand(ToilWDLStdLibBase):
    """
    Standard library implementation to use inside a WDL task command evaluation.

    Expects all the filenames in variable bindings to be container-side paths;
    these are the "virtualized" filenames, while the "devirtualized" filenames
    are host-side paths.
    """

    def __init__(self, file_store: AbstractFileStore, task_path: str, container: TaskContainer, execution_dir: Optional[str] = None):
        """
        Set up the standard library for the task command section.
        """

        # TODO: Don't we want to make sure we don't actually use the file store?
        super().__init__(file_store, task_path, execution_dir=execution_dir)
        self.container = container

    @memoize
    def _devirtualize_filename(self, filename: str) -> str:
        """
        Go from a virtualized WDL-side filename to a local disk filename.

        Any WDL-side filenames which are paths will be paths in the container.
        """
        if is_url(filename):
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

    @memoize
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

    def __init__(
        self,
        file_store: AbstractFileStore,
        task_path: str,
        stdout_path: str,
        stderr_path: str,
        file_to_mountpoint: Dict[str, str],
        current_directory_override: Optional[str] = None,
        share_files_with: Optional[ToilWDLStdLibBase] = None
    ):
        """
        Set up the standard library for a task output section. Needs to know
        where standard output and error from the task have been stored, and
        what local paths to pretend are where for resolving symlinks.

        :param current_directory_override: If set, resolves relative paths and
            globs from there instead of from the real current directory.
        :param share_files_with: If set to an existing standard library
            instance, use the same file upload and download paths as it.
        """

        # Just set up as ToilWDLStdLibBase, but it will call into
        # WDL.StdLib.TaskOutputs next.
        super().__init__(
            file_store,
            task_path,
            execution_dir=current_directory_override,
            share_files_with=share_files_with
        )

        # Remember task output files
        self._stdout_path = stdout_path
        self._stderr_path = stderr_path

        # Remember that the WDL code has not referenced them yet.
        self._stdout_used = False
        self._stderr_used = False

        # Reverse and store the file mount dict
        self._mountpoint_to_file = {v: k for k, v in file_to_mountpoint.items()}

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
        self._stdout_used = True
        return WDL.Value.File(self._stdout_path)

    def stdout_used(self) -> bool:
        """
        Return True if the standard output was read by the WDL.
        """
        return self._stdout_used

    def _stderr(self) -> WDL.Value.File:
        """
        Get the standard error of the command that ran, as a WDL File, outside the container.
        """
        self._stderr_used = True
        return WDL.Value.File(self._stderr_path)

    def stderr_used(self) -> bool:
        """
        Return True if the standard error was read by the WDL.
        """
        return self._stderr_used

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
        work_dir = '.' if not self._execution_dir else self._execution_dir

        # TODO: get this to run in the right container if there is one
        # We would use compgen -G to resolve the glob but that doesn't output
        # files in the same (lexicographical) order as actually using a glob on
        # the command line.
        #
        # But we still want to support spaces in filenames so we can't actually
        # parse the result of `echo <glob>` like the spec shows.
        #
        # So we use the method of <https://unix.stackexchange.com/a/766527>
        # where dumping a glob with spaces onto the command line from an
        # unquoted variable, with IFS cleared, allows it to be globbed as a
        # single unit. Then we loop over the results and print them
        # newline-delimited.
        lines = subprocess.run(['bash', '-c', ''.join([
            'cd ',
            shlex.quote(work_dir),
            ' && (shopt -s nullglob; IFS=""; PATTERN=',
            shlex.quote(pattern_string),
            '; for RESULT in ${PATTERN} ; do echo "${RESULT}" ; done)'
        ])], stdout=subprocess.PIPE).stdout.decode('utf-8')

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

    @memoize
    def _devirtualize_filename(self, filename: str) -> str:
        """
        Go from a virtualized WDL-side filename to a local disk filename.

        Any WDL-side filenames which are relative will be relative to the
        current directory override, if set.
        """
        if not is_url(filename) and not filename.startswith('/'):
            # We are getting a bare relative path from the WDL side.
            # Find a real path to it relative to the current directory override.
            work_dir = '.' if not self._execution_dir else self._execution_dir
            filename = os.path.join(work_dir, filename)

        return super()._devirtualize_filename(filename)

    @memoize
    def _virtualize_filename(self, filename: str) -> str:
        """
        Go from a local disk filename to a virtualized WDL-side filename.

        Any relative paths will be relative to the current directory override,
        if set, to account for how they might not be *real* devirtualized
        filenames.
        """

        if not is_url(filename) and not filename.startswith('/'):
            # We are getting a bare relative path on the supposedly devirtualized side.
            # Find a real path to it relative to the current directory override.
            work_dir = '.' if not self._execution_dir else self._execution_dir
            filename = os.path.join(work_dir, filename)

        if filename in self._devirtualized_to_virtualized:
            result = self._devirtualized_to_virtualized[filename]
            logger.debug("Re-using virtualized filename %s for %s", result, filename)
            return result

        if os.path.islink(filename):
            # Recursively resolve symlinks
            here = filename
            # Notice if we have a symlink loop
            seen = {here}
            while os.path.islink(here):
                dest = os.readlink(here)
                if not dest.startswith('/'):
                    # Make it absolute
                    dest = os.path.join(os.path.dirname(here), dest)
                here = dest
                if here in self._mountpoint_to_file:
                    # This points to something mounted into the container, so use that path instead.
                    here = self._mountpoint_to_file[here]
                if here in self._devirtualized_to_virtualized:
                    # Check the virtualized filenames before following symlinks
                    # all the way back to workflow inputs.
                    result = self._devirtualized_to_virtualized[here]
                    logger.debug("Re-using virtualized filename %s for %s linked from %s", result, here, filename)
                    return result
                if here in seen:
                    raise RuntimeError(f"Symlink {filename} leads to symlink loop at {here}")
                seen.add(here)

            if os.path.exists(here):
                logger.debug("Handling symlink %s ultimately to %s", filename, here)
            else:
                logger.error("Handling broken symlink %s ultimately to %s", filename, here)
            filename = here

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
            logger.debug("Got value %s of type %s", value, value.type)
        except Exception:
            # If something goes wrong, dump.
            logger.exception("Expression evaluation failed for %s: %s", name, expression)
            log_bindings(logger.error, "Expression was evaluated in:", [environment])
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

def evaluate_call_inputs(context: Union[WDL.Error.SourceNode, WDL.Error.SourcePosition], expressions: Dict[str, WDL.Expr.Base], environment: WDLBindings, stdlib: WDL.StdLib.Base, inputs_dict: Optional[Dict[str, WDL.Type.Base]] = None) -> WDLBindings:
    """
    Evaluate a bunch of expressions with names, and make them into a fresh set of bindings. `inputs_dict` is a mapping of
    variable names to their expected type for the input decls in a task.
    """
    new_bindings: WDLBindings = WDL.Env.Bindings()
    for k, v in expressions.items():
        # Add each binding in turn
        # If the expected type is optional, then don't type check the lhs and rhs as miniwdl will return a StaticTypeMismatch error, so pass in None
        expected_type = None
        if not v.type.optional and inputs_dict is not None:
            # This is done to enable passing in a string into a task input of file type
            expected_type = inputs_dict.get(k, None)
        try:
            new_bindings = new_bindings.bind(k, evaluate_named_expression(context, k, expected_type, v, environment, stdlib))
        except FileNotFoundError as e:
            # MiniWDL's type coercion will raise this when trying to make a File out of Null.
            raise WDL.Error.EvalError(context, f"Cannot evaluate expression for {k} with value {v}")
    return new_bindings

def evaluate_defaultable_decl(node: WDL.Tree.Decl, environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDL.Value.Base:
    """
    If the name of the declaration is already defined in the environment, return its value. Otherwise, return the evaluated expression.
    """

    try:
        if ((node.name in environment and not isinstance(environment[node.name], WDL.Value.Null))
                or (isinstance(environment.get(node.name), WDL.Value.Null) and node.type.optional)):
            logger.debug('Name %s is already defined, not using default', node.name)
            if not isinstance(environment[node.name].type, type(node.type)):
                return environment[node.name].coerce(node.type)
            else:
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
        log_bindings(logger.error, "Statement was evaluated in:", [environment])
        raise

# TODO: make these stdlib methods???
def devirtualize_files(environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that are actually available to command line commands.
    The same virtual file always maps to the same devirtualized filename even with duplicates
    """
    return map_over_files_in_bindings(environment, stdlib._devirtualize_filename)

def virtualize_files(environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that are usable from other machines.
    """

    return map_over_files_in_bindings(environment, stdlib._virtualize_filename)

def add_paths(task_container: TaskContainer, host_paths: Iterable[str]) -> None:
    """
    Based off of WDL.runtime.task_container.add_paths from miniwdl
    Maps the host path to the container paths
    """
    # partition the files by host directory
    host_paths_by_dir: Dict[str, Set[str]] = {}
    for host_path in host_paths:
        host_path_strip = host_path.rstrip("/")
        if host_path not in task_container.input_path_map and host_path_strip not in task_container.input_path_map:
            if not os.path.exists(host_path_strip):
                raise WDL.Error.InputError("input path not found: " + host_path)
            host_paths_by_dir.setdefault(os.path.dirname(host_path_strip), set()).add(host_path)
    # for each such partition of files
    # - if there are no basename collisions under input subdirectory 0, then mount them there.
    # - otherwise, mount them in a fresh subdirectory
    subd = 0
    id_to_subd: Dict[str, str] = {}
    for paths in host_paths_by_dir.values():
        based = os.path.join(task_container.container_dir, "work/_miniwdl_inputs")
        for host_path in paths:
            parent_id = os.path.basename(os.path.dirname(host_path))
            if id_to_subd.get(parent_id, None) is None:
                id_to_subd[parent_id] = str(subd)
                subd += 1
            host_path_subd = id_to_subd[parent_id]
            container_path = os.path.join(based, host_path_subd, os.path.basename(host_path.rstrip("/")))
            if host_path.endswith("/"):
                container_path += "/"
            assert container_path not in task_container.input_path_map_rev, f"{container_path}, {task_container.input_path_map_rev}"
            task_container.input_path_map[host_path] = container_path
            task_container.input_path_map_rev[container_path] = host_path

def import_files(environment: WDLBindings, task_path: str, toil: Toil, path: Optional[List[str]] = None, skip_remote: bool = False) -> WDLBindings:
    """
    Make sure all File values embedded in the given bindings are imported,
    using the given Toil object.

    :param task_path: Dotted WDL name of the user-level code doing the
           importing (probably the workflow name).

    :param path: If set, try resolving input location relative to the URLs or
           directories in this list.

    :param skip_remote: If set, don't try to import files from remote
           locations. Leave them as URIs.
    """
    path_to_id: Dict[str, uuid.UUID] = {}
    @memoize
    def import_file_from_uri(uri: str) -> str:
        """
        Import a file from a URI and return a virtualized filename for it.
        """

        tried = []
        for candidate_uri in potential_absolute_uris(uri, path if path is not None else []):
            # Try each place it could be according to WDL finding logic.
            tried.append(candidate_uri)
            try:
                if skip_remote and is_url(candidate_uri):
                    # Use remote URIs in place. But we need to find the one that exists.
                    if not AbstractJobStore.url_exists(candidate_uri):
                        # Wasn't found there
                        continue

                    # Now we know this exists, so pass it through
                    return candidate_uri
                else:
                    # Actually import
                    # Try to import the file. Don't raise if we can't find it, just
                    # return None!
                    imported = toil.import_file(candidate_uri, check_existence=False)
                    if imported is None:
                        # Wasn't found there
                        continue
                    logger.info('Imported %s', candidate_uri)
            except UnimplementedURLException as e:
                # We can't find anything that can even support this URL scheme.
                # Report to the user, they are probably missing an extra.
                logger.critical('Error: ' + str(e))
                sys.exit(1)
            except HTTPError as e:
                # Something went wrong looking for it there.
                logger.warning("Checked URL %s but got HTTP status %s", candidate_uri, e.code)
                # Try the next location.
                continue
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
            if is_url(candidate_uri):
                # Might be a file URI or other URI.
                # We need to make sure file URIs and local paths that point to
                # the same place are treated the same.
                parsed = urlsplit(candidate_uri)
                if parsed.scheme == "file:":
                    # This is a local file URI. Convert to a path for source directory tracking.
                    parent_dir = os.path.dirname(unquote(parsed.path))
                else:
                    # This is some other URL. Get the URL to the parent directory and use that.
                    parent_dir = urljoin(candidate_uri, ".")
            else:
                # Must be a local path
                parent_dir = os.path.dirname(candidate_uri)

            # Pack a UUID of the parent directory
            dir_id = path_to_id.setdefault(parent_dir, uuid.uuid4())

            return pack_toil_uri(imported, task_path, dir_id, file_basename)

        # If we get here we tried all the candidates
        raise RuntimeError(f"Could not find {uri} at any of: {tried}")

    return map_over_files_in_bindings(environment, import_file_from_uri)


def drop_if_missing(value_type: WDL.Type.Base, filename: str, work_dir: str) -> Optional[str]:
    """
    Return None if a file doesn't exist, or its path if it does.

    filename represents a URI or file name belonging to a WDL value of type value_type. work_dir represents
    the current working directory of the job and is where all relative paths will be interpreted from
    """
    logger.debug("Consider file %s", filename)

    if is_url(filename):
        try:
            if (not filename.startswith(TOIL_NONEXISTENT_URI_SCHEME)
                    and (filename.startswith(TOIL_URI_SCHEME) or AbstractJobStore.url_exists(filename))):
                # We assume anything in the filestore actually exists.
                return filename
            else:
                logger.warning('File %s with type %s does not actually exist at its URI', filename, value_type)
                return None
        except HTTPError as e:
            # The error doesn't always include the URL in its message.
            logger.error("File %s could not be checked for existence due to HTTP error %d", filename, e.code)
            raise
    else:
        # Get the absolute path, not resolving symlinks
        effective_path = os.path.abspath(os.path.join(work_dir, filename))
        if os.path.islink(effective_path) or os.path.exists(effective_path):
            # This is a broken symlink or a working symlink or a file.
            return filename
        else:
            logger.warning('File %s with type %s does not actually exist at %s', filename, value_type, effective_path)
            return None

def drop_missing_files(environment: WDLBindings, current_directory_override: Optional[str] = None) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that exist, or are null.

    Files must not be virtualized.
    """

    # Determine where to evaluate relative paths relative to
    work_dir = '.' if not current_directory_override else current_directory_override

    drop_if_missing_with_workdir = partial(drop_if_missing, work_dir=work_dir)
    return map_over_typed_files_in_bindings(environment, drop_if_missing_with_workdir)

def get_file_paths_in_bindings(environment: WDLBindings) -> List[str]:
    """
    Get the paths of all files in the bindings. Doesn't guarantee that
    duplicates are removed.

    TODO: Duplicative with WDL.runtime.task._fspaths, except that is internal
    and supports Directory objects.
    """

    paths = []

    def append_to_paths(path: str) -> Optional[str]:
        # Append element and return the element. This is to avoid a logger warning inside map_over_typed_files_in_value()
        # But don't process nonexistent files
        if not path.startswith(TOIL_NONEXISTENT_URI_SCHEME):
            paths.append(path)
            return path
    map_over_files_in_bindings(environment, append_to_paths)
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
            logger.warning("File %s became Null", value)
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

    Responsible for post-processing returned bindings, to do things like add in
    null values for things not defined in a section. Post-processing operations
    can be added onto any job before it is saved, and will be applied as long
    as the job's run method calls postprocess().

    Also responsible for remembering the Toil WDL configuration keys and values.
    """

    def __init__(self, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
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

        # We need an ordered list of postprocessing steps to apply, because we
        # may have coalesced postprocessing steps deferred by several levels of
        # jobs returning other jobs' promised RVs.
        self._postprocessing_steps: List[Tuple[str, Union[str, Promised[WDLBindings]]]] = []

        self._wdl_options = wdl_options if wdl_options is not None else {}

        assert self._wdl_options.get("container") is not None

    # TODO: We're not allowed by MyPy to override a method and widen the return
    # type, so this has to be Any.
    def run(self, file_store: AbstractFileStore) -> Any:
        """
        Run a WDL-related job.

        Remember to decorate non-trivial overrides with :func:`report_wdl_errors`.
        """
        # Make sure that pickle is prepared to save our return values, which
        # might take a lot of recursive calls. TODO: This might be because
        # bindings are actually linked lists or something?
        sys.setrecursionlimit(10000)

    def then_underlay(self, underlay: Promised[WDLBindings]) -> None:
        """
        Apply an underlay of backup bindings to the result.
        """
        logger.debug("Underlay %s after %s", underlay, self)
        self._postprocessing_steps.append(("underlay", underlay))

    def then_remove(self, remove: Promised[WDLBindings]) -> None:
        """
        Remove the given bindings from the result.
        """
        logger.debug("Remove %s after %s", remove, self)
        self._postprocessing_steps.append(("remove", remove))

    def then_namespace(self, namespace: str) -> None:
        """
        Put the result bindings into a namespace.
        """
        logger.debug("Namespace %s after %s", namespace, self)
        self._postprocessing_steps.append(("namespace", namespace))

    def then_overlay(self, overlay: Promised[WDLBindings]) -> None:
        """
        Overlay the given bindings on top of the (possibly namespaced) result.
        """
        logger.debug("Overlay %s after %s", overlay, self)
        self._postprocessing_steps.append(("overlay", overlay))

    def postprocess(self, bindings: WDLBindings) -> WDLBindings:
        """
        Apply queued changes to bindings.

        Should be applied by subclasses' run() implementations to their return
        values.
        """

        for action, argument in self._postprocessing_steps:

            logger.debug("Apply postprocessing step: (%s, %s)", action, argument)

            # Interpret the mini language of postprocessing steps.
            # These are too small to justify being their own separate jobs.
            if action == "underlay":
                if not isinstance(argument, WDL.Env.Bindings):
                    raise RuntimeError("Wrong postprocessing argument type")
                # We want to apply values from the underlay if not set in the bindings
                bindings = combine_bindings([bindings, argument.subtract(bindings)])
            elif action == "remove":
                if not isinstance(argument, WDL.Env.Bindings):
                    raise RuntimeError("Wrong postprocessing argument type")
                # We need to take stuff out of scope
                bindings = bindings.subtract(argument)
            elif action == "namespace":
                if not isinstance(argument, str):
                    raise RuntimeError("Wrong postprocessing argument type")
                # We are supposed to put all our results in a namespace
                bindings = bindings.wrap_namespace(argument)
            elif action == "overlay":
                if not isinstance(argument, WDL.Env.Bindings):
                    raise RuntimeError("Wrong postprocessing argument type")
                # We want to apply values from the overlay over the bindings
                bindings = combine_bindings([bindings.subtract(argument), argument])
            else:
                raise RuntimeError(f"Unknown postprocessing action {action}")

        return bindings

    def defer_postprocessing(self, other: "WDLBaseJob") -> None:
        """
        Give our postprocessing steps to a different job.

        Use this when you are returning a promise for bindings, on the job that issues the promise.
        """

        other._postprocessing_steps += self._postprocessing_steps
        self._postprocessing_steps = []

        logger.debug("Assigned postprocessing steps from %s to %s", self, other)

class WDLTaskWrapperJob(WDLBaseJob):
    """
    Job that determines the resources needed to run a WDL job.

    Responsible for evaluating the input declarations for unspecified inputs,
    evaluating the runtime section, and scheduling or chaining to the real WDL
    job.

    All bindings are in terms of task-internal names.
    """

    def __init__(self, task: WDL.Tree.Task, prev_node_results: Sequence[Promised[WDLBindings]], task_id: List[str], namespace: str, task_path: str, **kwargs: Any) -> None:
        """
        Make a new job to determine resources and run a task.

        :param namespace: The namespace that the task's *contents* exist in.
               The caller has alredy added the task's own name.

        :param task_path: Like the namespace, but including subscript numbers
               for scatters.
        """
        super().__init__(unitName=task_path + ".inputs", displayName=namespace + ".inputs", local=True, **kwargs)

        logger.info("Preparing to run task code for %s as %s", task.name, namespace)

        self._task = task
        self._prev_node_results = prev_node_results
        self._task_id = task_id
        self._namespace = namespace
        self._task_path = task_path

    @report_wdl_errors("evaluate task code", exit=True)
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Evaluate inputs and runtime and schedule the task.
        """
        super().run(file_store)
        logger.info("Evaluating inputs and runtime for task %s (%s) called as %s", self._task.name, self._task_id, self._namespace)

        # Combine the bindings we get from previous jobs.
        # For a task we are only passed the inside-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        # UUID to use for virtualizing files
        standard_library = ToilWDLStdLibBase(file_store, self._task_path)
        with monkeypatch_coerce(standard_library):
            if self._task.inputs:
                logger.debug("Evaluating task code")
                for input_decl in self._task.inputs:
                    # Evaluate all the inputs that aren't pre-set
                    bindings = bindings.bind(input_decl.name, evaluate_defaultable_decl(input_decl, bindings, standard_library))
            for postinput_decl in self._task.postinputs:
                # Evaluate all the postinput decls.
                # We need these in order to evaluate the runtime.
                # TODO: What if they wanted resources from the runtime?
                bindings = bindings.bind(postinput_decl.name, evaluate_defaultable_decl(postinput_decl, bindings, standard_library))

            # Evaluate the runtime section
            runtime_bindings = evaluate_call_inputs(self._task, self._task.runtime, bindings, standard_library)

        # Fill these in with not-None if the workflow asks for each resource.
        runtime_memory: Optional[int] = None
        runtime_cores: Optional[float] = None
        runtime_disk: Optional[int] = None
        runtime_accelerators: Optional[List[AcceleratorRequirement]] = None

        if runtime_bindings.has_binding('cpu'):
            cpu_spec: int = runtime_bindings.resolve('cpu').value
            runtime_cores = float(cpu_spec)

        if runtime_bindings.has_binding('memory'):
            # Get the memory requirement and convert to bytes
            memory_spec: Union[int, str] = runtime_bindings.resolve('memory').value
            if isinstance(memory_spec, str):
                memory_spec = human2bytes(memory_spec)
            runtime_memory = memory_spec

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
            runtime_disk = int(total_bytes)


        if not runtime_bindings.has_binding("gpu") and self._task.effective_wdl_version in ('1.0', 'draft-2'):
            # For old WDL versions, guess whether the task wants GPUs if not specified.
            use_gpus = (runtime_bindings.has_binding('gpuCount') or
                        runtime_bindings.has_binding('gpuType') or
                        runtime_bindings.has_binding('nvidiaDriverVersion'))
        else:
            # The gpu field is the WDL 1.1 standard with a default value of false,
            # so in 1.1+ documents, this field will be the absolute
            # truth on whether to use GPUs or not.
            # Fields such as gpuType and gpuCount will control what GPUs are provided.
            use_gpus = cast(WDL.Value.Boolean, runtime_bindings.get('gpu', WDL.Value.Boolean(False))).value

        if use_gpus:
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
            runtime_accelerators = [accelerator_requirement]

        # Schedule to get resources. Pass along the bindings from evaluating all the inputs and decls, and the runtime, with files virtualized.
        run_job = WDLTaskJob(self._task, virtualize_files(bindings, standard_library), virtualize_files(runtime_bindings, standard_library), self._task_id, self._namespace, self._task_path, cores=runtime_cores or self.cores, memory=runtime_memory or self.memory, disk=runtime_disk or self.disk, accelerators=runtime_accelerators or self.accelerators, wdl_options=self._wdl_options)
        # Run that as a child
        self.addChild(run_job)

        # Give it our postprocessing steps
        self.defer_postprocessing(run_job)

        # And return its result.
        return run_job.rv()



class WDLTaskJob(WDLBaseJob):
    """
    Job that runs a WDL task.

    Responsible for re-evaluating input declarations for unspecified inputs,
    evaluating the runtime section, re-scheduling if resources are not
    available, running any command, and evaluating the outputs.

    All bindings are in terms of task-internal names.
    """

    def __init__(self, task: WDL.Tree.Task, task_internal_bindings: Promised[WDLBindings], runtime_bindings: Promised[WDLBindings], task_id: List[str], namespace: str, task_path: str, **kwargs: Any) -> None:
        """
        Make a new job to run a task.

        :param namespace: The namespace that the task's *contents* exist in.
               The caller has alredy added the task's own name.

        :param task_path: Like the namespace, but including subscript numbers
               for scatters.
        """

        # This job should not be local because it represents a real workflow task.
        # TODO: Instead of re-scheduling with more resources, add a local
        # "wrapper" job like CWL uses to determine the actual requirements.
        super().__init__(unitName=task_path + ".command", displayName=namespace + ".command", local=False, **kwargs)

        logger.info("Preparing to run task %s as %s", task.name, namespace)

        self._task = task
        self._task_internal_bindings = task_internal_bindings
        self._runtime_bindings = runtime_bindings
        self._task_id = task_id
        self._namespace = namespace
        self._task_path = task_path

    ###
    # Runtime code injection system
    ###

    # WDL runtime code injected in the container communicates back to the rest
    # of the runtime through files in this directory.
    INJECTED_MESSAGE_DIR = ".toil_wdl_runtime"

    def add_injections(self, command_string: str, task_container: TaskContainer) -> str:
        """
        Inject extra Bash code from the Toil WDL runtime into the command for the container.

        Currently doesn't implement the MiniWDL plugin system, but does add
        resource usage monitoring to Docker containers.
        """

        parts = []

        if isinstance(task_container, SwarmContainer):
            # We're running on Docker Swarm, so we need to monitor CPU usage
            # and so on from inside the container, since it won't be attributed
            # to Toil child processes in the leader's self-monitoring.
            # TODO: Mount this from a file Toil installs instead or something.
            script = textwrap.dedent("""\
                function _toil_resource_monitor () {
                    # Turn off error checking and echo in here
                    set +ex
                    MESSAGE_DIR="${1}"
                    mkdir -p "${MESSAGE_DIR}"

                    function sample_cpu_usec() {
                        if [[ -f  /sys/fs/cgroup/cpu.stat ]] ; then
                            awk '{ if ($1 == "usage_usec") {print $2} }' /sys/fs/cgroup/cpu.stat
                        elif [[ -f /sys/fs/cgroup/cpuacct/cpuacct.stat ]] ; then
                            echo $(( $(head -n 1 /sys/fs/cgroup/cpuacct/cpuacct.stat | cut -f2 -d' ') * 10000 ))
                        fi
                    }

                    function sample_memory_bytes() {
                        if [[ -f /sys/fs/cgroup/memory.stat ]] ; then
                            awk '{ if ($1 == "anon") { print $2 } }' /sys/fs/cgroup/memory.stat
                        elif [[ -f /sys/fs/cgroup/memory/memory.stat ]] ; then
                            awk '{ if ($1 == "total_rss") { print $2 } }' /sys/fs/cgroup/memory/memory.stat
                        fi
                    }

                    while true ; do
                        printf "CPU\\t" >> ${MESSAGE_DIR}/resources.tsv
                        sample_cpu_usec >> ${MESSAGE_DIR}/resources.tsv
                        printf "Memory\\t" >> ${MESSAGE_DIR}/resources.tsv
                        sample_memory_bytes >> ${MESSAGE_DIR}/resources.tsv
                        sleep 1
                    done
                }
                """)
            parts.append(script)
            # Launch in a subshell so that it doesn't interfere with Bash "wait" in the main shell
            parts.append(f"(_toil_resource_monitor {self.INJECTED_MESSAGE_DIR} &)")

        if isinstance(task_container, SwarmContainer) and platform.system() == "Darwin":
            # With gRPC FUSE file sharing, files immediately downloaded before
            # being mounted may appear as size 0 in the container due to a race
            # condition. Check for this and produce an approperiate error.

            script = textwrap.dedent("""\
                function _toil_check_size () {
                    TARGET_FILE="${1}"
                    GOT_SIZE="$(stat -c %s "${TARGET_FILE}")"
                    EXPECTED_SIZE="${2}"
                    if [[ "${GOT_SIZE}" != "${EXPECTED_SIZE}" ]] ; then
                        echo >&2 "Toil Error:"
                        echo >&2 "File size visible in container for ${TARGET_FILE} is size ${GOT_SIZE} but should be size ${EXPECTED_SIZE}"
                        echo >&2 "Are you using gRPC FUSE file sharing in Docker Desktop?"
                        echo >&2 "It doesn't work: see <https://github.com/DataBiosphere/toil/issues/4542>."
                        exit 1
                    fi
                }
            """)
            parts.append(script)
            for host_path, job_path in task_container.input_path_map.items():
                expected_size = os.path.getsize(host_path)
                if expected_size != 0:
                    parts.append(f"_toil_check_size \"{job_path}\" {expected_size}")

        parts.append(command_string)

        return "\n".join(parts)

    def handle_injection_messages(self, outputs_library: ToilWDLStdLibTaskOutputs) -> None:
        """
        Handle any data received from injected runtime code in the container.
        """

        message_files = outputs_library._glob(WDL.Value.String(os.path.join(self.INJECTED_MESSAGE_DIR, "*")))
        logger.debug("Handling message files: %s", message_files)
        for message_file in message_files.value:
            self.handle_message_file(message_file.value)

    def handle_message_file(self, file_path: str) -> None:
        """
        Handle a message file received from in-container injected code.

        Takes the host-side path of the file.
        """
        if os.path.basename(file_path) == "resources.tsv":
            # This is a TSV of resource usage info.
            first_cpu_usec: Optional[int] = None
            last_cpu_usec: Optional[int] = None
            max_memory_bytes: Optional[int] = None

            for line in open(file_path):
                if not line.endswith("\n"):
                    # Skip partial lines
                    continue
                # For each full line we got
                parts = line.strip().split("\t")
                if len(parts) != 2:
                    # Skip odd-shaped lines
                    continue
                if parts[0] == "CPU":
                    # Parse CPU usage
                    cpu_usec = int(parts[1])
                    # Update summary stats
                    if first_cpu_usec is None:
                        first_cpu_usec = cpu_usec
                    last_cpu_usec = cpu_usec
                elif parts[0] == "Memory":
                    # Parse memory usage
                    memory_bytes = int(parts[1])
                    # Update summary stats
                    if max_memory_bytes is None or max_memory_bytes < memory_bytes:
                        max_memory_bytes = memory_bytes

            if max_memory_bytes is not None:
                logger.info("Container used at about %s bytes of memory at peak", max_memory_bytes)
                # Treat it as if used by a child process
                ResourceMonitor.record_extra_memory(max_memory_bytes // 1024)
            if last_cpu_usec is not None:
                assert(first_cpu_usec is not None)
                cpu_seconds = (last_cpu_usec - first_cpu_usec) / 1000000
                logger.info("Container used about %s seconds of CPU time", cpu_seconds)
                # Treat it as if used by a child process
                ResourceMonitor.record_extra_cpu(cpu_seconds)

    ###
    # Helper functions to work out what containers runtime we can use
    ###

    def can_fake_root(self) -> bool:
        """
        Determine if --fakeroot is likely to work for Singularity.
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

    def can_mount_proc(self) -> bool:
        """
        Determine if --containall will work for Singularity. On Kubernetes, this will result in operation not permitted
        See: https://github.com/apptainer/singularity/issues/5857

        So if Kubernetes is detected, return False
        :return: bool
        """
        return "KUBERNETES_SERVICE_HOST" not in os.environ

    @report_wdl_errors("run task command", exit=True)
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually run the task.
        """
        super().run(file_store)
        logger.info("Running task command for %s (%s) called as %s", self._task.name, self._task_id, self._namespace)

        # Set up the WDL standard library
        # UUID to use for virtualizing files
        # We process nonexistent files in WDLTaskWrapperJob as those must be run locally, so don't try to devirtualize them
        standard_library = ToilWDLStdLibBase(file_store, self._task_path, enforce_existence=False)

        # Get the bindings from after the input section
        bindings = unwrap(self._task_internal_bindings)
        # And the bindings from evaluating the runtime section
        runtime_bindings = unwrap(self._runtime_bindings)

        # We have all the resources we need, so run the task

        if shutil.which('singularity') and self._wdl_options.get("container") in ["singularity", "auto"]:
            # Prepare to use Singularity. We will need plenty of space to
            # download images.
            # Default the Singularity and MiniWDL cache directories. This sets the cache to the same place as
            # Singularity/MiniWDL's default cache directory
            # With launch-cluster, the singularity and miniwdl cache is set to /var/lib/toil in abstractProvisioner.py
            # A current limitation with the singularity/miniwdl cache is it cannot check for image updates if the
            # filename is the same
            singularity_cache = os.path.join(os.path.expanduser("~"), ".singularity")
            miniwdl_cache = os.path.join(os.path.expanduser("~"), ".cache/miniwdl")

            # Cache Singularity's layers somewhere known to have space
            os.environ['SINGULARITY_CACHEDIR'] = os.environ.get("SINGULARITY_CACHEDIR", singularity_cache)

            # Make sure it exists.
            os.makedirs(os.environ['SINGULARITY_CACHEDIR'], exist_ok=True)

            # Cache Singularity images for the workflow on this machine.
            # Since MiniWDL does only within-process synchronization for pulls,
            # we also will need to pre-pull one image into here at a time.
            os.environ['MINIWDL__SINGULARITY__IMAGE_CACHE'] = os.environ.get("MINIWDL__SINGULARITY__IMAGE_CACHE", miniwdl_cache)

            # Make sure it exists.
            os.makedirs(os.environ['MINIWDL__SINGULARITY__IMAGE_CACHE'], exist_ok=True)

            # Run containers with Singularity
            TaskContainerImplementation: Type[TaskContainer]  = SingularityContainer
        elif self._wdl_options.get("container") in ["docker", "auto"]:
            # Run containers with Docker
            # TODO: Poll if it is available and don't just try and fail.
            TaskContainerImplementation = SwarmContainer
            if runtime_bindings.has_binding('gpuType') or runtime_bindings.has_binding('gpuCount') or runtime_bindings.has_binding('nvidiaDriverVersion'):
                # Complain to the user that this is unlikely to work.
                logger.warning("Running job that might need accelerators with Docker. "
                               "Accelerator and GPU support "
                               "is not yet implemented in the MiniWDL Docker "
                               "containerization implementation.")
        else:
            raise RuntimeError(f"Could not find a working container engine to use; told to use {self._wdl_options.get('container')}")

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
            # When the command string references a File, we need to get a path
            # to the file on a local disk, which the commnad will be able to
            # actually use, accounting for e.g. containers.
            #
            # TODO: Figure out whan the command template actually uses File
            # values and lazily download them.
            #
            # For now we just grab all the File values in the inside-the-task
            # environment, since any of them *might* be used.
            #
            # Some also might be expected to be adjacent to files that are
            # used, like a BAI that doesn't get referenced in a command line
            # but must be next to its BAM.
            #
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

                    # If on Kubernetes and proc cannot be mounted, get rid of --containall
                    if '--containall' in command_line and not self.can_mount_proc():
                        command_line.remove('--containall')

                    extra_flags: Set[str] = set()
                    accelerators_needed: Optional[List[AcceleratorRequirement]] = self.accelerators
                    local_accelerators = get_individual_local_accelerators()
                    if accelerators_needed is not None:
                        for accelerator in accelerators_needed:
                            # This logic will not work if a workflow needs to specify multiple GPUs of different types
                            # Right now this assumes all GPUs on the node are the same; we only look at the first available GPU
                            # and assume homogeneity
                            # This shouldn't cause issues unless a user has a very odd machine setup, which should be rare
                            if accelerator['kind'] == 'gpu':
                                # Grab detected GPUs
                                local_gpus: List[Optional[str]] = [accel['brand'] for accel in local_accelerators if accel['kind'] == 'gpu'] or [None]
                                # Tell singularity the GPU type
                                gpu_brand = accelerator.get('brand') or local_gpus[0]
                                if gpu_brand == 'nvidia':
                                    # Tell Singularity to expose nvidia GPUs
                                    extra_flags.add('--nv')
                                elif gpu_brand == 'amd':
                                    # Tell Singularity to expose ROCm GPUs
                                    extra_flags.add('--rocm')
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
            task_container.process_runtime(miniwdl_logger, {binding.name: binding.value for binding in devirtualize_files(runtime_bindings, standard_library)})

            # Tell the container to take up all these files. It will assign
            # them all new paths in task_container.input_path_map which we can
            # read. We also get a task_container.host_path() to go the other way.
            add_paths(task_container, get_file_paths_in_bindings(bindings))
            # This maps from oustide container to inside container
            logger.debug("Using container path map: %s", task_container.input_path_map)

            # Replace everything with in-container paths for the command.
            # TODO: MiniWDL deals with directory paths specially here.
            def get_path_in_container(path: str) -> Optional[str]:
                if path.startswith(TOIL_NONEXISTENT_URI_SCHEME):
                    return None
                return task_container.input_path_map[path]
            contained_bindings = map_over_files_in_bindings(bindings, get_path_in_container)

            # Make a new standard library for evaluating the command specifically, which only deals with in-container paths and out-of-container paths.
            command_library = ToilWDLStdLibTaskCommand(file_store, self._task_path, task_container, workdir_in_container)

            # Work out the command string, and unwrap it
            command_string: str = evaluate_named_expression(self._task, "command", WDL.Type.String(), remove_common_leading_whitespace(self._task.command), contained_bindings, command_library).coerce(WDL.Type.String()).value

            # Do any command injection we might need to do
            command_string = self.add_injections(command_string, task_container)

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

            # Log that we are about to run the command in the container
            logger.info('Executing command in %s: %s', task_container, command_string)

            # Now our inputs are all downloaded. Let debugging break in (after command is logged).
            # But we need to hint which host paths are meant to be which container paths
            host_and_job_paths: List[Tuple[str, str]] = [(k, v) for k, v in task_container.input_path_map.items()]
            self.files_downloaded_hook(host_and_job_paths)

            # TODO: Really we might want to set up a fake container working directory, to actually help the user.

            try:
                task_container.run(miniwdl_logger, command_string)
            except Exception:
                if os.path.exists(host_stderr_txt):
                    size = os.path.getsize(host_stderr_txt)
                    logger.error('Failed task left standard error at %s of %d bytes', host_stderr_txt, size)
                    if size > 0:
                        # Send the whole error stream.
                        file_store.log_user_stream(self._task_path + '.stderr', open(host_stderr_txt, 'rb'))
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug("MiniWDL already logged standard error")
                        else:
                            # At debug level, MiniWDL itself logs command error lines.
                            # But otherwise we just dump into StatsAndLogging;
                            # we also want the messages in the job log that
                            # gets printed at the end of the workflow. So log
                            # the error log ourselves.
                            logger.error("====TASK ERROR LOG====")
                            for line in open(host_stderr_txt, 'r', errors="replace"):
                                logger.error("> %s", line.rstrip('\n'))
                            logger.error("====TASK ERROR LOG====")

                if os.path.exists(host_stdout_txt):
                    size = os.path.getsize(host_stdout_txt)
                    logger.info('Failed task left standard output at %s of %d bytes', host_stdout_txt, size)
                    if size > 0:
                        # Save the whole output stream.
                        # TODO: We can't tell if this was supposed to be
                        # captured. It might really be huge binary data.
                        file_store.log_user_stream(self._task_path + '.stdout', open(host_stdout_txt, 'rb'))

                # Keep crashing
                raise
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
        outputs_library = ToilWDLStdLibTaskOutputs(
            file_store,
            self._task_path,
            host_stdout_txt,
            host_stderr_txt,
            task_container.input_path_map,
            current_directory_override=workdir_in_container,
            share_files_with=standard_library
        )
        with monkeypatch_coerce(outputs_library):
            output_bindings = evaluate_output_decls(self._task.outputs, bindings, outputs_library)

        # Now we know if the standard output and error were sent somewhere by
        # the workflow. If not, we should report them to the leader.

        if not outputs_library.stderr_used() and os.path.exists(host_stderr_txt):
            size = os.path.getsize(host_stderr_txt)
            logger.info('Unused standard error at %s of %d bytes', host_stderr_txt, size)
            if size > 0:
                # Save the whole error stream because the workflow didn't capture it.
                file_store.log_user_stream(self._task_path + '.stderr', open(host_stderr_txt, 'rb'))

        if not outputs_library.stdout_used() and os.path.exists(host_stdout_txt):
            size = os.path.getsize(host_stdout_txt)
            logger.info('Unused standard output at %s of %d bytes', host_stdout_txt, size)
            if size > 0:
                # Save the whole output stream because the workflow didn't capture it.
                file_store.log_user_stream(self._task_path + '.stdout', open(host_stdout_txt, 'rb'))

        # Collect output messages from any code Toil injected into the task.
        self.handle_injection_messages(outputs_library)

        # Drop any files from the output which don't actually exist
        output_bindings = drop_missing_files(output_bindings, current_directory_override=workdir_in_container)
        for decl in self._task.outputs:
            if not decl.type.optional and output_bindings[decl.name].value is None:
                # todo: make recursive
                # We have an unacceptable null value. This can happen if a file
                # is missing but not optional. Don't let it out to annoy the
                # next task.
                raise WDL.Error.EvalError(decl, f"non-optional value {decl.name} = {decl.expr} is missing")

        # Upload any files in the outputs if not uploaded already. Accounts for how relative paths may still need to be container-relative.
        output_bindings = virtualize_files(output_bindings, outputs_library)

        # Do postprocessing steps to e.g. apply namespaces.
        output_bindings = self.postprocess(output_bindings)

        return output_bindings

class WDLWorkflowNodeJob(WDLBaseJob):
    """
    Job that evaluates a WDL workflow node.
    """

    def __init__(self, node: WDL.Tree.WorkflowNode, prev_node_results: Sequence[Promised[WDLBindings]], namespace: str, task_path: str, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """
        Make a new job to run a workflow node to completion.
        """
        super().__init__(unitName=node.workflow_node_id, displayName=node.workflow_node_id, wdl_options=wdl_options or {}, **kwargs)

        self._node = node
        self._prev_node_results = prev_node_results
        self._namespace = namespace
        self._task_path = task_path

        if isinstance(self._node, WDL.Tree.Call):
            logger.debug("Preparing job for call node %s", self._node.workflow_node_id)

    @report_wdl_errors("run workflow node")
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually execute the workflow node.
        """
        super().run(file_store)
        logger.info("Running node %s", self._node.workflow_node_id)

        # Combine the bindings we get from previous jobs
        incoming_bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store, self._task_path, execution_dir=self._wdl_options.get("execution_dir"))
        with monkeypatch_coerce(standard_library):
            if isinstance(self._node, WDL.Tree.Decl):
                # This is a variable assignment
                logger.info('Setting %s to %s', self._node.name, self._node.expr)
                value = evaluate_decl(self._node, incoming_bindings, standard_library)
                return self.postprocess(incoming_bindings.bind(self._node.name, value))
            elif isinstance(self._node, WDL.Tree.Call):
                # This is a call of a task or workflow

                # Fetch all the inputs we are passing and bind them.
                # The call is only allowed to use these.
                logger.debug("Evaluating step inputs")
                if self._node.callee is None:
                    # This should never be None, but mypy gets unhappy and this is better than an assert
                    inputs_mapping = None
                else:
                    inputs_mapping = {e.name: e.type for e in self._node.callee.inputs or []}
                input_bindings = evaluate_call_inputs(self._node, self._node.inputs, incoming_bindings, standard_library, inputs_mapping)

                # Bindings may also be added in from the enclosing workflow inputs
                # TODO: this is letting us also inject them from the workflow body.
                # TODO: Can this result in picking up non-namespaced values that
                # aren't meant to be inputs, by not changing their names?
                passed_down_bindings = incoming_bindings.enter_namespace(self._node.name)

                if isinstance(self._node.callee, WDL.Tree.Workflow):
                    # This is a call of a workflow
                    subjob: WDLBaseJob = WDLWorkflowJob(self._node.callee, [input_bindings, passed_down_bindings], self._node.callee_id, f'{self._namespace}.{self._node.name}', f'{self._task_path}.{self._node.name}', wdl_options=self._wdl_options)
                    self.addChild(subjob)
                elif isinstance(self._node.callee, WDL.Tree.Task):
                    # This is a call of a task
                    subjob = WDLTaskWrapperJob(self._node.callee, [input_bindings, passed_down_bindings], self._node.callee_id, f'{self._namespace}.{self._node.name}', f'{self._task_path}.{self._node.name}', wdl_options=self._wdl_options)
                    self.addChild(subjob)
                else:
                    raise WDL.Error.InvalidType(self._node, "Cannot call a " + str(type(self._node.callee)))

                # We need to agregate outputs namespaced with our node name, and existing bindings
                subjob.then_namespace(self._node.name)
                subjob.then_overlay(incoming_bindings)
                self.defer_postprocessing(subjob)
                return subjob.rv()
            elif isinstance(self._node, WDL.Tree.Scatter):
                subjob = WDLScatterJob(self._node, [incoming_bindings], self._namespace, self._task_path, wdl_options=self._wdl_options)
                self.addChild(subjob)
                # Scatters don't really make a namespace, just kind of a scope?
                # TODO: Let stuff leave scope!
                self.defer_postprocessing(subjob)
                return subjob.rv()
            elif isinstance(self._node, WDL.Tree.Conditional):
                subjob = WDLConditionalJob(self._node, [incoming_bindings], self._namespace, self._task_path, wdl_options=self._wdl_options)
                self.addChild(subjob)
                # Conditionals don't really make a namespace, just kind of a scope?
                # TODO: Let stuff leave scope!
                self.defer_postprocessing(subjob)
                return subjob.rv()
            else:
                raise WDL.Error.InvalidType(self._node, "Unimplemented WorkflowNode: " + str(type(self._node)))

class WDLWorkflowNodeListJob(WDLBaseJob):
    """
    Job that evaluates a list of WDL workflow nodes, which are in the same
    scope and in a topological dependency order, and which do not call out to any other
    workflows or tasks or sections.
    """

    def __init__(self, nodes: List[WDL.Tree.WorkflowNode], prev_node_results: Sequence[Promised[WDLBindings]], namespace: str, task_path: str, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """
        Make a new job to run a list of workflow nodes to completion.
        """
        super().__init__(unitName=nodes[0].workflow_node_id + '+', displayName=nodes[0].workflow_node_id + '+', wdl_options=wdl_options, **kwargs)

        self._nodes = nodes
        self._prev_node_results = prev_node_results
        self._namespace = namespace
        self._task_path = task_path

        for n in self._nodes:
            if isinstance(n, (WDL.Tree.Call, WDL.Tree.Scatter, WDL.Tree.Conditional)):
                raise RuntimeError("Node cannot be evaluated with other nodes: " + str(n))

    @report_wdl_errors("run workflow node list")
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually execute the workflow nodes.
        """
        super().run(file_store)

        # Combine the bindings we get from previous jobs
        current_bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store, self._task_path, execution_dir=self._wdl_options.get("execution_dir"))

        with monkeypatch_coerce(standard_library):
            for node in self._nodes:
                if isinstance(node, WDL.Tree.Decl):
                    # This is a variable assignment
                    logger.info('Setting %s to %s', node.name, node.expr)
                    value = evaluate_decl(node, current_bindings, standard_library)
                    current_bindings = current_bindings.bind(node.name, value)
                else:
                    raise WDL.Error.InvalidType(node, "Unimplemented WorkflowNode: " + str(type(node)))

        return self.postprocess(current_bindings)


class WDLCombineBindingsJob(WDLBaseJob):
    """
    Job that collects the results from WDL workflow nodes and combines their
    environment changes.
    """

    def __init__(self, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        """
        Make a new job to combine the results of previous jobs.

        If underlay is set, those bindings will be injected to be overridden by other bindings.

        If remove is set, bindings there will be subtracted out of the result.
        """
        super().__init__(**kwargs)

        self._prev_node_results = prev_node_results

    @report_wdl_errors("combine bindings")
    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Aggregate incoming results.
        """
        super().run(file_store)
        combined = combine_bindings(unwrap_all(self._prev_node_results))
        # Make sure to run the universal postprocessing steps
        return self.postprocess(combined)

class WDLWorkflowGraph:
    """
    Represents a graph of WDL WorkflowNodes.

    Operates at a certain level of instantiation (i.e. sub-sections are
    represented by single nodes).

    Assumes all relevant nodes are provided; dependencies outside the provided
    nodes are assumed to be satisfied already.
    """

    def __init__(self, nodes: Sequence[WDL.Tree.WorkflowNode]) -> None:
        """
        Make a graph for analyzing a set of workflow nodes.
        """

        # For Gather nodes, the Toil interpreter handles them as part of their
        # associated section. So make a map from gather ID to the section node
        # ID.
        self._gather_to_section: Dict[str, str] = {}
        for node in nodes:
            if isinstance(node, WDL.Tree.WorkflowSection):
                for gather_node in node.gathers.values():
                    self._gather_to_section[gather_node.workflow_node_id] = node.workflow_node_id

        # Store all the nodes by ID, except the gathers which we elide.
        self._nodes: Dict[str, WDL.Tree.WorkflowNode] = {node.workflow_node_id: node for node in nodes if not isinstance(node, WDL.Tree.Gather)}

    def real_id(self, node_id: str) -> str:
        """
        Map multiple IDs for what we consider the same node to one ID.

        This elides/resolves gathers.
        """
        return self._gather_to_section.get(node_id, node_id)

    def is_decl(self, node_id: str) -> bool:
        """
        Return True if a node represents a WDL declaration, and false
        otherwise.
        """
        return isinstance(self.get(node_id), WDL.Tree.Decl)

    def get(self, node_id: str) -> WDL.Tree.WorkflowNode:
        """
        Get a node by ID.
        """
        return self._nodes[self.real_id(node_id)]

    def get_dependencies(self, node_id: str) -> Set[str]:
        """
        Get all the nodes that a node depends on, recursively (into the node if
        it has a body) but not transitively.

        Produces dependencies after resolving gathers and internal-to-section
        dependencies, on nodes that are also in this graph.
        """

        # We need to make sure to bubble up dependencies from inside sections.
        # A conditional might only appear to depend on the variables in the
        # conditional expression, but its body can depend on other stuff, and
        # we need to make sure that that stuff has finished and updated the
        # environment before the conditional body runs. TODO: This is because
        # Toil can't go and get and add successors to the relevant jobs later,
        # while MiniWDL's engine apparently can. This ends up reducing
        # parallelism more than would strictly be necessary; nothing in the
        # conditional can start until the dependencies of everything in the
        # conditional are ready.

        dependencies = set()

        node = self.get(node_id)
        for dependency in recursive_dependencies(node):
            real_dependency = self.real_id(dependency)
            if real_dependency in self._nodes:
                dependencies.add(real_dependency)

        return dependencies

    def get_transitive_dependencies(self, node_id: str) -> Set[str]:
        """
        Get all the nodes that a node depends on, transitively.
        """

        dependencies: Set[str] = set()
        visited: Set[str] = set()
        queue = [node_id]

        while len(queue) > 0:
            # Grab the enxt thing off the queue
            here = queue[-1]
            queue.pop()
            if here in visited:
                # Skip if we got it already
                continue
            # Mark it got
            visited.add(here)
            # Get all its dependencies
            here_deps = self.get_dependencies(here)
            dependencies |= here_deps
            for dep in here_deps:
                if dep not in visited:
                    # And queue all the ones we haven't visited.
                    queue.append(dep)

        return dependencies

    def topological_order(self) -> List[str]:
        """
        Get a topological order of the nodes, based on their dependencies.
        """

        sorter : TopologicalSorter[str] = TopologicalSorter()
        for node_id in self._nodes.keys():
            # Add all the edges
            sorter.add(node_id, *self.get_dependencies(node_id))
        return list(sorter.static_order())

    def leaves(self) -> List[str]:
        """
        Get all the workflow node IDs that have no dependents in the graph.
        """

        leaves = set(self._nodes.keys())
        for node_id in self._nodes.keys():
            for dependency in self.get_dependencies(node_id):
                if dependency in leaves:
                    # Mark everything depended on as not a leaf
                    leaves.remove(dependency)
        return list(leaves)


class WDLSectionJob(WDLBaseJob):
    """
    Job that can create more graph for a section of the workflow.
    """

    def __init__(self, namespace: str, task_path: str, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """
        Make a WDLSectionJob where the interior runs in the given namespace,
        starting with the root workflow.
        """
        super().__init__(wdl_options=wdl_options, **kwargs)
        self._namespace = namespace
        self._task_path = task_path

    @staticmethod
    def coalesce_nodes(order: List[str], section_graph: WDLWorkflowGraph) -> List[List[str]]:
        """
        Given a topological order of WDL workflow node IDs, produce a list of
        lists of IDs, still in topological order, where each list of IDs can be
        run under a single Toil job.
        """

        # All the buckets of merged nodes
        to_return: List[List[str]] = []
        # The nodes we are currently merging, in topological order
        current_bucket: List[str] = []
        # All the non-decl transitive dependencies of nodes in the bucket
        current_bucket_dependencies: Set[str] = set()

        for next_id in order:
            # Consider adding each node to the bucket
            # Get all the dependencies on things that aren't decls.
            next_dependencies = {dep for dep in section_graph.get_transitive_dependencies(next_id) if not section_graph.is_decl(dep)}
            if len(current_bucket) == 0:
                # This is the first thing for the bucket
                current_bucket.append(next_id)
                current_bucket_dependencies |= next_dependencies
            else:
                # Get a node already in the bucket
                current_id = current_bucket[0]

                if not section_graph.is_decl(current_id) or not section_graph.is_decl(next_id):
                    # We can only combine decls with decls, so we can't go in
                    # the bucket.

                    # Finish the bucket.
                    to_return.append(current_bucket)
                    # Start a new one with this next node
                    current_bucket = [next_id]
                    current_bucket_dependencies = next_dependencies
                else:
                    # We have a decl in the bucket and a decl we could maybe
                    # add. We know they are part of the same section, so we
                    # aren't jumping in and out of conditionals or scatters.

                    # We are going in a topological order, so we know the
                    # bucket can't depend on the new node.

                    if next_dependencies == current_bucket_dependencies:
                        # We can add this node without adding more dependencies on non-decls on either side.
                        # Nothing in the bucket can be in the dependency set because the bucket is only decls.
                        # Put it in
                        current_bucket.append(next_id)
                        # TODO: With this condition, this is redundant.
                        current_bucket_dependencies |= next_dependencies
                    else:
                        # Finish the bucket.
                        to_return.append(current_bucket)
                        # Start a new one with this next node
                        current_bucket = [next_id]
                        current_bucket_dependencies = next_dependencies

        if len(current_bucket) > 0:
            # Now finish the last bucket
            to_return.append(current_bucket)

        return to_return



    def create_subgraph(self, nodes: Sequence[WDL.Tree.WorkflowNode], gather_nodes: Sequence[WDL.Tree.Gather], environment: WDLBindings, local_environment: Optional[WDLBindings] = None, subscript: Optional[int] = None) -> WDLBaseJob:
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
        :param subscript: If the subgraph is being evaluated multiple times,
               this should be a disambiguating integer for logging.
        """

        # Work out what to call what we are working on
        task_path = self._task_path
        if subscript is not None:
            # We need to include a scatter loop number.
            task_path += f'.{subscript}'

        if local_environment is not None:
            # Bring local environment into scope
            environment = combine_bindings([environment, local_environment])

        # Make a graph of all the nodes at this level
        section_graph = WDLWorkflowGraph(nodes)

        # To make Toil jobs, we need all the jobs they depend on made so we can
        # call .rv(). So we need to solve the workflow DAG ourselves to set it up
        # properly.

        # When a WDL node depends on another, we need to be able to find the Toil job we need an rv from.
        wdl_id_to_toil_job: Dict[str, WDLBaseJob] = {}
        # We need the set of Toil jobs not depended on so we can wire them up to the sink.
        # This maps from Toil job store ID to job.
        toil_leaves: Dict[Union[str, TemporaryID], WDLBaseJob] = {}

        def get_job_set_any(wdl_ids: Set[str]) -> List[WDLBaseJob]:
            """
            Get the distinct Toil jobs executing any of the given WDL nodes.
            """
            job_ids = set()
            jobs = []
            for job in (wdl_id_to_toil_job[wdl_id] for wdl_id in wdl_ids):
                # For each job that is registered under any of these WDL IDs
                if job.jobStoreID not in job_ids:
                    # If we haven't taken it already, take it
                    job_ids.add(job.jobStoreID)
                    jobs.append(job)
            return jobs

        creation_order = section_graph.topological_order()
        logger.debug('Creation order: %s', creation_order)

        # Now we want to organize the linear list of nodes into collections of nodes that can be in the same Toil job.
        creation_jobs = self.coalesce_nodes(creation_order, section_graph)
        logger.debug('Creation jobs: %s', creation_jobs)

        for node_ids in creation_jobs:
            logger.debug('Make Toil job for %s', node_ids)
            # Collect the return values from previous jobs. Some nodes may have been inputs, without jobs.
            # Don't inlude stuff in the current batch.
            prev_node_ids = {prev_node_id for node_id in node_ids for prev_node_id in section_graph.get_dependencies(node_id) if prev_node_id not in node_ids}


            # Get the Toil jobs we depend on
            prev_jobs = get_job_set_any(prev_node_ids)
            for prev_job in prev_jobs:
                if prev_job.jobStoreID in toil_leaves:
                    # Mark them all as depended on
                    del toil_leaves[prev_job.jobStoreID]

            # Get their return values to feed into the new job
            rvs: List[Union[WDLBindings, Promise]] = [prev_job.rv() for prev_job in prev_jobs]
            # We also need access to section-level bindings like inputs
            rvs.append(environment)

            if len(node_ids) == 1:
                # Make a one-node job
                job: WDLBaseJob = WDLWorkflowNodeJob(section_graph.get(node_ids[0]), rvs, self._namespace, task_path, wdl_options=self._wdl_options)
            else:
                # Make a multi-node job
                job = WDLWorkflowNodeListJob([section_graph.get(node_id) for node_id in node_ids], rvs, self._namespace, task_path, wdl_options=self._wdl_options)
            for prev_job in prev_jobs:
                # Connect up the happens-after relationships to make sure the
                # return values are available.
                # We have a graph that only needs one kind of happens-after
                # relationship, so we always use follow-ons.
                prev_job.addFollowOn(job)

            if len(prev_jobs) == 0:
                # Nothing came before this job, so connect it to the workflow.
                self.addChild(job)

            for node_id in node_ids:
                # Save the job for everything it executes
                wdl_id_to_toil_job[node_id] = job

            # It isn't depended on yet
            toil_leaves[job.jobStoreID] = job

        if len(toil_leaves) == 1:
            # There's one final node so we can just tack postprocessing onto that.
            sink: WDLBaseJob = next(iter(toil_leaves.values()))
        else:
            # We need to bring together with a new sink
            # Make the sink job to collect all their results.
            leaf_rvs: List[Union[WDLBindings, Promise]] = [leaf_job.rv() for leaf_job in toil_leaves.values()]
            # Make sure to also send the section-level bindings
            leaf_rvs.append(environment)
            # And to fill in bindings from code not executed in this instantiation
            # with Null, and filter out stuff that should leave scope.
            sink = WDLCombineBindingsJob(leaf_rvs, wdl_options=self._wdl_options)
            # It runs inside us
            self.addChild(sink)
            for leaf_job in toil_leaves.values():
                # And after all the leaf jobs.
                leaf_job.addFollowOn(sink)

        logger.debug("Sink job is: %s", sink)


        # Apply the final postprocessing for leaving the section.
        sink.then_underlay(self.make_gather_bindings(gather_nodes, WDL.Value.Null()))
        if local_environment is not None:
            sink.then_remove(local_environment)

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
    def __init__(self, scatter: WDL.Tree.Scatter, prev_node_results: Sequence[Promised[WDLBindings]], namespace: str, task_path: str, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL scatter. The scatter itself and the contents live in the given namespace.
        """
        super().__init__(namespace, task_path, **kwargs, unitName=scatter.workflow_node_id, displayName=scatter.workflow_node_id, wdl_options=wdl_options)

        # Because we need to return the return value of the workflow, we need
        # to return a Toil promise for the last/sink job in the workflow's
        # graph. But we can't save either a job that takes promises, or a
        # promise, in ourselves, because of the way that Toil resolves promises
        # at deserialization. So we need to do the actual building-out of the
        # workflow in run().

        logger.info("Preparing to run scatter on %s", scatter.variable)

        self._scatter = scatter
        self._prev_node_results = prev_node_results

    @report_wdl_errors("run scatter")
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
        standard_library = ToilWDLStdLibBase(file_store, self._task_path)

        # Get what to scatter over
        with monkeypatch_coerce(standard_library):
            try:
                scatter_value = evaluate_named_expression(self._scatter, self._scatter.variable, None, self._scatter.expr, bindings, standard_library)
            finally:
                # Report all files are downloaded now that all expressions are evaluated.
                self.files_downloaded_hook([(p, p) for p in standard_library.get_local_paths()])

        if not isinstance(scatter_value, WDL.Value.Array):
            raise RuntimeError("The returned value from a scatter is not an Array type.")

        scatter_jobs = []
        for subscript, item in enumerate(scatter_value.value):
            # Make an instantiation of our subgraph for each possible value of
            # the variable. Make sure the variable is bound only for the
            # duration of the body.
            local_bindings: WDLBindings = WDL.Env.Bindings()
            local_bindings = local_bindings.bind(self._scatter.variable, item)
            # TODO: We need to turn values() into a list because MyPy seems to
            # think a dict_values isn't a Sequence. This is a waste of time to
            # appease MyPy but probably better than a cast?
            scatter_jobs.append(self.create_subgraph(self._scatter.body, list(self._scatter.gathers.values()), bindings, local_bindings, subscript=subscript))

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
        gather_job = WDLArrayBindingsJob([j.rv() for j in scatter_jobs], bindings, wdl_options=self._wdl_options)
        self.addChild(gather_job)
        for j in scatter_jobs:
            j.addFollowOn(gather_job)
        self.defer_postprocessing(gather_job)
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

    @report_wdl_errors("create array bindings")
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
                binding_type = env.resolve(name).type if env.has_binding(name) else WDL.Type.Any()
                if binding_type not in observed_types:
                    observed_types.append(binding_type)
            # Get the supertype of those types
            supertype: WDL.Type.Base = get_supertype(observed_types)
            # Bind an array of the values
            # TODO: We should be able to assume the binding is always there if this is a scatter, because we create and underlay bindings based on the gathers.
            result = result.bind(name, WDL.Value.Array(supertype, [env.resolve(name) if env.has_binding(name) else WDL.Value.Null() for env in new_bindings]))

        # Base bindings are already included so return the result
        return self.postprocess(result)

class WDLConditionalJob(WDLSectionJob):
    """
    Job that evaluates a conditional in a WDL workflow.
    """
    def __init__(self, conditional: WDL.Tree.Conditional, prev_node_results: Sequence[Promised[WDLBindings]], namespace: str, task_path: str, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL conditional. The conditional itself and its contents live in the given namespace.
        """
        super().__init__(namespace, task_path, **kwargs, unitName=conditional.workflow_node_id, displayName=conditional.workflow_node_id, wdl_options=wdl_options)

        # Once again we need to ship the whole body template to be instantiated
        # into Toil jobs only if it will actually run.

        logger.info("Preparing to run conditional on %s", conditional.expr)

        self._conditional = conditional
        self._prev_node_results = prev_node_results

    @report_wdl_errors("run conditional")
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
        standard_library = ToilWDLStdLibBase(file_store, self._task_path)

        # Get the expression value. Fake a name.
        with monkeypatch_coerce(standard_library):
            try:
                expr_value = evaluate_named_expression(self._conditional, "<conditional expression>", WDL.Type.Boolean(), self._conditional.expr, bindings, standard_library)
            finally:
                # Report all files are downloaded now that all expressions are evaluated.
                self.files_downloaded_hook([(p, p) for p in standard_library.get_local_paths()])

        if expr_value.value:
            # Evaluated to true!
            logger.info('Condition is true')
            # Run the body and return its effects
            body_job = self.create_subgraph(self._conditional.body, list(self._conditional.gathers.values()), bindings)
            self.defer_postprocessing(body_job)
            return body_job.rv()
        else:
            logger.info('Condition is false')
            # Return the input bindings and null bindings for all our gathers.
            # Should not collide at all.
            gather_bindings = self.make_gather_bindings(list(self._conditional.gathers.values()), WDL.Value.Null())
            return self.postprocess(combine_bindings([bindings, gather_bindings]))

class WDLWorkflowJob(WDLSectionJob):
    """
    Job that evaluates an entire WDL workflow.
    """

    def __init__(self, workflow: WDL.Tree.Workflow, prev_node_results: Sequence[Promised[WDLBindings]], workflow_id: List[str], namespace: str, task_path: str, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL workflow. The job returns the
        return value of the workflow.

        :param namespace: the namespace that the workflow's *contents* will be
               in. Caller has already added the workflow's own name.
        """
        super().__init__(namespace, task_path, wdl_options=wdl_options, **kwargs)

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

    @report_wdl_errors("run workflow")
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
        standard_library = ToilWDLStdLibBase(file_store, self._task_path, execution_dir=self._wdl_options.get("execution_dir"))

        if self._workflow.inputs:
            with monkeypatch_coerce(standard_library):
                try:
                    for input_decl in self._workflow.inputs:
                        # Evaluate all the inputs that aren't pre-set
                        bindings = bindings.bind(input_decl.name, evaluate_defaultable_decl(input_decl, bindings, standard_library))
                finally:
                    # Report all files are downloaded now that all expressions are evaluated.
                    self.files_downloaded_hook([(p, p) for p in standard_library.get_local_paths()])

        # Make jobs to run all the parts of the workflow
        sink = self.create_subgraph(self._workflow.body, [], bindings)

        if self._workflow.outputs != []:  # Compare against empty list as None means there should be outputs
            # Either the output section is declared and nonempty or it is not declared
            # Add evaluating the outputs after the sink
            outputs_job = WDLOutputsJob(self._workflow, sink.rv(), self._task_path, wdl_options=self._wdl_options)
            sink.addFollowOn(outputs_job)
            # Caller is responsible for making sure namespaces are applied
            self.defer_postprocessing(outputs_job)
            return outputs_job.rv()
        else:
            # No outputs from this workflow.
            return self.postprocess(WDL.Env.Bindings())

class WDLOutputsJob(WDLBaseJob):
    """
    Job which evaluates an outputs section (such as for a workflow).

    Returns an environment with just the outputs bound, in no namespace.
    """
    def __init__(self, workflow: WDL.Tree.Workflow, bindings: Promised[WDLBindings], task_path: str, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any):
        """
        Make a new WDLWorkflowOutputsJob for the given workflow, with the given set of bindings after its body runs.
        """
        super().__init__(wdl_options=wdl_options, **kwargs)

        self._bindings = bindings
        self._workflow = workflow
        self._task_path = task_path

    @report_wdl_errors("evaluate outputs")
    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Make bindings for the outputs.
        """
        super().run(file_store)

        # Evaluate all output expressions in the normal, non-task-outputs library context
        standard_library = ToilWDLStdLibBase(file_store, self._task_path, execution_dir=self._wdl_options.get("execution_dir"))

        try:
            if self._workflow.outputs is not None:
                # Output section is declared and is nonempty, so evaluate normally

                # Combine the bindings from the previous job
                with monkeypatch_coerce(standard_library):
                    output_bindings = evaluate_output_decls(self._workflow.outputs, unwrap(self._bindings), standard_library)
            else:
                # If no output section is present, start with an empty bindings
                output_bindings = WDL.Env.Bindings()

            if self._workflow.outputs is None or self._wdl_options.get("all_call_outputs", False):
                # The output section is not declared, or we want to keep task outputs anyway.

                # Get all task outputs and return that
                # First get all task output names
                output_set = set()
                # We need to recurse down through scatters and conditionals to find all the task names.
                # The output variable names won't involve the scatters or conditionals as components.
                stack = list(self._workflow.body)
                while stack != []:
                    node = stack.pop()
                    if isinstance(node, WDL.Tree.Call):
                        # For calls, promote all output names to workflow output names
                        # TODO: Does effective_outputs already have the right
                        # stuff for calls to workflows that themselves lack
                        # output sections? If so, can't we just use that for
                        # *this* workflow?
                        for type_binding in node.effective_outputs:
                            output_set.add(type_binding.name)
                    elif isinstance(node, WDL.Tree.Scatter) or isinstance(node, WDL.Tree.Conditional):
                        # For scatters and conditionals, recurse looking for calls.
                        for subnode in node.body:
                            stack.append(subnode)
                # Collect all bindings that are task outputs
                output_bindings: WDL.Env.Bindings[WDL.Value.Base] = WDL.Env.Bindings()
                for binding in unwrap(self._bindings):
                    if binding.name in output_set:
                        # The bindings will already be namespaced with the task namespaces
                        output_bindings = output_bindings.bind(binding.name, binding.value)
        finally:
            # We don't actually know when all our files are downloaded since
            # anything we evaluate might devirtualize inside any expression.
            # But we definitely know they're done being downloaded if we throw
            # an error or if we finish, so hook in now and let the debugging
            # logic stop the worker before any error does.
            #
            # Make sure to feed in all the paths we devirtualized as if they
            # were mounted into a container at their actual paths.
            self.files_downloaded_hook([(p, p) for p in standard_library.get_local_paths()])

        # Null nonexistent optional values and error on the rest
        output_bindings = drop_missing_files(output_bindings, self._wdl_options.get("execution_dir"))

        return self.postprocess(output_bindings)

class WDLRootJob(WDLSectionJob):
    """
    Job that evaluates an entire WDL workflow, and returns the workflow outputs
    namespaced with the workflow name. Inputs may or may not be namespaced with
    the workflow name; both forms are accepted.
    """

    def __init__(self, target: Union[WDL.Tree.Workflow, WDL.Tree.Task], inputs: WDLBindings, wdl_options: Optional[Dict[str, str]] = None, **kwargs: Any) -> None:
        """
        Create a subtree to run the workflow and namespace the outputs.
        """

        # The root workflow names the root namespace and task path.
        super().__init__(target.name, target.name, wdl_options=wdl_options, **kwargs)
        self._target = target
        self._inputs = inputs

    @report_wdl_errors("run root job")
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually build the subgraph.
        """
        super().run(file_store)
        if isinstance(self._target, WDL.Tree.Workflow):
            # Create a workflow job. We rely in this to handle entering the input
            # namespace if needed, or handling free-floating inputs.
            job: WDLBaseJob = WDLWorkflowJob(self._target, [self._inputs], [self._target.name], self._namespace, self._task_path, wdl_options=self._wdl_options)
        else:
            # There is no workflow. Create a task job.
            job = WDLTaskWrapperJob(self._target, [self._inputs], [self._target.name], self._namespace, self._task_path, wdl_options=self._wdl_options)
        # Run the task or workflow
        job.then_namespace(self._namespace)
        self.addChild(job)
        self.defer_postprocessing(job)
        return job.rv()


@contextmanager
def monkeypatch_coerce(standard_library: ToilWDLStdLibBase) -> Generator[None, None, None]:
    """
    Monkeypatch miniwdl's WDL.Value.Base.coerce() function to virtualize files when they are represented as Strings.
    Calls _virtualize_filename from a given standard library object.
    :param standard_library: a standard library object
    :return
    """
    # We're doing this because while miniwdl recognizes when a string needs to be converted into a file, its method of
    # conversion is to just store the local filepath. Toil needs to virtualize the file into the jobstore so until
    # there is a proper hook, monkeypatch it.

    SelfType = TypeVar("SelfType", bound=WDL.Value.Base)
    def make_coerce(old_coerce: Callable[[SelfType, Optional[WDL.Type.Base]], WDL.Value.Base]) -> Callable[[Arg(SelfType, 'self'), DefaultArg(Optional[WDL.Type.Base], 'desired_type')], WDL.Value.Base]:
        """
        Stamp out a replacement coerce method that calls the given original one.
        """
        def coerce(self: SelfType, desired_type: Optional[WDL.Type.Base] = None) -> WDL.Value.Base:
            if isinstance(desired_type, WDL.Type.File) and not isinstance(self, WDL.Value.File):
                # Coercing something to File.
                if not is_url(self.value) and not os.path.isfile(os.path.join(standard_library.execution_dir or ".", self.value)):
                    # It is a local file that isn't there.
                    return WDL.Value.File(TOIL_NONEXISTENT_URI_SCHEME + self.value, self.expr)
                else:
                    # Virtualize normally
                    return WDL.Value.File(standard_library._virtualize_filename(self.value), self.expr)
            return old_coerce(self, desired_type)

        return coerce

    old_base_coerce = WDL.Value.Base.coerce
    old_str_coerce = WDL.Value.String.coerce
    try:
        # Mypy does not like monkeypatching:
        # https://github.com/python/mypy/issues/2427#issuecomment-1419206807
        WDL.Value.Base.coerce = make_coerce(old_base_coerce)  # type: ignore[method-assign]
        WDL.Value.String.coerce = make_coerce(old_str_coerce)  # type: ignore[method-assign]
        yield
    finally:
        WDL.Value.Base.coerce = old_base_coerce  # type: ignore[method-assign]
        WDL.Value.String.coerce = old_str_coerce  # type: ignore[method-assign]

@report_wdl_errors("run workflow", exit=True)
def main() -> None:
    """
    A Toil workflow to interpret WDL input files.
    """
    args = sys.argv[1:]

    parser = ArgParser(description='Runs WDL files with toil.')
    addOptions(parser, jobstore_as_flag=True, wdl=True)

    options = parser.parse_args(args)

    # Make sure we have a jobStore
    if options.jobStore is None:
        # TODO: Move cwltoil's generate_default_job_store where we can use it
        options.jobStore = os.path.join(mkdtemp(), 'tree')

    # Make sure we have an output directory (or URL prefix) and we don't need
    # to ever worry about a None, and MyPy knows it.
    # If we don't have a directory assigned, make one in the current directory.
    output_directory: str = options.output_directory if options.output_directory else mkdtemp(prefix='wdl-out-', dir=os.getcwd())

    # Get the execution directory
    execution_dir = os.getcwd()
    try:
        with Toil(options) as toil:
            if options.restart:
                output_bindings = toil.restart()
            else:
                # Load the WDL document
                document: WDL.Tree.Document = WDL.load(options.wdl_uri, read_source=toil_read_source)

                # See if we're going to run a workflow or a task
                target: Union[WDL.Tree.Workflow, WDL.Tree.Task]
                if document.workflow:
                    target = document.workflow
                elif len(document.tasks) == 1:
                    target = document.tasks[0]
                elif len(document.tasks) > 1:
                    raise WDL.Error.InputError("Multiple tasks found with no workflow! Either add a workflow or keep one task.")
                else:
                    raise WDL.Error.InputError("WDL document is empty!")

                if "croo_out_def" in target.meta:
                    # This workflow or task wants to have its outputs
                    # "organized" by the Cromwell Output Organizer:
                    # <https://github.com/ENCODE-DCC/croo>.
                    #
                    # TODO: We don't support generating anything that CROO can read.
                    logger.warning("This WDL expects to be used with the Cromwell Output Organizer (croo) <https://github.com/ENCODE-DCC/croo>. Toil cannot yet produce the outputs that croo requires. You will not be able to use croo on the output of this Toil run!")

                    # But we can assume that we need to preserve individual
                    # taks outputs since the point of CROO is fetching those
                    # from Cromwell's output directories.
                    #
                    # This isn't quite WDL spec compliant but it will rescue
                    # runs of the popular
                    # <https://github.com/ENCODE-DCC/atac-seq-pipeline>
                    if options.all_call_outputs is None:
                        logger.warning("Inferring --allCallOutputs=True to preserve probable actual outputs of a croo WDL file.")
                        options.all_call_outputs = True


                if options.inputs_uri:
                    # Load the inputs. Use the same loading mechanism, which means we
                    # have to break into async temporarily.
                    if options.inputs_uri[0] == "{":
                        input_json = options.inputs_uri
                    elif options.inputs_uri == "-":
                        input_json = sys.stdin.read()
                    else:
                        input_json = asyncio.run(toil_read_source(options.inputs_uri, [], None)).source_text
                    try:
                        inputs = json.loads(input_json)
                    except json.JSONDecodeError as e:
                        # Complain about the JSON document.
                        # We need the absolute path or URL to raise the error
                        inputs_abspath = options.inputs_uri if not os.path.exists(options.inputs_uri) else os.path.abspath(options.inputs_uri)
                        raise WDL.Error.ValidationError(WDL.Error.SourcePosition(options.inputs_uri, inputs_abspath, e.lineno, e.colno, e.lineno, e.colno + 1), "Cannot parse input JSON: " + e.msg) from e
                else:
                    inputs = {}

                # Parse out the available and required inputs. Each key in the
                # JSON ought to start with the workflow's name and then a .
                # TODO: WDL's Bindings[] isn't variant in the right way, so we
                # have to cast from more specific to less specific ones here.
                # The miniwld values_from_json function can evaluate
                # expressions in the inputs or something.
                WDLTypeDeclBindings = Union[WDL.Env.Bindings[WDL.Tree.Decl], WDL.Env.Bindings[WDL.Type.Base]]
                input_bindings = WDL.values_from_json(
                    inputs,
                    cast(WDLTypeDeclBindings, target.available_inputs),
                    cast(Optional[WDLTypeDeclBindings], target.required_inputs),
                    target.name
                )

                # Determine where to look for files referenced in the inputs, in addition to here.
                inputs_search_path = []
                if options.inputs_uri:
                    inputs_search_path.append(options.inputs_uri)

                    match = re.match(r'https://raw\.githubusercontent\.com/[^/]*/[^/]*/[^/]*/', options.inputs_uri)
                    if match:
                        # Special magic for Github repos to make e.g.
                        # https://raw.githubusercontent.com/vgteam/vg_wdl/44a03d9664db3f6d041a2f4a69bbc4f65c79533f/params/giraffe.json
                        # work when it references things relative to repo root.
                        logger.info("Inputs appear to come from a Github repository; adding repository root to file search path")
                        inputs_search_path.append(match.group(0))

                # Import any files in the bindings
                input_bindings = import_files(input_bindings, target.name, toil, inputs_search_path, skip_remote=options.reference_inputs)

                # TODO: Automatically set a good MINIWDL__SINGULARITY__IMAGE_CACHE ?

                # Get the execution directory
                execution_dir = os.getcwd()

                # Configure workflow interpreter options
                wdl_options: Dict[str, str] = {}
                wdl_options["execution_dir"] = execution_dir
                wdl_options["container"] = options.container
                assert wdl_options.get("container") is not None
                wdl_options["all_call_outputs"] = options.all_call_outputs

                # Run the workflow and get its outputs namespaced with the workflow name.
                root_job = WDLRootJob(target, input_bindings, wdl_options=wdl_options)
                output_bindings = toil.start(root_job)
            if not isinstance(output_bindings, WDL.Env.Bindings):
                raise RuntimeError("The output of the WDL job is not a binding.")

            devirtualization_state: DirectoryNamingStateDict = {}
            devirtualized_to_virtualized: Dict[str, str] = dict()
            virtualized_to_devirtualized: Dict[str, str] = dict()

            # Fetch all the output files
            def devirtualize_output(filename: str) -> str:
                """
                'devirtualize' a file using the "toil" object instead of a filestore.
                Returns its local path.
                """
                # Make sure the output directory exists if we have output files
                # that might need to use it.
                os.makedirs(output_directory, exist_ok=True)
                return ToilWDLStdLibBase.devirtualize_to(
                    filename,
                    output_directory,
                    toil,
                    execution_dir,
                    devirtualization_state,
                    devirtualized_to_virtualized,
                    virtualized_to_devirtualized
                )

            # Make all the files local files
            output_bindings = map_over_files_in_bindings(output_bindings, devirtualize_output)

            # Report the result in the right format
            outputs = WDL.values_to_json(output_bindings)
            if options.output_dialect == 'miniwdl':
                outputs = {'dir': output_directory, 'outputs': outputs}
            if options.output_file is None:
                # Send outputs to standard out
                print(json.dumps(outputs))
            else:
                # Export output to path or URL.
                # So we need to import and then export.
                fd, filename = mkstemp()
                with open(fd, 'w') as handle:
                    # Populate the file
                    handle.write(json.dumps(outputs))
                    handle.write('\n')
                # Import it. Don't link because the temp file will go away.
                file_id = toil.import_file(filename, symlink=False)
                # Delete the temp file
                os.remove(filename)
                # Export it into place
                toil.export_file(file_id, options.output_file)
    except FailedJobsException as e:
        logger.error("WDL job failed: %s", e)
        sys.exit(e.exit_code)


if __name__ == "__main__":
    main()





