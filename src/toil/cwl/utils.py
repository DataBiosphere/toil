# Copyright (C) 2015-2021 Regents of the University of California
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

"""Utility functions used for Toil's CWL interpreter."""

import logging
import os
import posixpath
import stat
from collections.abc import Iterable, MutableMapping, MutableSequence
from pathlib import PurePosixPath
from typing import (
    Any,
    Callable,
    TypeVar,
    Union,
    Optional,
    cast,
    MutableSequence,
    MutableMapping,
    TYPE_CHECKING,
)
from urllib.parse import unquote, urlparse

if TYPE_CHECKING:
    # This module needs to be importable even if cwltool is not installed.
    from cwltool.utils import CWLObjectType, CWLOutputType
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.lib.url import URLAccess

logger = logging.getLogger(__name__)

# Customized CWL utilities

# What exit code do we need to bail with if we or any of the local jobs that
# parse workflow files see an unsupported feature?
CWL_UNSUPPORTED_REQUIREMENT_EXIT_CODE = 33

# And what error will make the worker exit with that code


class CWLUnsupportedException(Exception):
    """Fallback exception."""


try:
    import cwltool.errors

    CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION: Union[
        type[cwltool.errors.UnsupportedRequirement], type[CWLUnsupportedException]
    ] = cwltool.errors.UnsupportedRequirement
except ImportError:
    CWL_UNSUPPORTED_REQUIREMENT_EXCEPTION = CWLUnsupportedException


def visit_top_cwl_class(
    rec: Any, classes: Iterable[str], op: Callable[[Any], Any]
) -> None:
    """
    Apply the given operation to all top-level CWL objects with the given named CWL class.

    Like cwltool's visit_class but doesn't look inside any object visited.
    """
    if isinstance(rec, MutableMapping):
        if rec.get("class", None) in classes:
            # This is one of the classes requested
            # So process it
            op(rec)
        else:
            # Look inside it instead
            for key in rec:
                visit_top_cwl_class(rec[key], classes, op)
    elif isinstance(rec, MutableSequence):
        # This item is actually a list of things, so look at all of them.
        for key in rec:
            visit_top_cwl_class(key, classes, op)


DownReturnType = TypeVar("DownReturnType")
UpReturnType = TypeVar("UpReturnType")


def visit_cwl_class_and_reduce(
    rec: Any,
    classes: Iterable[str],
    op_down: Callable[[Any], DownReturnType],
    op_up: Callable[[Any, DownReturnType, list[UpReturnType]], UpReturnType],
) -> list[UpReturnType]:
    """
    Apply the given operations to all CWL objects with the given named CWL class.

    Applies the down operation top-down, and the up operation bottom-up, and
    passes the down operation's result and a list of the up operation results
    for all child keys (flattening across lists and collapsing nodes of
    non-matching classes) to the up operation.

    :returns: The flattened list of up operation results from all calls.
    """
    results = []

    if isinstance(rec, MutableMapping):
        child_results = []
        if rec.get("class", None) in classes:
            # Apply the down operation
            down_result = op_down(rec)
        for key in rec:
            # Look inside and collect child results
            for result in visit_cwl_class_and_reduce(rec[key], classes, op_down, op_up):
                child_results.append(result)
        if rec.get("class", None) in classes:
            # Apply the up operation
            results.append(op_up(rec, down_result, child_results))
        else:
            # We aren't processing here so pass up all the child results
            results += child_results
    elif isinstance(rec, MutableSequence):
        # This item is actually a list of things, so look at all of them.
        for key in rec:
            for result in visit_cwl_class_and_reduce(key, classes, op_down, op_up):
                # And flatten together all their results.
                results.append(result)
    return results


DirectoryStructure = dict[str, Union[str, "DirectoryStructure"]]


def get_from_structure(
    dir_dict: DirectoryStructure, path: str
) -> Union[str, DirectoryStructure, None]:
    """
    Given a relative path, follow it in the given directory structure.

    Return the string URI for files, the directory dict for
    subdirectories, or None for nonexistent things.
    """

    # Resolve .. and split into path components
    parts = PurePosixPath(posixpath.normpath(path)).parts
    if len(parts) == 0:
        return dir_dict
    if parts[0] in ("..", "/"):
        raise RuntimeError(f"Path {path} not resolvable in virtual directory")
    found: Union[str, DirectoryStructure] = dir_dict
    for part in parts:
        # Go down by each path component in turn
        if isinstance(found, str):
            # Looking for a subdirectory of a file, which doesn't exist
            return None
        if part not in found:
            return None
        found = found[part]
    # Now we're at the place we want to be.
    return found


def download_structure(
    file_store: AbstractFileStore,
    index: dict[str, str],
    existing: dict[str, str],
    dir_dict: DirectoryStructure,
    into_dir: str,
) -> None:
    """
    Download nested dictionary from the Toil file store to a local path.

    Guaranteed to fill the structure with real files, and not symlinks out of
    it to elsewhere. File URIs may be toilfile: URIs or any other URI that
    Toil's job store system can read.

    :param file_store: The Toil file store to download from.

    :param index: Maps from downloaded file path back to input URI.

    :param existing: Maps from file_store_id URI to downloaded file path.

    :param dir_dict: a dict from string to string (for files) or dict (for
        subdirectories) describing a directory structure.

    :param into_dir: The directory to download the top-level dict's files
        into.
    """
    logger.debug("Downloading directory with %s items", len(dir_dict))

    for name, value in dir_dict.items():
        if name == ".":
            # Skip this key that isn't a real child file.
            continue
        if isinstance(value, dict):
            # This is a subdirectory, so make it and download
            # its contents
            logger.debug("Downloading subdirectory '%s'", name)
            subdir = os.path.join(into_dir, name)
            os.mkdir(subdir)
            download_structure(file_store, index, existing, value, subdir)
        else:
            # This must be a file path uploaded to Toil.
            if not isinstance(value, str):
                raise RuntimeError(f"Did not find a file at {value}.")

            logger.debug("Downloading contained file '%s'", name)
            dest_path = os.path.join(into_dir, name)

            if value.startswith("toilfile:"):
                # So download the file into place.
                # Make sure to get a real copy of the file because we may need to
                # mount the directory into a container as a whole.
                file_store.readGlobalFile(
                    FileID.unpack(value[len("toilfile:") :]), dest_path, symlink=False
                )
            else:
                # We need to download from some other kind of URL.
                size, executable = URLAccess.read_from_url(
                    value, open(dest_path, "wb")
                )
                if executable:
                    # Make the written file executable
                    os.chmod(dest_path, os.stat(dest_path).st_mode | stat.S_IXUSR)

            # Update the index dicts
            # TODO: why?
            index[dest_path] = value
            existing[value] = dest_path


def trim_mounts_op_down(file_or_directory: "CWLObjectType") -> None:
    """
    No-op function for mount-point trimming.
    """
    return


def sniff_location(file_or_directory: "CWLObjectType") -> Optional[str]:
    """
    Get the local bare path for a CWL file or directory, or None.

    :return: None if we don't have a local path or file URI
    """
    if file_or_directory.get('location') is None and file_or_directory.get('path') is None:
        # file or directory is defined by contents or listing respectively, this is not redundant
        return None
    # Since we only consider mountable paths, if path is not file URI or bare path, don't consider it
    path_or_url = cast(str, file_or_directory.get('location') or file_or_directory.get('path'))
    parsed = urlparse(path_or_url)
    if parsed.scheme == 'file':
        return unquote(parsed.path)
    elif parsed.scheme == '':
        return path_or_url
    else:
        return None


def trim_mounts_op_up(file_or_directory: "CWLObjectType", op_down_ret: None, child_results: list[bool]) -> bool:
    """
    Remove subtrees of the CWL file or directory object tree that only have redundant stuff in them.

    Nonredundant for something in a directory means its path or location is not within the parent directory or doesn't match its basename
    Nonredundant for something in a secondary file means its path or location is not adjacent to the primary file or doesn't match its basename

    If on a File:
    Returns True if anything in secondary files is nonredundant or has nonredundant children to this file, false otherwise
    If on a Directory:
    Returns True if anything in top level listing is nonredundant or has nonredundant children, otherwise false.
    If something in the listing is redundant and all children are redundant, then delete it
    :param file_or_directory: CWL file or CWL directory type
    :return: boolean
    """
    own_path = sniff_location(file_or_directory)
    if own_path is None:
        return True
    # basename should be set as we are the implementation
    own_basename = cast(str, file_or_directory['basename'])

    # If the basename does not match the path, then this is nonredundant
    if not own_path.endswith("/" + own_basename):
        return True

    if file_or_directory['class'] == 'File':
        if any(child_results):
            # one of the children was detected as not redundant
            return True
        for secondary in cast(MutableSequence[MutableMapping[str, "CWLOutputType"]], file_or_directory.get('secondaryFiles', [])):
            # secondary files should already be flagged nonredundant if they don't have either a path or location
            secondary_path = sniff_location(secondary)
            secondary_basename = cast(str, secondary['basename'])
            # If we swap the secondary basename for the primary basename in the primary path, and they don't match, then they are nonredundant
            if os.path.join(own_path[:-len(own_basename)], secondary_basename) != secondary_path:
                return True
    else:
        listings = cast(MutableSequence[MutableMapping[str, "CWLOutputType"]], file_or_directory.get('listing', []))
        if len(listings) == 0:
            return False
        # We assume child_results is in the same order as the directory listing
        # iterate backwards to avoid iteration issues
        for i in range(len(listings) - 1, -1, -1):
            if child_results[i] is False:
                if os.path.join(own_path, cast(str, listings[i]['basename'])) == sniff_location(listings[i]):
                    del listings[i]
        # If one of the listings was nonredundant, then this directory is also nonredundant
        if any(child_results):
            return True
    return False

def remove_redundant_mounts(cwljob: "CWLObjectType") -> None:
    """
    Remove any redundant mount points from the listing. Modifies the CWL object in place.
    """
    visit_cwl_class_and_reduce(cwljob, ["Directory", "File"], trim_mounts_op_down, trim_mounts_op_up)
