# Copyright (C) 2015-2025 Regents of the University of California
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

import json
import base64

from urllib.parse import quote, unquote

from typing import Iterator, Optional, Union

TOIL_DIR_URI_SCHEME = "toildir:"


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
) -> tuple[DirectoryContents, Optional[str], str, Optional[str], Optional[str]]:
    """
    Decode a directory from a "toildir:" path to a directory (or a file in it).

    :returns: the decoded directory dict, the remaining part of the path (which
        may be None), an identifier string for the directory (which is the
        stored name URI if one was provided), and the name URI and source task
        info.
    """
    if not dir_path.startswith(TOIL_DIR_URI_SCHEME):
        raise RuntimeError(f"Cannot decode non-directory path: {dir_path}")

    # We will decode the directory and then look inside it

    # Since this was encoded by upload_directory we know the
    # next pieces are encoded source URL, encoded source task, and JSON
    # describing the directory structure, and it can't contain any slashes.
    #
    # So split on slash to separate all that from the path components within
    # the directory to whatever we're trying to get.
    parts = dir_path[len(TOIL_DIR_URI_SCHEME) :].split("/", 1)

    # Before the first slash is the encoded data describing the directory contents
    encoded_name, encoded_source, dir_data = parts[0].split(":")
    # Decode the name and source, replacing empty string with None again.
    name: Optional[str] = unquote(encoded_name) or None
    source: Optional[str] = unquote(encoded_source) or None
    
    # We need the unique key identifying this directory, which is where it came
    # from if stored, or the encoded data itself otherwise.
    # TODO: Is this too complicated?
    directory_identifier = name if name is not None else dir_data

    # Decode what to download
    contents = json.loads(
        base64.urlsafe_b64decode(dir_data.encode("utf-8")).decode("utf-8")
    )

    check_directory_dict_invariants(contents)

    if len(parts) == 1 or parts[1] == "/":
        # We didn't have any subdirectory
        return contents, None, directory_identifier, name, source
    else:
        # We have a path below this
        return contents, parts[1], directory_identifier, name, source

def encode_directory(contents: DirectoryContents, name: Optional[str] = None, source: Optional[str] = None) -> str:
    """
    Encode a directory from a "toildir:" path to a directory (or a file in it).

    :param contents: the directory dict, which is a dict from name to URI for a
        file or dict for a subdirectory.
    :param name: the path or URI the directory belongs at, including its
        basename. May not be empty if set.
    :param source: the name of a workflow component that uploaded the
        directory. May not be empty if set.
    """

    check_directory_dict_invariants(contents)

    parts = [
        TOIL_DIR_URI_SCHEME[:-1],
        quote(name or "", safe=""),
        quote(source or "", safe=""),
        base64.urlsafe_b64encode(
            json.dumps(contents).encode("utf-8")
        ).decode("utf-8"),
    ]

    return ":".join(parts)


def directory_item_exists(dir_path: str) -> bool:
    """
    Checks that a URL to a Toil directory or thing in it actually exists.

    Assumes that all the pointed-to URLs exist; just checks tha tthe thing is
    actually in the encoded directory structure.
    """

    try:
        get_directory_item(dir_path)
    except FileNotFoundError:
        return False
    return True

def get_directory_contents_item(contents: DirectoryContents, remaining_path: Optional[str]) -> Union[DirectoryContents, str]:
    """
    Get a subdirectory or file from a decoded directory and remaining path.
    """

    if remaining_path is None:
        return contents
    
    here: Union[str, DirectoryContents] = contents
    for part in remaining_path.split("/"):
        if not isinstance(here, dict):
            # We're trying to go inside a file
            raise FileNotFoundError(remaining_path)
        if part not in here:
            # We've hit a nonexistent path component
            raise FileNotFoundError(remaining_path)
        here = here[part]
    # If we get here we successfully looked up the thing in the structure
    return here

def get_directory_item(dir_path: str) -> Union[DirectoryContents, str]:
    """
    Get a subdirectory or file from a URL pointing to or into a toildir: directory.
    """

    contents, remaining_path, _, _, _ = decode_directory(dir_path)
    
    try:
        return get_directory_contents_item(contents, remaining_path)
    except FileNotFoundError:
        # Rewrite file not found to be about the full thing we went to look up.
        raise FileNotFoundError(dir_path)

def directory_contents_items(contents: DirectoryContents) -> Iterator[tuple[str, Union[str, None]]]:
    """
    Yield each file or directory under the given contents, including itself.

    Yields parent items before children.

    Yields each item as a str path from the root (possibly empty), and either a
        str value for files or a None for directories.

    The path won't have trailing slashes.
    """

    # Yield the thing itself
    yield ("", None)

    for k, v in contents.items():
        if isinstance(v, str):
            # Yield a file
            yield (k, v)
        else:
            # Recurse on the directory
            for child_path, child_value in directory_contents_items(v):
                yield (f"{k}/{child_path}", child_value)

def directory_items(dir_path: str) -> Iterator[tuple[str, Union[str, None]]]:
    """
    Yield each file or directory under the given path, including itself.

    Yields parent items before children.

    Yields each item as a str path from the root (possibly empty), and either a
        str value for files or a None for directories.

    The path won't have trailing slashes.
    """

    item = get_directory_item(dir_path)

    if isinstance(item, str):
        # Only one item and it's this file
        yield ("", item)
    else:
        # It's a directory in there.
        yield from directory_contents_items(item)

        





