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

from typing import Iterator, Optional, Union

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

def get_directory_item(dir_path: str) -> Union[DirectoryContents, str]:
    """
    Get a subdirectory or file from a URL pointing to or into a toildir: directory.
    """

    contents, remaining_path, _ = decode_directory(dir_path)

    if remaining_path is None:
        return contents
    
    here: Union[str, DirectoryContents] = contents
    for part in remaining_path.split("/"):
        if not isinstance(here, dict):
            # We're trying to go inside a file
            raise FileNotFoundError(dir_path)
        if part not in here:
            # We've hit a nonexistent path component
            raise FileNotFoundError(dir_path)
        here = here[part]
    # If we get here we successfully looked up the thing in the structure
    return here

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

        





