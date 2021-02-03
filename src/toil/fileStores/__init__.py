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
import os
import stat

from uuid import uuid4


def make_public_dir(dirName: str) -> str:
    """Makes a given subdirectory if it doesn't already exist, making sure it is public."""
    if not os.path.exists(dirName):
        os.mkdir(dirName)
        os.chmod(dirName, 0o777)
    return dirName


def unused_dir_path(prefix: str) -> str:
    """
    Try to make a random directory name with length 4 that doesn't exist, with the given prefix.

    Otherwise, try length 5, length 6, etc, up to a max of 32 (len of uuid4 with dashes replaced).

    This function's purpose is mostly to avoid having long file names when generating directories.

    If somehow this fails, which should be incredibly unlikely, default to a normal uuid4, which was
    our old default.
    """
    for i in range(4, 32 + 1):  # make random uuids and truncate to lengths starting at 4 and working up to max 32
        for _ in range(10):  # make 10 attempts for each length
            truncated_uuid = str(uuid4()).replace('-', '')[:i]
            generated_dir_path = os.path.join(prefix, truncated_uuid)
            if not os.path.exists(generated_dir_path):
                return generated_dir_path
    this_should_never_happen = os.path.join(prefix, str(uuid4()))
    assert not os.path.exists(this_should_never_happen)
    return this_should_never_happen


def make_unique_public_dir(prefix: str) -> str:
    """Makes a new subdirectory with the given prefix, making sure it is public."""
    return make_public_dir(unused_dir_path(prefix))


class FileID(str):
    """
    A small wrapper around Python's builtin string class. It is used to represent a file's ID in the file store, and
    has a size attribute that is the file's size in bytes. This object is returned by importFile and writeGlobalFile.

    Calls into the file store can use bare strings; size will be queried from the job store if unavailable in the ID.
    """

    def __new__(cls, fileStoreID, *args):
        return super(FileID, cls).__new__(cls, fileStoreID)

    def __init__(self, fileStoreID, size, executable=False):
        # Don't pass an argument to parent class's __init__.
        # In Python 3 we can have super(FileID, self) hand us object's __init__ which chokes on any arguments.
        super(FileID, self).__init__()
        self.size = size
        self.executable = executable

    def pack(self):
        """
        Pack the FileID into a string so it can be passed through external code.
        """
        return '{}:{}:{}'.format(self.size, int(self.executable), self)

    @classmethod
    def forPath(cls, fileStoreID, filePath):
        executable = os.stat(filePath).st_mode & stat.S_IXUSR != 0
        return cls(fileStoreID, os.stat(filePath).st_size, executable)

    @classmethod
    def unpack(cls, packedFileStoreID):
        """
        Unpack the result of pack() into a FileID object.
        """
        # Only separate twice in case the FileID itself has colons in it
        vals = packedFileStoreID.split(':', 2)
        # Break up the packed value
        size = int(vals[0])
        executable = bool(vals[1])
        value = vals[2]
        # Create the FileID
        return cls(value, size, executable)
