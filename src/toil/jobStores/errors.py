# Copyright (C) 2020 Regents of the University of California
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


class NoSuchJobException(Exception):
    """Indicates that the specified job does not exist."""
    def __init__(self, jobStoreID):
        """
        :param str jobStoreID: the jobStoreID that was mistakenly assumed to exist
        """
        super().__init__("The job '%s' does not exist." % jobStoreID)


class ConcurrentFileModificationException(Exception):
    """Indicates that the file was attempted to be modified by multiple processes at once."""
    def __init__(self, jobStoreFileID):
        """
        :param str jobStoreFileID: the ID of the file that was modified by multiple workers
               or processes concurrently
        """
        super().__init__('Concurrent update to file %s detected.' % jobStoreFileID)


class NoSuchFileException(Exception):
    """Indicates that the specified file does not exist."""
    def __init__(self, jobStoreFileID, customName=None, *extra):
        """
        :param str jobStoreFileID: the ID of the file that was mistakenly assumed to exist
        :param str customName: optionally, an alternate name for the nonexistent file
        :param list extra: optional extra information to add to the error message
        """
        # Having the extra argument may help resolve the __init__() takes at
        # most three arguments error reported in
        # https://github.com/DataBiosphere/toil/issues/2589#issuecomment-481912211
        if customName is None:
            message = f"File '{jobStoreFileID}' does not exist."
        else:
            message = f"File '{customName}' ({jobStoreFileID}) does not exist."

        if extra:
            message += " Extra info: " + " ".join((str(x) for x in extra))

        super().__init__(message)


class NoSuchJobStoreException(Exception):
    """Indicates that the specified job store does not exist."""
    def __init__(self, location):
        super().__init__(f"The job store '{location}' does not exist, so there is nothing to restart.")


class JobStoreExistsException(Exception):
    """Indicates that the specified job store already exists."""
    def __init__(self, location):
        super().__init__(
            f"The job store '{location}' already exists. Use --restart to resume the workflow, or remove "
            f"the job store with 'toil clean' to start the workflow from scratch.")


class ChecksumError(Exception):
    """
    Raised when a download from AWS does not contain the correct data.
    """
    pass
