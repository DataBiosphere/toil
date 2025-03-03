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
import urllib.parse as urlparse

from typing import Optional, List


class InvalidImportExportUrlException(Exception):
    def __init__(self, url: urlparse.ParseResult) -> None:
        """
        :param urlparse.ParseResult url:
        """
        super().__init__("The URL '%s' is invalid." % url.geturl())


class NoSuchJobException(Exception):
    """Indicates that the specified job does not exist."""
    def __init__(self, jobStoreID: str) -> None:
        """
        :param str jobStoreID: the jobStoreID that was mistakenly assumed to exist
        """
        super().__init__("The job '%s' does not exist." % jobStoreID)


class ConcurrentFileModificationException(Exception):
    """Indicates that the file was attempted to be modified by multiple processes at once."""
    def __init__(self, jobStoreFileID: str) -> None:
        """
        :param str jobStoreFileID: the ID of the file that was modified by multiple workers
               or processes concurrently
        """
        super().__init__('Concurrent update to file %s detected.' % jobStoreFileID)


class NoSuchFileException(Exception):
    """Indicates that the specified file does not exist."""
    def __init__(self, jobStoreFileID: str, customName: Optional[str] = None, *extra: Optional[List[str]]) -> None:
        """
        :param str jobStoreFileID: the ID of the file that was mistakenly assumed to exist
        :param str customName: optionally, an alternate name for the nonexistent file
        :param list extra: optional extra information to add to the error message
        """
        # Having the extra argument may help resolve the __init__() takes at
        # most three arguments error reported in
        # https://github.com/DataBiosphere/toil/issues/2589#issuecomment-481912211
        if customName is None:
            message = "File '%s' does not exist." % jobStoreFileID
        else:
            message = "File '%s' (%s) does not exist." % (customName, jobStoreFileID)

        if extra:
            # Append extra data.
            message += " Extra info: " + " ".join((str(x) for x in extra))

        super().__init__(message)


class NoSuchJobStoreException(Exception):
    """Indicates that the specified job store does not exist."""
    def __init__(self, locator: str) -> None:
        super().__init__("The job store '%s' does not exist, so there is nothing to restart." % locator)


class JobStoreExistsException(Exception):
    """Indicates that the specified job store already exists."""
    def __init__(self, locator: str) -> None:
        super().__init__(
            "The job store '%s' already exists. Use --restart to resume the workflow, or remove "
            "the job store with 'toil clean' to start the workflow from scratch." % locator)
