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
from abc import abstractmethod, ABCMeta
from contextlib import contextmanager
import logging
import os
from typing import Any, Union

from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore


logger = logging.getLogger(__name__)

class AccessReportingWrapper:
    """
    Wrapper class that records access to files via a
    :class:`toil.fileStores.abstractFileStore.AbstractFileStore`
    implementation, and reports the accessed files should an error occur while
    the file store is open is open.
    """
    
    def __init__(self, wrapped: AbstractFileStore):
        """
        Wrap the given file store instance.
        """
        
        # Set these in self.__dict__ manually, to bypass
        # __getattr__/__setattr__.
        self.__dict__['_wrappedfileStore'] = wrapped
        self.__dict__['_accessLog'] = []
    
    def __getattr__(self, name: str) -> Any:
        """
        Delegate queries for attributes not defined here to the wrapped class.
        """
        if name in self.__dict__:
            return self.__dict__[name]
        else:
            return getattr(self.__dict__['_wrappedfileStore'], name)
        
    def __setattr__(self, name: str, value: Any):
        """
        Delegate sets for attributes not defined here to the wrapped class.
        """
        if name in self.__dict__:
            self.__dict__[name] = value
        else:
            setattr(self.__dict__['_wrappedfileStore'], name, value)
        
    @contextmanager
    def open(self, *args):
        """
        Hook the open() context manager to report accesses on an error.
        """
        with self.__dict__['_wrappedfileStore'].open(*args):
            failed = True
            try:
                yield
                failed = False
            finally:
                # Do a finally instead of an except/raise because we don't want
                # to appear as "another exception occurred" in the stack trace.
                if failed:
                    self._dumpAccessLogs()
                
    def readGlobalFile(self, fileStoreID: Union[FileID, str], *args, **kwargs) -> str:
        """
        Hook reads to disk so they log.
        """
        # Download the file
        download_path = self._wrappedfileStore.readGlobalFile(fileStoreID, *args, **kwargs)
        # Log the access
        self.__dict__['_accessLog'].append((fileStoreID, download_path))
        # Return the path it was downloaded to
        return download_path
        
    def readGlobalFileStream(self, fileStoreID: Union[FileID, str], *args, **kwargs):
        """
        Hook reads of streams so they log.
        """
        # Grab the stream
        stream = self._wrappedfileStore.readGlobalFileStream(fileStoreID, *args, **kwargs)
        # Log the access
        self.__dict__['_accessLog'].append((fileStoreID,))
        # Return the stream
        return stream
                
    def _dumpAccessLogs(self):
        """
        When something goes wrong, log a report of the files that were accessed while the file store was open.
        """
        
        if len(self.__dict__['_accessLog']) > 0:
            logger.warning('Failed job accessed files:')
        
            for item in self.__dict__['_accessLog']:
                # For each access record
                if len(item) == 2:
                    # If it has a name, dump wit the name
                    logger.warning('Downloaded file \'%s\' to path \'%s\'', *item)
                else:
                    # Otherwise dump without the name
                    logger.warning('Streamed file \'%s\'', *item)
        
    

