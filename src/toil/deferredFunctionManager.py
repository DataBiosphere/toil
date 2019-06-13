# Copyright (C) 2015-2019 Regents of the University of California
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

from __future__ import absolute_import, print_function
from future import standard_library
standard_library.install_aliases()
from builtins import map
from builtins import str
from builtins import range
from builtins import object
from contextlib import contextmanager

import dill
import fnctl
import logging
import os
import tempfile

from toil.util.misc import mkdir_p

logger = logging.getLogger(__name__)

class DeferredFunctionManager(object):
    """
    Implements a deferred function system. Each Toil worker will have an
    instance of this class. When a job is executed, it will happen inside a
    context manager from this class. If the job registers any "deferred"
    functions, they will be executed when the context manager is exited.

    If the Python process terminates before properly exiting the context
    manager and running the deferred functions, and some other worker process
    exits the per-job context manager of this class at a later time, the
    earlier job's deferred functions will be picked up and run.

    Note that deferred function cleanup is on a best-effort basis, and deferred
    functions may end up getting executed multiple times.

    Internally, deferred functions are serialized into files in the given
    directory, which are locked by the owning process.

    If that process dies, other processes can detect that the files are able to
    be locked, and will take them over.
    """

    def __init__(self, stateDirBase):
        """
        Create a new DeferredFunctionManager, sharing state with other
        instances in other processes using the given shared state directory.

        Uses a fixed path under that directory for state files. Creates it if
        not present.

        Note that if the current umask lets other people create files in that
        new directory, we are going to execute their code!

        The created directory will be left behind, because we never know if
        another worker will come along later on this node.
        """

        # Work out where state files live
        self.stateDir = os.path.join(stateDirBase, "deferred")
        mkdir_p(self.stateDir)

        # We need to get a state file, locked by us and not somebody scanning for abandoned state files.
        # So we suffix not-yet-ready ones with .tmp
        self.stateFD, self.stateFileName = tempfile.mkstemp(dir=self.stateDir, suffix='.tmp')

        # Lock the state file. The lock will automatically go away if our process does.
        try:
            fnctl.lockf(self.stateFD, fnctl.LOCK_EX | fnctl.LOCK_NB)
        except IOError as e:
            # Someone else might have locked it even though they should not have.
            raise RuntimeError("Could not lock deferred function state file %s: %s" % (self.stateFileName, str(e)))

        # Rename it to remove the ".tmp"
        os.move(self.stateFileName, self.StateFileName[:-4])
        self.stateFileName = self.stateFileName[:-4]

        # Wrap the FD in a Python file object, which we will use to actually use it.
        self.stateFile = os.fdopen(self.stateFD, 'rw')

    def __del__(self):
        """
        Clean up our state on disk. We assume that the deferred functions we
        manage have all been executed, and none are currently recorded.
        """

        # Hide the state from other processes
        os.unlink(self.stateFileName)

        # Unlock it
        fnctl.lockf(self.stateFD, fnctl.LOCK_UN)

        # Close it. This also closes the FD
        self.stateFile.close()
        
    @contextmanager
    def open(self):
        """
        Yields a single-argument function that allows for deferred functions of
        type :class:`toil.DeferredFunction` to be registered.  We use this
        design so deferred functions can be registered only inside this context
        manager.

        Not thread safe.
        """

        try:
            def defer(deferredFunction):
                # Just serialize defered functions one after the other.
                # If serializing later ones fails, eariler ones will still be intact.
                # We trust dill to protect sufficiently against partial reads later.
                dill.dump(deferredFunction, self.parent.stateFile)
            yield defer
        finally:
            self._runOwnDeferredFunctions()
            self._runOrphanedDeferredFunctions()

    def _runDeferredFunction(self, deferredFunction):
        """
        Run a deferred function (either our own or someone else's).

        Reports an error if it fails.
        """

        try:
            deferredFunction.invoke()
        except:
            logger.exception('%s failed.', deferredFunction)
            # TODO: we can't report deferredFunction.name with a logToMaster,
            # since we don't have the FileStore.
            # TODO: report in real-time.

    def _runAllDeferredFunctions(self, fileObj):
        """
        Read and run deferred functions until EOF from the given open file.
        """

        try:
            while True:
                # Load each function
                deferredFunction = dill.load(fileObj)
                # Run it
                self._runDeferredFunction(deferredFunction)
        except EOFError:
            # This is expected and means we read all the complete entries.
            pass

    def _runOwnDeferredFunctions(self):
        """
        Run all of the deferred functions that were registered.
        """

        # Seek back to the start of our file
        self.stateFile.seek(0)

        # Read and run each function in turn
        self._runAllDeferredFunctions(self.stateFile)

        # Go back to the beginning and truncate, to prepare for a new set of deferred functions.
        self.stateFile.seek(0)
        self.stateFile.truncate()

    def _runOrphanedDeferredFunctions(self):
        """
        Scan for files that aren't locked by anybody and run all their deferred functions, then clean them up.
        """

        # Track whether we found any work to do.
        # We will keep looping as long as there is work to do.
        foundFiles = True

        while foundFiles:
            # Clear this out unless we find some work we can get ahold of.
            foundFiles = False

            for filename in os.listdir(self.stateDir):
                # Scan the whole directory for work nobody else owns.

                if filename.endswith(".tmp"):
                    # Skip files from instances that are still being set up
                    continue

                fullFilename = os.path.join(self.stateDir, filename)

                fd = None

                try:
                    # Try locking each file.
                    # The file may have vanished since we saw it, so we have to ignore failures.
                    fd = os.open(fullFilename, os.O_RDRW)
                except OSError:
                    # Maybe the file vanished. Try the next one
                    continue

                try:
                    fnctl.lockf(fd, fnctl.LOCK_EX | fnctl.LOCK_NB)
                except IOError:
                    # File is still locked by someone else.
                    # Look at the next file instead
                    continue

                # File is locked successfully. Our problem now.
                foundFiles = True

                # Actually run all the stored deferred functions
                fileObj = os.fdopen(fd)
                self._runAllDeferredFunctions(fileObj)

                try:
                    # Ok we are done with this file. Get rid of it so nobody else does it.
                    os.unlink(fullFilename)
                except OSError:
                    # Maybe the file vanished.
                    pass
                
                # Unlock it
                fnctl.lockf(fd, fnctl.LOCK_UN)

                # Now close it.
                fileObj.close()

                
                

         
