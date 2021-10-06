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
import fcntl
import logging
import os
import shutil
import tempfile
from collections import namedtuple
from contextlib import contextmanager

import dill

from toil.realtimeLogger import RealtimeLogger
from toil.resource import ModuleDescriptor

logger = logging.getLogger(__name__)


class DeferredFunction(namedtuple('DeferredFunction', 'function args kwargs name module')):
    """
    >>> from collections import defaultdict
    >>> df = DeferredFunction.create(defaultdict, None, {'x':1}, y=2)
    >>> df
    DeferredFunction(defaultdict, ...)
    >>> df.invoke() == defaultdict(None, x=1, y=2)
    True
    """
    @classmethod
    def create(cls, function, *args, **kwargs):
        """
        Capture the given callable and arguments as an instance of this class.

        :param callable function: The deferred action to take in the form of a function
        :param tuple args: Non-keyword arguments to the function
        :param dict kwargs: Keyword arguments to the function
        """
        # The general principle is to deserialize as late as possible, i.e. when the function is
        # to be invoked, as that will avoid redundantly deserializing deferred functions for
        # concurrently running jobs when the cache state is loaded from disk. By implication we
        # should serialize as early as possible. We need to serialize the function as well as its
        # arguments.
        return cls(*list(map(dill.dumps, (function, args, kwargs))),
                   name=function.__name__,
                   module=ModuleDescriptor.forModule(function.__module__).globalize())

    def invoke(self):
        """
        Invoke the captured function with the captured arguments.
        """
        logger.debug('Running deferred function %s.', self)
        self.module.makeLoadable()
        function, args, kwargs = list(map(dill.loads, (self.function, self.args, self.kwargs)))
        return function(*args, **kwargs)

    def __str__(self):
        return '%s(%s, ...)' % (self.__class__.__name__, self.name)

    __repr__ = __str__


class DeferredFunctionManager(object):
    """
    Implements a deferred function system. Each Toil worker will have an
    instance of this class. When a job is executed, it will happen inside a
    context manager from this class. If the job registers any "deferred"
    functions, they will be executed when the context manager is exited.

    If the Python process terminates before properly exiting the context
    manager and running the deferred functions, and some other worker process
    enters or exits the per-job context manager of this class at a later time,
    or when the DeferredFunctionManager is shut down on the worker, the earlier
    job's deferred functions will be picked up and run.

    Note that deferred function cleanup is on a best-effort basis, and deferred
    functions may end up getting executed multiple times.

    Internally, deferred functions are serialized into files in the given
    directory, which are locked by the owning process.

    If that process dies, other processes can detect that the files are able to
    be locked, and will take them over.
    """

    # Define what directory the state directory should actaully be, under the base
    STATE_DIR_STEM = 'deferred'
    # Have a prefix to distinguish our deferred functions from e.g. NFS
    # "silly rename" files, or other garbage that people put in our
    # directory
    PREFIX = 'func'
    # And a suffix to distinguish in-progress from completed files
    WIP_SUFFIX = '.tmp'

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
        self.stateDir = os.path.join(stateDirBase, self.STATE_DIR_STEM)
        os.makedirs(self.stateDir, exist_ok=True)

        # We need to get a state file, locked by us and not somebody scanning for abandoned state files.
        # So we suffix not-yet-ready ones with our suffix
        self.stateFD, self.stateFileName = tempfile.mkstemp(dir=self.stateDir,
                                                            prefix=self.PREFIX,
                                                            suffix=self.WIP_SUFFIX)

        # Lock the state file. The lock will automatically go away if our process does.
        try:
            fcntl.lockf(self.stateFD, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as e:
            # Someone else might have locked it even though they should not have.
            raise RuntimeError("Could not lock deferred function state file %s: %s" % (self.stateFileName, str(e)))

        # Rename it to remove the suffix
        os.rename(self.stateFileName, self.stateFileName[:-len(self.WIP_SUFFIX)])
        self.stateFileName = self.stateFileName[:-len(self.WIP_SUFFIX)]

        # Wrap the FD in a Python file object, which we will use to actually use it.
        # Problem: we can't be readable and writable at the same time. So we need two file objects.
        self.stateFileOut = os.fdopen(self.stateFD, 'wb')
        self.stateFileIn = open(self.stateFileName, 'rb')

        logger.debug("Running for file %s" % self.stateFileName)

    def __del__(self):
        """
        Clean up our state on disk. We assume that the deferred functions we
        manage have all been executed, and none are currently recorded.
        """

        logger.debug("Deleting %s" % self.stateFileName)

        # Hide the state from other processes
        if os.path.exists(self.stateFileName):
            os.unlink(self.stateFileName)

        # Unlock it
        fcntl.lockf(self.stateFD, fcntl.LOCK_UN)

        # Don't bother with close, destroying will close and it seems to maybe
        # have been GC'd already anyway.

    @contextmanager
    def open(self):
        """
        Yields a single-argument function that allows for deferred functions of
        type :class:`toil.DeferredFunction` to be registered.  We use this
        design so deferred functions can be registered only inside this context
        manager.

        Not thread safe.
        """

        # Clean up other jobs before we run, so our job has a nice clean node
        self._runOrphanedDeferredFunctions()

        try:
            def defer(deferredFunction):
                # Just serialize defered functions one after the other.
                # If serializing later ones fails, eariler ones will still be intact.
                # We trust dill to protect sufficiently against partial reads later.
                logger.debug("Deferring function %s" % repr(deferredFunction))
                dill.dump(deferredFunction, self.stateFileOut)
                # Flush before returning so we can guarantee the write is on disk if we die.
                self.stateFileOut.flush()

            logger.debug("Running job")
            yield defer
        finally:
            self._runOwnDeferredFunctions()
            self._runOrphanedDeferredFunctions()

    @classmethod
    def cleanupWorker(cls, stateDirBase):
        """
        Called by the batch system when it shuts down the node, after all
        workers are done, if the batch system supports worker cleanup. Checks
        once more for orphaned deferred functions and runs them.
        """

        logger.debug("Cleaning up deferred functions system")

        # Open up
        cleaner = cls(stateDirBase)
        # Do the final round of cleanup
        cleaner._runOrphanedDeferredFunctions()

        # Close all the files in there.
        del cleaner

        # Clean up the directory we have been using.
        # It might not be empty if .tmp files escaped: nobody can tell they
        # aren't just waiting to be locked.
        shutil.rmtree(os.path.join(stateDirBase, cls.STATE_DIR_STEM))



    def _runDeferredFunction(self, deferredFunction):
        """
        Run a deferred function (either our own or someone else's).

        Reports an error if it fails.
        """

        try:
            deferredFunction.invoke()
        except Exception as err:
            # Report this in real time, if enabled. Otherwise the only place it ends up is the worker log.
            RealtimeLogger.error("Failed to run deferred function %s: %s", repr(deferredFunction), str(err))
        except:
            RealtimeLogger.error("Failed to run deferred function %s", repr(deferredFunction))

    def _runAllDeferredFunctions(self, fileObj):
        """
        Read and run deferred functions until EOF from the given open file.
        """

        try:
            while True:
                # Load each function
                deferredFunction = dill.load(fileObj)
                logger.debug("Loaded deferred function %s" % repr(deferredFunction))
                # Run it
                self._runDeferredFunction(deferredFunction)
        except EOFError as e:
            # This is expected and means we read all the complete entries.
            logger.debug("Out of deferred functions!")

    def _runOwnDeferredFunctions(self):
        """
        Run all of the deferred functions that were registered.
        """

        logger.debug("Running own deferred functions")

        # Seek back to the start of our file
        self.stateFileIn.seek(0)

        # Read and run each function in turn
        self._runAllDeferredFunctions(self.stateFileIn)

        # Go back to the beginning and truncate, to prepare for a new set of deferred functions.
        self.stateFileIn.seek(0)
        self.stateFileOut.seek(0)
        self.stateFileOut.truncate()

    def _runOrphanedDeferredFunctions(self):
        """
        Scan for files that aren't locked by anybody and run all their deferred functions, then clean them up.
        """

        logger.debug("Running orphaned deferred functions")

        # Track whether we found any work to do.
        # We will keep looping as long as there is work to do.
        foundFiles = True

        while foundFiles:
            # Clear this out unless we find some work we can get ahold of.
            foundFiles = False

            for filename in os.listdir(self.stateDir):
                # Scan the whole directory for work nobody else owns.

                if filename.endswith(self.WIP_SUFFIX):
                    # Skip files from instances that are still being set up
                    continue

                if not filename.startswith(self.PREFIX):
                    # Skip NFS deleted files and any other contaminants
                    continue

                fullFilename = os.path.join(self.stateDir, filename)

                if fullFilename == self.stateFileName:
                    # We would be able to lock our own file, and it would appear unowned.
                    # So skip it.
                    continue

                fd = None

                try:
                    # Try locking each file.
                    # The file may have vanished since we saw it, so we have to ignore failures.
                    # We open in read write mode because the fcntl docs say you
                    # might only be able to exclusively lock files opened for
                    # writing.
                    fd = os.open(fullFilename, os.O_RDWR)
                except OSError:
                    # Maybe the file vanished. Try the next one
                    continue

                try:
                    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except IOError:
                    # File is still locked by someone else.
                    # Look at the next file instead
                    continue

                logger.debug("Locked file %s" % fullFilename)

                # File is locked successfully. Our problem now.
                foundFiles = True

                # Actually run all the stored deferred functions
                fileObj = os.fdopen(fd, 'rb')
                self._runAllDeferredFunctions(fileObj)

                try:
                    # Ok we are done with this file. Get rid of it so nobody else does it.
                    os.unlink(fullFilename)
                except OSError:
                    # Maybe the file vanished.
                    pass

                # Unlock it
                fcntl.lockf(fd, fcntl.LOCK_UN)

                # Now close it. This closes the backing file descriptor. See
                # <https://stackoverflow.com/a/24984929>
                fileObj.close()
