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
import logging
from types import TracebackType
from typing import Any, ContextManager, List, Optional, Type

from toil.batchSystems.abstractBatchSystem import (BatchSystemSupport,
                                                   WorkerCleanupInfo)
from toil.batchSystems.local_support import BatchSystemLocalSupport
from toil.common import Config, Toil
from toil.lib.threading import LastProcessStandingArena

logger = logging.getLogger(__name__)

class BatchSystemCleanupSupport(BatchSystemLocalSupport):
    """
    Adds cleanup support when the last running job leaves a node, for batch
    systems that can't provide it using the backing scheduler.
    """

    @classmethod
    def supportsWorkerCleanup(cls) -> bool:
        return True

    def getWorkerContexts(self) -> List[ContextManager[Any]]:
        # Tell worker to register for and invoke cleanup

        # Create a context manager that has a copy of our cleanup info
        context = WorkerCleanupContext(self.workerCleanupInfo)

        # Send it along so the worker works inside of it
        contexts = super().getWorkerContexts()
        contexts.append(context)
        return contexts

    def __init__(self, config: Config, maxCores: float, maxMemory: int, maxDisk: int) -> None:
        super().__init__(config, maxCores, maxMemory, maxDisk)

class WorkerCleanupContext:
    """
    Context manager used by :class:`BatchSystemCleanupSupport` to implement
    cleanup on a node after the last worker is done working.

    Gets wrapped around the worker's work.
    """

    def __init__(self, workerCleanupInfo: WorkerCleanupInfo) -> None:
        """
        Wrap the given workerCleanupInfo in a context manager.

        :param workerCleanupInfo: Info to use to clean up the worker if we are
                                  the last to exit the context manager.
        """


        self.workerCleanupInfo = workerCleanupInfo
        # Don't set self.arena or MyPy will be upset that sometimes it doesn't have the right type.

    def __enter__(self) -> None:
        # Set up an arena so we know who is the last worker to leave
        self.arena = LastProcessStandingArena(Toil.get_toil_coordination_dir(self.workerCleanupInfo.work_dir, self.workerCleanupInfo.coordination_dir),
                                              self.workerCleanupInfo.workflow_id + '-cleanup')
        logger.debug('Entering cleanup arena')
        self.arena.enter()
        logger.debug('Cleanup arena entered')

    # This is exactly the signature MyPy demands.
    # Also, it demands we not say we can return a bool if we return False
    # always, because it can be smarter about reachability if it knows what
    # context managers never eat exceptions. So it decides any context manager
    # that is always falsey but claims to return a bool is an error.
    def __exit__(self, type: Optional[Type[BaseException]], value: Optional[BaseException], traceback: Optional[TracebackType]) -> None:
        logger.debug('Leaving cleanup arena')
        for _ in self.arena.leave():
            # We are the last concurrent worker to finish.
            # Do batch system cleanup.
            logger.debug('Cleaning up worker')
            BatchSystemSupport.workerCleanup(self.workerCleanupInfo)
            # Now the coordination_dir is allowed to no longer exist on the node.
        logger.debug('Cleanup arena left')


