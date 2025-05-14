# Copyright (C) 2015-2022 Regents of the University of California
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
import os
from pathlib import Path
from threading import Thread, current_thread
from typing import NoReturn

from toil.batchSystems.abstractBatchSystem import BatchJobExitReason
from toil.bus import (
    JobCompletedMessage,
    JobIssuedMessage,
    MessageBus,
    replay_message_bus,
)
from toil.common import Toil
from toil.exceptions import FailedJobsException
from toil.job import Job

logger = logging.getLogger(__name__)


class TestMessageBus:

    def test_enum_ints_in_file(self, tmp_path: Path) -> None:
        """
        Make sure writing bus messages to files works with enums.
        """
        bus_file = tmp_path / "bus"

        bus = MessageBus()
        # Connect the handler and hold the result to protect it from GC
        handler_to_keep_alive = bus.connect_output_file(bus_file)
        logger.debug("Connected bus to file %s", bus_file)
        bus.publish(JobCompletedMessage("blah", "blah", BatchJobExitReason.MEMLIMIT))
        # Make sure stuff goes away in the right order
        del handler_to_keep_alive
        del bus

        for line in open(bus_file):
            logger.debug("Bus line: %s", line)

        replay = replay_message_bus(bus_file)
        assert len(replay) > 0

    def test_cross_thread_messaging(self) -> None:
        """
        Make sure message bus works across threads.
        """

        main_thread = current_thread()

        bus = MessageBus()

        message_count = 0

        # We will count the messages going to the handler
        def handler(received: JobIssuedMessage) -> None:
            """
            Handle a message in the main thread.
            """
            # Message should always arrive in the main thread.
            nonlocal message_count
            logger.debug("Got message: %s", received)
            assert current_thread() == main_thread
            message_count += 1

        bus.subscribe(JobIssuedMessage, handler)

        # And we will collect them in an inbox
        box = bus.connect([JobIssuedMessage])

        bus.publish(JobIssuedMessage("FromMainThread", "", 0))

        def send_thread_message() -> None:
            """
            Publish a message from some other thread.
            """
            bus.publish(JobIssuedMessage("FromOtherThread", "", 1))

        other_threads = []
        for _ in range(10):
            # Make a bunch of threads to send messages
            other_threads.append(Thread(target=send_thread_message))
        for t in other_threads:
            # Start all the threads
            t.start()
        for t in other_threads:
            # Wait for them to finish
            t.join()

        # We should ge tone message per thread, plus our own
        assert box.count(JobIssuedMessage) == 11
        # And having polled for those, our handler should have run
        assert message_count == 11

    def test_restart_without_bus_path(self, tmp_path: Path) -> None:
        """
        Test the ability to restart a workflow when the message bus path used
        by the previous attempt is gone.
        """
        temp_dir = tmp_path / "tempDir"
        temp_dir.mkdir()
        job_store = tmp_path / "jobstore"

        bus_holder_dir = temp_dir / "bus_holder"
        bus_holder_dir.mkdir()

        start_options = Job.Runner.getDefaultOptions(job_store)
        start_options.logLevel = "DEBUG"
        start_options.retryCount = 0
        start_options.clean = "never"
        start_options.write_messages = str(bus_holder_dir / "messagebus.txt")

        root = Job.wrapJobFn(failing_job_fn)

        try:
            with Toil(start_options) as toil:
                # Run once and observe a failed job
                toil.start(root)
        except FailedJobsException:
            pass

        logger.info("First attempt successfully failed, removing message bus log")

        # Get rid of the bus
        os.unlink(start_options.write_messages)
        bus_holder_dir.rmdir()

        logger.info("Making second attempt")

        # Set up options without a specific bus path
        restart_options = Job.Runner.getDefaultOptions(job_store)
        restart_options.logLevel = "DEBUG"
        restart_options.retryCount = 0
        restart_options.clean = "never"
        restart_options.restart = True

        try:
            with Toil(restart_options) as toil:
                # Run again and observe a failed job (and not a failure to start)
                toil.restart()
        except FailedJobsException:
            pass

        logger.info("Second attempt successfully failed")


def failing_job_fn(job: Job) -> NoReturn:
    """
    This function is guaranteed to fail.
    """
    raise RuntimeError("Job attempted to run but failed")
