# Copyright (C) 2015-2016 Regents of the University of California
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

from __future__ import absolute_import

import gzip
import json
import logging
import os
import time
from threading import Thread, Event

from bd2k.util.expando import Expando
from toil.lib.bioio import getTotalCpuTime

logger = logging.getLogger( __name__ )


class StatsAndLogging( object ):
    """
    Class manages a thread that aggregates statistics and logging information on a toil run.
    """

    def __init__(self, jobStore, config):
        self._stop = Event()
        self._worker = Thread(target=self.statsAndLoggingAggregator,
                              args=(jobStore, self._stop, config))

    def start(self):
        """
        Start the stats and logging thread.
        """
        self._worker.start()

    @classmethod
    def logWithFormatting(cls, jobStoreID, jobLogs, method=logger.debug, message=None):
        if message is not None:
            method(message)
        for line in jobLogs:
            method('%s    %s', jobStoreID, line.rstrip('\n'))

    @classmethod
    def writeLogFiles(cls, jobNames, jobLogList, config):
        def createName(logPath, jobName, logExtension):
            logName = jobName.replace('-', '--')
            logName = logName.replace('/', '-')
            logName = logName.replace(' ', '_')
            logName = logName.replace("'", '')
            logName = logName.replace('"', '')
            counter = 0
            while True:
                suffix = str(counter).zfill(3) + logExtension
                fullName = os.path.join(logPath, logName + suffix)
                if not os.path.exists(fullName):
                    return fullName
                counter += 1

        mainFileName = jobNames[0]
        extension = '.log'

        assert not (config.writeLogs and config.writeLogsGzip), \
            "Cannot use both --writeLogs and --writeLogsGzip at the same time."

        if config.writeLogs:
            path = config.writeLogs
            writeFn = open
        elif config.writeLogsGzip:
            path = config.writeLogsGzip
            writeFn = gzip.open
            extension += '.gz'
        else:
            # we don't have anywhere to write the logs, return now
            return

        fullName = createName(path, mainFileName, extension)
        with writeFn(fullName, 'w') as f:
            f.writelines(l + '\n' for l in jobLogList)
        for alternateName in jobNames[1:]:
            # There are chained jobs in this output - indicate this with a symlink
            # of the job's name to this file
            name = createName(path, alternateName, extension)
            os.symlink(os.path.relpath(fullName, path), name)

    @classmethod
    def statsAndLoggingAggregator(cls, jobStore, stop, config):
        """
        The following function is used for collating stats/reporting log messages from the workers.
        Works inside of a thread, collates as long as the stop flag is not True.
        """
        #  Overall timing
        startTime = time.time()
        startClock = getTotalCpuTime()

        def callback(fileHandle):
            stats = json.load(fileHandle, object_hook=Expando)
            try:
                logs = stats.workers.logsToMaster
            except AttributeError:
                # To be expected if there were no calls to logToMaster()
                pass
            else:
                for message in logs:
                    logger.log(int(message.level),
                               'Got message from job at time %s: %s',
                               time.strftime('%m-%d-%Y %H:%M:%S'), message.text)
            try:
                logs = stats.logs
            except AttributeError:
                pass
            else:
                # we may have multiple jobs per worker
                jobNames = logs.names
                messages = logs.messages
                cls.logWithFormatting(jobNames[0], messages,
                                      message='Received Toil worker log. Disable debug level logging to hide this output')
                cls.writeLogFiles(jobNames, messages, config=config)

        while True:
            # This is a indirect way of getting a message to the thread to exit
            if stop.is_set():
                jobStore.readStatsAndLogging(callback)
                break
            if jobStore.readStatsAndLogging(callback) == 0:
                time.sleep(0.5)  # Avoid cycling too fast

        # Finish the stats file
        text = json.dumps(dict(total_time=str(time.time() - startTime),
                               total_clock=str(getTotalCpuTime() - startClock)))
        jobStore.writeStatsAndLogging(text)

    def check(self):
        """
        Check on the stats and logging aggregator.
        :raise RuntimeError: If the underlying thread has quit.
        """
        if not self._worker.is_alive():
            raise RuntimeError("Stats and logging thread has quit")

    def shutdown(self):
        """
        Finish up the stats/logging aggregation thread
        """
        logger.info('Waiting for stats and logging collator thread to finish ...')
        startTime = time.time()
        self._stop.set()
        self._worker.join()
        logger.info('... finished collating stats and logs. Took %s seconds', time.time() - startTime)
        # in addition to cleaning on exceptions, onError should clean if there are any failed jobs