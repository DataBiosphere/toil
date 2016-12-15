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

import json
import logging
import time
from threading import Thread, Event
from bd2k.util.expando import Expando
from toil.lib.bioio import getTotalCpuTime

logger = logging.getLogger( __name__ )

class StatsAndLogging( object ):
    """
    Class manages a thread that aggregates statistics and logging information on a toil run.
    """

    def __init__(self, jobStore):
        self._stop = Event()
        self._worker = Thread(target=self.statsAndLoggingAggregator,
                              args=(jobStore, self._stop))
    
    def start(self):
        """
        Start the stats and logging thread.
        """
        self._worker.start()

    @staticmethod
    def statsAndLoggingAggregator(jobStore, stop):
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
                def logWithFormatting(jobStoreID, jobLogs):
                    logFormat = '\n%s    ' % jobStoreID
                    logger.debug('Received Toil worker log. Disable debug level '
                                 'logging to hide this output\n%s', logFormat.join(jobLogs))
                # we may have multiple jobs per worker
                # logs[0] is guaranteed to exist in this branch
                currentJobStoreID = logs[0].jobStoreID
                jobLogs = []
                for log in logs:
                    jobStoreID = log.jobStoreID
                    if jobStoreID == currentJobStoreID:
                        # aggregate all the job's logs into 1 list
                        jobLogs.append(log.text)
                    else:
                        # we have reached the next job, output the aggregated logs and continue
                        logWithFormatting(currentJobStoreID, jobLogs)
                        jobLogs = []
                        currentJobStoreID = jobStoreID
                # output the last job's logs
                logWithFormatting(currentJobStoreID, jobLogs)

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