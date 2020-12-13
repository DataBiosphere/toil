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
import gzip
import json
import logging
import os
import time
from argparse import ArgumentParser
from threading import Event, Thread

from toil.lib.expando import Expando
from toil.lib.resources import get_total_cpu_time

logger = logging.getLogger(__name__)
root_logger = logging.getLogger()
toil_logger = logging.getLogger('toil')

DEFAULT_LOGLEVEL = logging.INFO
__loggingFiles = []


class StatsAndLogging:
    """A thread to aggregate statistics and logging."""
    def __init__(self, jobStore, config):
        self._stop = Event()
        self._worker = Thread(target=self.statsAndLoggingAggregator,
                              args=(jobStore, self._stop, config),
                              daemon=True)

    def start(self):
        """Start the stats and logging thread."""
        self._worker.start()
        
    @classmethod
    def formatLogStream(cls, stream, job_name=None):
        """
        Given a stream of text or bytes, and the job name, job itself, or some
        other optional stringifyable identity info for the job, return a big
        text string with the formatted job log, suitable for printing for the
        user.

        We don't want to prefix every line of the job's log with our own
        logging info, or we get prefixes wider than any reasonable terminal
        and longer than the messages.
        """
        if isinstance(job_name, bytes):
            job_name = job_name.decode('utf-8', errors='replace')

        lines = [f'Log from job "{job_name}" follows:', '=========>']

        for line in stream:
            if isinstance(line, bytes):
                line = line.decode('utf-8', errors='replace')
            lines.append('\t' + line.rstrip('\n'))
            
        lines.append('<=========')
        
        return '\n'.join(lines)
    

    @classmethod
    def logWithFormatting(cls, jobStoreID, jobLogs, method=logger.debug, message=None):
        if message is not None:
            method(message)

        # Format and log the logs, identifying the job with its job store ID.
        method(cls.formatLogStream(jobLogs, jobStoreID))
        
    @classmethod
    def writeLogFiles(cls, jobNames, jobLogList, config, failed=False):
        def createName(logPath, jobName, logExtension, failed=False):
            logName = jobName.replace('-', '--')
            logName = logName.replace('/', '-')
            logName = logName.replace(' ', '_')
            logName = logName.replace("'", '')
            logName = logName.replace('"', '')
            # Add a "failed_" prefix to logs from failed jobs.
            logName = ('failed_' if failed else '') + logName
            counter = 0
            while True:
                suffix = str(counter).zfill(3) + logExtension
                fullName = os.path.join(logPath, logName + suffix)
                #  The maximum file name size in the default HFS+ file system is 255 UTF-16 encoding units, so basically 255 characters
                if len(fullName) >= 255:
                    return fullName[:(255-len(suffix))] + suffix
                if not os.path.exists(fullName):
                    return fullName
                counter += 1

        mainFileName = jobNames[0]
        extension = '.log'

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

        fullName = createName(path, mainFileName, extension, failed)
        with writeFn(fullName, 'wb') as f:
            for l in jobLogList:
                try:
                    l = l.decode('utf-8')
                except AttributeError:
                    pass
                if not l.endswith('\n'):
                    l += '\n'
                f.write(l.encode('utf-8'))
        for alternateName in jobNames[1:]:
            # There are chained jobs in this output - indicate this with a symlink
            # of the job's name to this file
            name = createName(path, alternateName, extension, failed)
            if not os.path.exists(name):
                os.symlink(os.path.relpath(fullName, path), name)

    @classmethod
    def statsAndLoggingAggregator(cls, jobStore, stop, config):
        """
        The following function is used for collating stats/reporting log messages from the workers.
        Works inside of a thread, collates as long as the stop flag is not True.
        """
        #  Overall timing
        startTime = time.time()
        startClock = get_total_cpu_time()

        def callback(fileHandle):
            statsStr = fileHandle.read()
            if not isinstance(statsStr, str):
                statsStr = statsStr.decode()
            stats = json.loads(statsStr, object_hook=Expando)
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
                               total_clock=str(get_total_cpu_time() - startClock)), ensure_ascii=True)
        jobStore.writeStatsAndLogging(text)

    def check(self):
        """
        Check on the stats and logging aggregator.
        :raise RuntimeError: If the underlying thread has quit.
        """
        if not self._worker.is_alive():
            raise RuntimeError("Stats and logging thread has quit")

    def shutdown(self):
        """Finish up the stats/logging aggregation thread."""
        logger.debug('Waiting for stats and logging collator thread to finish ...')
        startTime = time.time()
        self._stop.set()
        self._worker.join()
        logger.debug('... finished collating stats and logs. Took %s seconds', time.time() - startTime)
        # in addition to cleaning on exceptions, onError should clean if there are any failed jobs


def set_log_level(level, set_logger=None):
    """Sets the root logger level to a given string level (like "INFO")."""
    level = "CRITICAL" if level.upper() == "OFF" else level.upper()
    set_logger = set_logger if set_logger else root_logger
    set_logger.setLevel(level)
    # There are quite a few cases where we expect AWS requests to fail, but it seems
    # that boto handles these by logging the error *and* raising an exception. We
    # don't want to confuse the user with those error messages.
    logging.getLogger('boto').setLevel(logging.CRITICAL)


def add_logging_options(parser: ArgumentParser):
    """Add logging options to set the global log level."""
    group = parser.add_argument_group("Logging Options")
    default_loglevel = logging.getLevelName(DEFAULT_LOGLEVEL)

    levels = ['Critical', 'Error', 'Warning', 'Debug', 'Info']
    for level in levels:
        group.add_argument(f"--log{level}", dest="logLevel", default=default_loglevel, action="store_const",
                           const=level, help=f"Turn on loglevel {level}.  Default: {default_loglevel}.")

    levels += [l.lower() for l in levels] + [l.upper() for l in levels]
    group.add_argument("--logOff", dest="logLevel", default=default_loglevel,
                       action="store_const", const="CRITICAL", help="Same as --logCRITICAL.")
    group.add_argument("--logLevel", dest="logLevel", default=default_loglevel, choices=levels,
                       help=f"Set the log level. Default: {default_loglevel}.  Options: {levels}.")
    group.add_argument("--logFile", dest="logFile", help="File to log in.")
    group.add_argument("--rotatingLogging", dest="logRotating", action="store_true", default=False,
                       help="Turn on rotating logging, which prevents log files from getting too big.")


def configure_root_logger():
    """
    Set up the root logger with handlers and formatting.

    Should be called before any entry point tries to log anything,
    to ensure consistent formatting.
    """
    logging.basicConfig(format='[%(asctime)s] [%(threadName)-10s] [%(levelname).1s] [%(name)s] %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S%z')
    root_logger.setLevel(DEFAULT_LOGLEVEL)


def log_to_file(log_file, log_rotation):
    if log_file and log_file not in __loggingFiles:
        logger.debug(f"Logging to file '{log_file}'.")
        __loggingFiles.append(log_file)
        if log_rotation:
            handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=1000000, backupCount=1)
        else:
            handler = logging.FileHandler(log_file)
        root_logger.addHandler(handler)


def set_logging_from_options(options):
    configure_root_logger()
    options.logLevel = options.logLevel or logging.getLevelName(root_logger.getEffectiveLevel())
    set_log_level(options.logLevel)
    logger.debug(f"Root logger is at level '{logging.getLevelName(root_logger.getEffectiveLevel())}', "
                 f"'toil' logger at level '{logging.getLevelName(toil_logger.getEffectiveLevel())}'.")

    # start logging to log file if specified
    log_to_file(options.logFile, options.logRotating)
