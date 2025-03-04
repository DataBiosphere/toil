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
import io
import json
import logging
import os
import time
from argparse import ArgumentParser, Namespace
from logging.handlers import RotatingFileHandler
from threading import Event, Thread
from typing import IO, TYPE_CHECKING, Any, Callable, Optional, Union

from toil.lib.conversions import strtobool
from toil.lib.expando import Expando
from toil.lib.history import HistoryManager
from toil.lib.resources import ResourceMonitor

if TYPE_CHECKING:
    from toil.common import Config
    from toil.jobStores.abstractJobStore import AbstractJobStore

logger = logging.getLogger(__name__)
root_logger = logging.getLogger()
toil_logger = logging.getLogger("toil")

DEFAULT_LOGLEVEL = logging.INFO
__loggingFiles = []

# We have some logging that belongs at a TRACE level, below DEBUG
TRACE = logging.DEBUG - 5

logging.addLevelName(TRACE, "TRACE")


class StatsAndLogging:
    """A thread to aggregate statistics and logging."""

    def __init__(self, jobStore: "AbstractJobStore", config: "Config") -> None:
        self._stop = Event()
        self._worker = Thread(
            target=self.statsAndLoggingAggregator,
            args=(jobStore, self._stop, config),
            daemon=True,
        )

    def start(self) -> None:
        """Start the stats and logging thread."""
        self._worker.start()

    @classmethod
    def formatLogStream(
        cls, stream: Union[IO[str], IO[bytes]], stream_name: str
    ) -> str:
        """
        Given a stream of text or bytes, and the job name, job itself, or some
        other optional stringifyable identity info for the job, return a big
        text string with the formatted job log, suitable for printing for the
        user.

        We don't want to prefix every line of the job's log with our own
        logging info, or we get prefixes wider than any reasonable terminal
        and longer than the messages.

        :param stream: The stream of text or bytes to print for the user.
        """
        lines = [f"{stream_name} follows:", "=========>"]

        for line in stream:
            if isinstance(line, bytes):
                line = line.decode("utf-8", errors="replace")
            lines.append("\t" + line.rstrip("\n"))

        lines.append("<=========")

        return "\n".join(lines)

    @classmethod
    def logWithFormatting(
        cls,
        stream_name: str,
        jobLogs: Union[IO[str], IO[bytes]],
        method: Callable[[str], None] = logger.debug,
        message: Optional[str] = None,
    ) -> None:
        if message is not None:
            method(message)

        # Format and log the logs, identifying the stream with the given name.
        method(cls.formatLogStream(jobLogs, stream_name))

    @classmethod
    def writeLogFiles(
        cls,
        jobNames: list[str],
        jobLogList: list[str],
        config: "Config",
        failed: bool = False,
    ) -> None:
        def createName(
            logPath: str, jobName: str, logExtension: str, failed: bool = False
        ) -> str:
            logName = jobName.replace("-", "--")
            logName = logName.replace("/", "-")
            logName = logName.replace(" ", "_")
            logName = logName.replace("'", "")
            logName = logName.replace('"', "")
            # Add a "failed_" prefix to logs from failed jobs.
            logName = ("failed_" if failed else "") + logName
            counter = 0
            while True:
                suffix = "_" + str(counter).zfill(3) + logExtension
                fullName = os.path.join(logPath, logName + suffix)
                #  The maximum file name size in the default HFS+ file system is 255 UTF-16 encoding units, so basically 255 characters
                if len(fullName) >= 255:
                    return fullName[: (255 - len(suffix))] + suffix
                if not os.path.exists(fullName):
                    return fullName
                counter += 1

        mainFileName = jobNames[0]
        extension = ".log"
        writeFn: Callable[..., Any]
        if config.writeLogs:
            path = config.writeLogs
            writeFn = open
        elif config.writeLogsGzip:
            path = config.writeLogsGzip
            writeFn = gzip.open
            extension += ".gz"
        else:
            # we don't have anywhere to write the logs, return now
            return

        # Make sure the destination exists
        os.makedirs(path, exist_ok=True)

        fullName = createName(path, mainFileName, extension, failed)
        with writeFn(fullName, "wb") as f:
            for l in jobLogList:
                if isinstance(l, bytes):
                    l = l.decode("utf-8")
                if not l.endswith("\n"):
                    l += "\n"
                f.write(l.encode("utf-8"))
        for alternateName in jobNames[1:]:
            # There are chained jobs in this output - indicate this with a symlink
            # of the job's name to this file
            name = createName(path, alternateName, extension, failed)
            if not os.path.exists(name):
                os.symlink(os.path.relpath(fullName, path), name)

    @classmethod
    def statsAndLoggingAggregator(
        cls, jobStore: "AbstractJobStore", stop: Event, config: "Config"
    ) -> None:
        """
        The following function is used for collating stats/reporting log messages from the workers.
        Works inside of a thread, collates as long as the stop flag is not True.
        """

        #  Overall timing
        startTime = time.time()
        startClock = ResourceMonitor.get_total_cpu_time()

        def callback(fileHandle: Union[IO[bytes], IO[str]]) -> None:
            statsStr = fileHandle.read()
            if not isinstance(statsStr, str):
                statsStr = statsStr.decode()
            stats = json.loads(statsStr, object_hook=Expando)
            if not stats:
                return

            try:
                # Handle all the log_to_leader messages
                logs = stats.workers.logs_to_leader
            except AttributeError:
                # To be expected if there were no calls to log_to_leader()
                pass
            else:
                for message in logs:
                    logger.log(
                        int(message.level),
                        "Got message from job at time %s: %s",
                        time.strftime("%m-%d-%Y %H:%M:%S"),
                        message.text,
                    )

            try:
                # Handle all the user-level text streams reported back (command output, etc.)
                user_logs = stats.workers.logging_user_streams
            except AttributeError:
                # To be expected if there were no calls to log_user_stream()
                pass
            else:
                for stream_entry in user_logs:
                    try:
                        # Unpack the stream name and text.
                        name, text = stream_entry.name, stream_entry.text
                    except AttributeError:
                        # Doesn't have a user-provided stream name and stream
                        # text, so skip it.
                        continue
                    # Since this is sent as inline text we need to pretend to stream it.
                    # TODO: Save these as individual files if they start to get too big?
                    cls.logWithFormatting(name, io.StringIO(text), logger.info)
                    # Save it as a log file, as if it were a Toil-level job.
                    cls.writeLogFiles([name], [text], config=config)

            try:
                logs = stats.logs
            except AttributeError:
                pass
            else:
                # we may have multiple jobs per worker
                jobNames = logs.names
                messages = logs.messages
                cls.logWithFormatting(
                    f'Log from job "{jobNames[0]}"',
                    messages,
                    message="Received Toil worker log. Disable debug level logging to hide this output",
                )
                cls.writeLogFiles(jobNames, messages, config=config)

            try:
                jobs = stats.jobs
            except AttributeError:
                pass
            else:
                for job in jobs:
                    try:
                        # Here we're talking to job._executor which fills in these stats.

                        # Convince MyPy we won't be sent any job stats without
                        # a workflow ID. You can't set up the job store without
                        # one, but if we're somehow missing one, keep the stats
                        # and logging thread up.
                        assert config.workflowID is not None

                        # TODO: Use better job names!
                        HistoryManager.record_job_attempt(
                            config.workflowID,
                            config.workflowAttemptNumber,
                            job.class_name,
                            job.succeeded == "True",
                            float(job.start),
                            float(job.time),
                            cores=float(job.requested_cores),
                            cpu_seconds=float(job.clock),
                            memory_bytes=int(job.memory) * 1024,
                            disk_bytes=int(job.disk)
                        )
                    except:
                        logger.exception("Could not record job attempt in history!")
                        # Keep going. Don't fail the workflow for history-related issues.

        while True:
            # This is an indirect way of getting a message to the thread to exit
            if stop.is_set():
                jobStore.read_logs(callback)
                break
            if jobStore.read_logs(callback) == 0:
                time.sleep(0.5)  # Avoid cycling too fast

        # Finish the stats file
        text = json.dumps(
            dict(
                total_time=str(time.time() - startTime),
                total_clock=str(ResourceMonitor.get_total_cpu_time() - startClock),
            ),
            ensure_ascii=True,
        )
        jobStore.write_logs(text)

    def check(self) -> None:
        """
        Check on the stats and logging aggregator.
        :raise RuntimeError: If the underlying thread has quit.
        """
        if not self._worker.is_alive():
            raise RuntimeError("Stats and logging thread has quit")

    def shutdown(self) -> None:
        """Finish up the stats/logging aggregation thread."""
        logger.debug("Waiting for stats and logging collator thread to finish ...")
        startTime = time.time()
        self._stop.set()
        self._worker.join()
        logger.debug(
            "... finished collating stats and logs. Took %s seconds",
            time.time() - startTime,
        )
        # in addition to cleaning on exceptions, onError should clean if there are any failed jobs


def set_log_level(level: str, set_logger: Optional[logging.Logger] = None) -> None:
    """Sets the root logger level to a given string level (like "INFO")."""
    level = "CRITICAL" if level.upper() == "OFF" else level.upper()
    set_logger = set_logger if set_logger else root_logger
    set_logger.setLevel(level)
    # Suppress any random loggers introduced by libraries we use.
    # Especially boto/boto3.  They print too much.  -__-
    suppress_exotic_logging(__name__)


def install_log_color(set_logger: Optional[logging.Logger] = None) -> None:
    """Make logs colored."""
    # Most of this code is taken from miniwdl
    # delayed import
    import coloredlogs  # type: ignore[import-untyped]

    level_styles = dict(coloredlogs.DEFAULT_LEVEL_STYLES)
    level_styles["trace"] = dict(level_styles["debug"])

    # TODO: What if these fixed colors aren't right for the terminal background?
    # It might be light or dark or even grey.
    level_styles["trace"]["color"] = 242
    level_styles["debug"]["color"] = 242
    level_styles["notice"] = {"color": "green", "bold": True}
    level_styles["error"]["bold"] = True
    level_styles["warning"]["bold"] = True
    level_styles["info"] = {}
    field_styles = dict(coloredlogs.DEFAULT_FIELD_STYLES)
    field_styles["asctime"] = {"color": "blue"}
    field_styles["name"] = {"color": "magenta"}
    field_styles["levelname"] = {"color": "blue"}
    field_styles["threadName"] = {"color": "blue"}
    fmt = "[%(asctime)s] [%(threadName)s] [%(levelname).1s] [%(name)s] %(message)s"  # mimic old toil logging format
    set_logger = set_logger if set_logger else root_logger
    coloredlogs.install(
        level=set_logger.getEffectiveLevel(),
        logger=set_logger,
        level_styles=level_styles,
        field_styles=field_styles,
        datefmt="%Y-%m-%dT%H:%M:%S%z",  # mimic old toil date format
        fmt=fmt,
    )


def add_logging_options(
    parser: ArgumentParser, default_level: Optional[int] = None
) -> None:
    """
    Add logging options to set the global log level.

    :param default_level: A logging level, like logging.INFO, to use as the default.
    """
    if default_level is None:
        # Make sure we have a log levle to make the default
        default_level = DEFAULT_LOGLEVEL
    default_level_name = logging.getLevelName(default_level)

    group = parser.add_argument_group("Logging Options")

    levels = ["Critical", "Error", "Warning", "Info", "Debug", "Trace"]
    for level in levels:
        group.add_argument(
            f"--log{level}",
            dest="logLevel",
            default=default_level_name,
            action="store_const",
            const=level,
            help=f"Set logging level to {level}. Default: {default_level_name}.",
        )

    levels += [l.lower() for l in levels] + [l.upper() for l in levels]
    group.add_argument(
        "--logOff",
        dest="logLevel",
        default=default_level_name,
        action="store_const",
        const="CRITICAL",
        help="Same as --logCritical.",
    )
    # Maybe deprecate the above in favor of --logLevel?

    group.add_argument(
        "--logLevel",
        dest="logLevel",
        default=default_level_name,
        choices=levels,
        help=f"Set the log level. Default: {default_level_name}.  Options: {levels}.",
    )
    group.add_argument("--logFile", dest="logFile", help="File to log in.")
    group.add_argument(
        "--rotatingLogging",
        dest="logRotating",
        action="store_true",
        default=False,
        help="Turn on rotating logging, which prevents log files from getting too big.",
    )
    group.add_argument(
        "--logColors",
        dest="colored_logs",
        default=True,
        type=strtobool,
        metavar="BOOL",
        help="Enable or disable colored logging. Default: %(default)s",
    )


def configure_root_logger() -> None:
    """
    Set up the root logger with handlers and formatting.

    Should be called before any entry point tries to log anything,
    to ensure consistent formatting.
    """
    logging.basicConfig(
        format="[%(asctime)s] [%(threadName)-10s] [%(levelname).1s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    root_logger.setLevel(DEFAULT_LOGLEVEL)


def log_to_file(log_file: Optional[str], log_rotation: bool) -> None:
    if log_file and log_file not in __loggingFiles:
        logger.debug(f"Logging to file '{log_file}'.")
        __loggingFiles.append(log_file)
        handler: Union[RotatingFileHandler, logging.FileHandler]
        if log_rotation:
            handler = RotatingFileHandler(log_file, maxBytes=1000000, backupCount=1)
        else:
            handler = logging.FileHandler(log_file)
        root_logger.addHandler(handler)


def set_logging_from_options(options: Union["Config", Namespace]) -> None:
    configure_root_logger()
    options.logLevel = options.logLevel or logging.getLevelName(
        root_logger.getEffectiveLevel()
    )
    set_log_level(options.logLevel)
    if options.colored_logs:
        install_log_color()
    logger.debug(
        f"Root logger is at level '{logging.getLevelName(root_logger.getEffectiveLevel())}', "
        f"'toil' logger at level '{logging.getLevelName(toil_logger.getEffectiveLevel())}'."
    )

    # start logging to log file if specified
    log_to_file(options.logFile, options.logRotating)


def suppress_exotic_logging(local_logger: str) -> None:
    """
    Attempts to suppress the loggers of all non-Toil packages by setting them to CRITICAL.

    For example: 'requests_oauthlib', 'google', 'boto', 'websocket', 'oauthlib', etc.

    This will only suppress loggers that have already been instantiated and can be seen in the environment,
    except for the list declared in "always_suppress".

    This is important because some packages, particularly boto3, are not always instantiated yet in the
    environment when this is run, and so we create the logger and set the level preemptively.
    """
    never_suppress = ["toil", "__init__", "__main__", "toil-rt", "cwltool"]
    always_suppress = [
        "boto3",
        "boto",
        "botocore",
    ]  # ensure we suppress even before instantiated

    top_level_loggers: list[str] = []

    # Due to https://stackoverflow.com/questions/61683713
    for pkg_logger in list(logging.Logger.manager.loggerDict.keys()) + always_suppress:
        if pkg_logger != local_logger:
            # many sub-loggers may exist, like "boto.a", "boto.b", "boto.c"; we only want the top_level: "boto"
            top_level_logger = (
                pkg_logger.split(".")[0] if "." in pkg_logger else pkg_logger
            )

            if top_level_logger not in top_level_loggers + never_suppress:
                top_level_loggers.append(top_level_logger)
                logging.getLogger(top_level_logger).setLevel(logging.CRITICAL)
    logger.debug(f"Suppressing the following loggers: {set(top_level_loggers)}")
