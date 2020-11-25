# Copyright (C) 2015-2020 Regents of the University of California
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
import random
import resource
import tempfile
from argparse import ArgumentParser

from toil.version import version

logger = logging.getLogger(__name__)
root_logger = logging.getLogger()
toil_logger = logging.getLogger('toil')

DEFAULT_LOGLEVEL = logging.INFO
__loggingFiles = []


def set_log_level(level, set_logger=None):
    """Sets the root logger level to a given string level (like "INFO")."""
    level = "CRITICAL" if level.upper() == "OFF" else level.upper()
    set_logger = set_logger if set_logger else root_logger
    set_logger.setLevel(level)
    # There are quite a few cases where we expect AWS requests to fail, but it seems
    # that boto handles these by logging the error *and* raising an exception. We
    # don't want to confuse the user with those error messages.
    logging.getLogger('boto').setLevel(logging.CRITICAL)


def add_provisioner_options(parser):
    group = parser.add_argument_group("Provisioner Options")
    group.add_argument('-p', "--provisioner", dest='provisioner', choices=['aws', 'gce'], required=False,
                       default="aws", help="The provisioner for cluster auto-scaling.  "
                                           "AWS and Google are currently supported")
    group.add_argument('-z', '--zone', dest='zone', required=False, default=None,
                       help="The availability zone of the master. This parameter can also be set via the 'TOIL_X_ZONE' "
                            "environment variable, where X is AWS or GCE, or by the ec2_region_name parameter "
                            "in your .boto file, or derived from the instance metadata if using this utility on an "
                            "existing EC2 instance.")
    group.add_argument("clusterName", help="The name that the cluster will be identifiable by.  "
                                           "Must be lowercase and may not contain the '_' character.")


def add_logging_options(parser: ArgumentParser):
    """Add logging options to set the global log level."""
    group = parser.add_argument_group("Logging Options")
    default_loglevel = logging.getLevelName(DEFAULT_LOGLEVEL)

    levels = ('CRITICAL', 'ERROR', 'WARNING', 'DEBUG', 'INFO')
    for level in levels:
        group.add_argument(f"--log{level}", dest="logLevel", default=default_loglevel, action="store_const",
                           const=level, help=f"Turn on loglevel {level}.  Default: {default_loglevel}.")

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

    Should be called (either by itself or via setLoggingFromOptions) before any
    entry point tries to log anything, to ensure consistent formatting.
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


def get_total_cpu_time_and_memory_usage():
    """
    Gives the total cpu time of itself and all its children, and the maximum RSS memory usage of
    itself and its single largest child.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    total_cpu_time = me.ru_utime + me.ru_stime + children.ru_utime + children.ru_stime
    total_memory_usage = me.ru_maxrss + children.ru_maxrss
    return total_cpu_time, total_memory_usage


def get_total_cpu_time():
    """Gives the total cpu time, including the children."""
    me = resource.getrusage(resource.RUSAGE_SELF)
    childs = resource.getrusage(resource.RUSAGE_CHILDREN)
    return me.ru_utime + me.ru_stime + childs.ru_utime + childs.ru_stime


def absSymPath(path):
    """like os.path.abspath except it doesn't dereference symlinks."""
    curr_path = os.getcwd()
    return os.path.normpath(os.path.join(curr_path, path))


def makePublicDir(dirName):
    """Makes a given subdirectory if it doesn't already exist, making sure it is public.
    """
    if not os.path.exists(dirName):
        os.mkdir(dirName)
        os.chmod(dirName, 0o777)
    return dirName


def parser_with_common_options(provisioner_options=False):
    parser = ArgumentParser()

    if provisioner_options:
        add_provisioner_options(parser)

    # always add these
    add_logging_options(parser)
    parser.add_argument("--version", action='version', version=version)
    parser.add_argument("--tempDirRoot", dest="tempDirRoot", type=str, default=tempfile.gettempdir(),
                        help="Path to where temporary directory containing all temp files are created, "
                             "by default uses the current working directory as the base.")
    return parser


def get_temp_file(suffix="", rootDir=None):
    """Returns a string representing a temporary file, that must be manually deleted."""
    if rootDir is None:
        handle, tmp_file = tempfile.mkstemp(suffix)
        os.close(handle)
        return tmp_file
    else:
        alphanumerics = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        tmp_file = os.path.join(rootDir, f"tmp_{''.join([random.choice(alphanumerics) for _ in range(0, 10)])}{suffix}")
        open(tmp_file, 'w').close()
        os.chmod(tmp_file, 0o777)  # Ensure everyone has access to the file.
        return tmp_file
