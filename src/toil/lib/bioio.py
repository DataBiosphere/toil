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
import subprocess

from toil.statsAndLogging import set_logging_from_options, logger, root_logger, toil_logger, DEFAULT_LOGLEVEL
from toil.test import get_temp_file


# used by cactus; now a wrapper
def setLoggingFromOptions(options):
    set_logging_from_options(options)


# used by cactus; now a wrapper
def getTempFile(suffix="", rootDir=None):
    get_temp_file(suffix, rootDir)


# used by cactus
# TODO: only used in utilsTest.py; move this there once out of cactus
def system(command):
    """
    A convenience wrapper around subprocess.check_call that logs the command before passing it
    on. The command can be either a string or a sequence of strings. If it is a string shell=True
    will be passed to subprocess.check_call.
    :type command: str | sequence[string]
    """
    logger.debug('Running: %r', command)
    subprocess.check_call(command, shell=isinstance(command, str), bufsize=-1)


# Only used by cactus.
# Not used in Toil.
# TODO: Remove from cactus and then remove from Toil.
def getLogLevelString(logger=None):
    if logger is None:
        logger = root_logger
    return logging.getLevelName(logger.getEffectiveLevel())
