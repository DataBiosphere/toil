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
import subprocess

from toil.statsAndLogging import (logger,
                                  root_logger,
                                  set_logging_from_options)
from toil.test import get_temp_file


# used by cactus
# TODO: only used in utilsTest.py; move this there once out of cactus
def system(command):
    """
    A convenience wrapper around subprocess.check_call that logs the command before passing it
    on. The command can be either a string or a sequence of strings. If it is a string shell=True
    will be passed to subprocess.check_call.
    :type command: str | sequence[string]
    """
    logger.warning('Deprecated toil method that will be moved/replaced in a future release."')
    logger.debug(f'Running: {command}')
    subprocess.check_call(command, shell=isinstance(command, str), bufsize=-1)


# Used by cactus; now a wrapper and not used in Toil.
# TODO: Remove from cactus and then remove from Toil.
def getLogLevelString(logger=None):
    root_logger.warning('Deprecated toil method.  Please call "logging.getLevelName" directly.')
    if logger is None:
        logger = root_logger
    return logging.getLevelName(logger.getEffectiveLevel())


# Used by cactus; now a wrapper and not used in Toil.
# TODO: Remove from cactus and then remove from Toil.
def setLoggingFromOptions(options):
    logger.warning('Deprecated toil method.  Please use "toil.statsAndLogging.set_logging_from_options()" instead."')
    set_logging_from_options(options)


# Used by cactus; now a wrapper and not used in Toil.
# TODO: Remove from cactus and then remove from Toil.
def getTempFile(suffix="", rootDir=None):
    logger.warning('Deprecated toil method.  Please use "toil.test.get_temp_file()" instead."')
    return get_temp_file(suffix, rootDir)
