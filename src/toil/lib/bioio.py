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

import socket
import sys
import os
import logging
import resource
import logging.handlers
import tempfile
import random
import math
import shutil
from argparse import ArgumentParser
from optparse import OptionContainer, OptionGroup
import subprocess
import xml.etree.cElementTree as ET
from xml.dom import minidom  # For making stuff pretty

defaultLogLevel = logging.INFO
logger = logging.getLogger(__name__)
rootLogger = logging.getLogger()
toilLogger = logging.getLogger('toil')


def getLogLevelString(logger=None):
    if logger is None:
        logger = rootLogger
    return logging.getLevelName(logger.getEffectiveLevel())

__loggingFiles = []
def addLoggingFileHandler(fileName, rotatingLogging=False):
    if fileName in __loggingFiles:
        return
    __loggingFiles.append(fileName)
    if rotatingLogging:
        handler = logging.handlers.RotatingFileHandler(fileName, maxBytes=1000000, backupCount=1)
    else:
        handler = logging.FileHandler(fileName)
    rootLogger.addHandler(handler)
    return handler


def setLogLevel(level, logger=None):
    """
    Sets the log level to a given string level (like "INFO"). Operates on the
    root logger by default, but another logger can be specified instead.
    """
    if logger is None:
        logger = rootLogger
    level = level.upper()
    if level == "OFF": level = "CRITICAL"
    # Note that getLevelName works in both directions, numeric to textual and textual to numeric
    numericLevel = logging.getLevelName(level)
    assert logging.getLevelName(numericLevel) == level
    logger.setLevel(numericLevel)
    # There are quite a few cases where we expect AWS requests to fail, but it seems
    # that boto handles these by logging the error *and* raising an exception. We
    # don't want to confuse the user with those error messages.
    logging.getLogger( 'boto' ).setLevel( logging.CRITICAL )

def logFile(fileName, printFunction=logger.info):
    """Writes out a formatted version of the given log file
    """
    printFunction("Reporting file: %s" % fileName)
    shortName = fileName.split("/")[-1]
    fileHandle = open(fileName, 'r')
    line = fileHandle.readline()
    while line != '':
        if line[-1] == '\n':
            line = line[:-1]
        printFunction("%s:\t%s" % (shortName, line))
        line = fileHandle.readline()
    fileHandle.close()
    
def logStream(fileHandle, shortName, printFunction=logger.info):
    """Writes out a formatted version of the given log stream.
    """
    printFunction("Reporting file: %s" % shortName)
    line = fileHandle.readline()
    while line != '':
        if line[-1] == '\n':
            line = line[:-1]
        printFunction("%s:\t%s" % (shortName, line))
        line = fileHandle.readline()
    fileHandle.close()

def addLoggingOptions(parser):
    # Wrapper function that allows toil to be used with both the optparse and
    # argparse option parsing modules
    if isinstance(parser, ArgumentParser):
        group = parser.add_argument_group("Logging Options",
                                          "Options that control logging")
        _addLoggingOptions(group.add_argument)
    else:
        raise RuntimeError("Unanticipated class passed to "
                           "addLoggingOptions(), %s. Expecting "
                           "argparse.ArgumentParser" % parser.__class__)

supportedLogLevels = (logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG)

def _addLoggingOptions(addOptionFn):
    """
    Adds logging options
    """
    # BEFORE YOU ADD OR REMOVE OPTIONS TO THIS FUNCTION, KNOW THAT YOU MAY ONLY USE VARIABLES ACCEPTED BY BOTH
    # optparse AND argparse FOR EXAMPLE, YOU MAY NOT USE default=%default OR default=%(default)s
    defaultLogLevelName = logging.getLevelName( defaultLogLevel )
    addOptionFn("--logOff", dest="logCritical", action="store_true", default=False,
                help="Same as --logCritical")
    for level in supportedLogLevels:
        levelName = logging.getLevelName(level)
        levelNameCapitalized = levelName.capitalize()
        addOptionFn("--log" + levelNameCapitalized, dest="logLevel",
                    action="store_const", const=levelName,
                    help="Turn on logging at level %s and above. (default is %s)" % (levelName, defaultLogLevelName))
    addOptionFn("--logLevel", dest="logLevel", default=defaultLogLevelName,
                help=("Log at given level (may be either OFF (or CRITICAL), ERROR, WARN (or WARNING), INFO or DEBUG). "
                      "(default is %s)" % defaultLogLevelName))
    addOptionFn("--logFile", dest="logFile", help="File to log in")
    addOptionFn("--rotatingLogging", dest="logRotating", action="store_true", default=False,
                help="Turn on rotating logging, which prevents log files getting too big.")

def setLoggingFromOptions(options):
    """
    Sets the logging from a dictionary of name/value options.
    """
    formatStr = ' '.join([socket.gethostname(), '%(asctime)s', '%(threadName)s',
                          '%(levelname)s', '%(name)s:', '%(message)s'])
    logging.basicConfig(format=formatStr)
    rootLogger.setLevel(defaultLogLevel)
    if options.logLevel is not None:
        setLogLevel(options.logLevel)
    else:
        # Ensure that any other log level overrides are in effect even if no log level is explicitly set
        setLogLevel(getLogLevelString())
    logger.info("Root logger is at level '%s', 'toil' logger at level '%s'.",
                getLogLevelString(logger=rootLogger), getLogLevelString(logger=toilLogger))
    if options.logFile is not None:
        addLoggingFileHandler(options.logFile, rotatingLogging=options.logRotating)
        logger.info("Logging to file '%s'." % options.logFile)


def system(command):
    """
    A convenience wrapper around subprocess.check_call that logs the command before passing it
    on. The command can be either a string or a sequence of strings. If it is a string shell=True
    will be passed to subprocess.check_call.

    :type command: str | sequence[string]
    """
    logger.debug('Running: %r', command)
    subprocess.check_call(command, shell=isinstance(command,basestring), bufsize=-1)

def getTotalCpuTimeAndMemoryUsage():
    """Gives the total cpu time and memory usage of itself and its children.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    childs = resource.getrusage(resource.RUSAGE_CHILDREN)
    totalCPUTime = me.ru_utime+me.ru_stime+childs.ru_utime+childs.ru_stime
    totalMemoryUsage = me.ru_maxrss+ me.ru_maxrss
    return totalCPUTime, totalMemoryUsage

def getTotalCpuTime():
    """Gives the total cpu time, including the children.
    """
    return getTotalCpuTimeAndMemoryUsage()[0]

def getTotalMemoryUsage():
    """Gets the amount of memory used by the process and its children.
    """
    return getTotalCpuTimeAndMemoryUsage()[1]

def absSymPath(path):
    """like os.path.abspath except it doesn't dereference symlinks
    """
    curr_path = os.getcwd()
    return os.path.normpath(os.path.join(curr_path, path))

#########################################################
#########################################################
#########################################################
#testing settings
#########################################################
#########################################################
#########################################################

class TestStatus:
    ###Global variables used by testing framework to run tests.
    TEST_SHORT = 0
    TEST_MEDIUM = 1
    TEST_LONG = 2
    TEST_VERY_LONG = 3

    TEST_STATUS = TEST_SHORT

    SAVE_ERROR_LOCATION = None

    def getTestStatus():
        return TestStatus.TEST_STATUS
    getTestStatus = staticmethod(getTestStatus)

    def setTestStatus(status):
        assert status in (TestStatus.TEST_SHORT, TestStatus.TEST_MEDIUM, TestStatus.TEST_LONG, TestStatus.TEST_VERY_LONG)
        TestStatus.TEST_STATUS = status
    setTestStatus = staticmethod(setTestStatus)

    def getSaveErrorLocation():
        """Location to in which to write inputs which created test error.
        """
        return TestStatus.SAVE_ERROR_LOCATION
    getSaveErrorLocation = staticmethod(getSaveErrorLocation)

    def setSaveErrorLocation(dir):
        """Set location in which to write inputs which created test error.
        """
        logger.info("Location to save error files in: %s" % dir)
        assert os.path.isdir(dir)
        TestStatus.SAVE_ERROR_LOCATION = dir
    setSaveErrorLocation = staticmethod(setSaveErrorLocation)

    def getTestSetup(shortTestNo=1, mediumTestNo=5, longTestNo=100, veryLongTestNo=0):
        if TestStatus.TEST_STATUS == TestStatus.TEST_SHORT:
            return shortTestNo
        elif TestStatus.TEST_STATUS == TestStatus.TEST_MEDIUM:
            return mediumTestNo
        elif TestStatus.TEST_STATUS == TestStatus.TEST_LONG:
            return longTestNo
        else: #Used for long example tests
            return veryLongTestNo
    getTestSetup = staticmethod(getTestSetup)

    def getPathToDataSets():
        """This method is used to store the location of
        the path where all the data sets used by tests for analysis are kept.
        These are not kept in the distrbution itself for reasons of size.
        """
        assert "SON_TRACE_DATASETS" in os.environ
        return os.environ["SON_TRACE_DATASETS"]
    getPathToDataSets = staticmethod(getPathToDataSets)

def getBasicOptionParser( parser=None):
    if parser is None:
        parser = ArgumentParser()

    addLoggingOptions(parser)

    parser.add_argument("--tempDirRoot", dest="tempDirRoot", type=str,
                      help="Path to where temporary directory containing all temp files are created, by default uses the current working directory as the base.",
                      default=tempfile.gettempdir())

    return parser

def parseBasicOptions(parser):
    """Setups the standard things from things added by getBasicOptionParser.
    """
    options = parser.parse_args()

    setLoggingFromOptions(options)

    #Set up the temp dir root
    if options.tempDirRoot == "None": # FIXME: Really, a string containing the word None?
        options.tempDirRoot = tempfile.gettempdir()

    return options

def getRandomAlphaNumericString(length=10):
    """Returns a random alpha numeric string of the given length.
    """
    return "".join([ random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') for i in xrange(0, length) ])

def makePublicDir(dirName):
    """Makes a given subdirectory if it doesn't already exist, making sure it is public.
    """
    if not os.path.exists(dirName):
        os.mkdir(dirName)
        os.chmod(dirName, 0777)
    return dirName

def getTempFile(suffix="", rootDir=None):
    """Returns a string representing a temporary file, that must be manually deleted
    """
    if rootDir is None:
        handle, tmpFile = tempfile.mkstemp(suffix)
        os.close(handle)
        return tmpFile
    else:
        tmpFile = os.path.join(rootDir, "tmp_" + getRandomAlphaNumericString() + suffix)
        open(tmpFile, 'w').close()
        os.chmod(tmpFile, 0777) #Ensure everyone has access to the file.
        return tmpFile
