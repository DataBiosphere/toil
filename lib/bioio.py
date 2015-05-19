#!/usr/bin/env python

#Copyright (C) 2006-2012 by Benedict Paten (benedictpaten@gmail.com)
#
#Released under the MIT license, see LICENSE.txt

import sys
import os
import logging
import resource
import logging.handlers
import tempfile
import random
from argparse import ArgumentParser
from optparse import OptionParser, OptionContainer, OptionGroup
import subprocess

DEFAULT_DISTANCE = 0.001

#########################################################
#   global logging settings / log functions
#########################################################


loggingFormatter = logging.Formatter('%(asctime)s %(levelname)s %(lineno)s %(message)s')

def __setDefaultLogger():
    l = logging.getLogger()
    for handler in l.handlers: #Do not add a duplicate handler unless needed
        if handler.stream == sys.stderr:
            return l
    handler = logging.StreamHandler(sys.stderr)
    l.addHandler(handler)
    l.setLevel(logging.CRITICAL)
    return l

logger = __setDefaultLogger()
logLevelString = "CRITICAL"

def redirectLoggerStreamHandlers(oldStream, newStream):
    """Redirect the stream of a stream handler to a different stream
    """
    for handler in list(logger.handlers): #Remove old handlers
        if handler.stream == oldStream:
            handler.close()
            logger.removeHandler(handler)
    for handler in logger.handlers: #Do not add a duplicate handler
        if handler.stream == newStream:
           return
    logger.addHandler(logging.StreamHandler(newStream))

def getLogLevelString():
    return logLevelString

__loggingFiles = []
def addLoggingFileHandler(fileName, rotatingLogging=False):
    if fileName in __loggingFiles:
        return
    __loggingFiles.append(fileName)
    if rotatingLogging:
        handler = logging.handlers.RotatingFileHandler(fileName, maxBytes=1000000, backupCount=1)
    else:
        handler = logging.FileHandler(fileName)
    logger.addHandler(handler)
    return handler

def setLogLevel(logLevel):
    logLevel = logLevel.upper()
    assert logLevel in [ "OFF", "CRITICAL", "INFO", "DEBUG" ] #Log level must be one of these strings.
    global logLevelString
    logLevelString = logLevel
    if logLevel == "OFF":
        logger.setLevel(logging.FATAL)
    elif logLevel == "INFO":
        logger.setLevel(logging.INFO)
    elif logLevel == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif logLevel == "CRITICAL":
        logger.setLevel(logging.CRITICAL)

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

def addLoggingOptions(parser):
    # Wrapper function that allows jobTree to be used with both the optparse and
    # argparse option parsing modules
    if isinstance(parser, OptionContainer):
        group = OptionGroup(parser, "Logging options",
                            "Options that control logging")
        _addLoggingOptions(group.add_option)
        parser.add_option_group(group)
    elif isinstance(parser, ArgumentParser):
        group = parser.add_argument_group("Logging Options",
                                          "Options that control logging")
        _addLoggingOptions(group.add_argument)
    else:
        raise RuntimeError("Unanticipated class passed to "
                           "addLoggingOptions(), %s. Expecting "
                           "Either optparse.OptionParser or "
                           "argparse.ArgumentParser" % parser.__class__)

def _addLoggingOptions(addOptionFn):
    """Adds logging options
    """
    ##################################################
    # BEFORE YOU ADD OR REMOVE OPTIONS TO THIS FUNCTION, KNOW THAT
    # YOU MAY ONLY USE VARIABLES ACCEPTED BY BOTH optparse AND argparse
    # FOR EXAMPLE, YOU MAY NOT USE default=%default OR default=%(default)s
    ##################################################
    addOptionFn("--logOff", dest="logOff", action="store_true", default=False,
                     help="Turn off logging. (default is CRITICAL)")
    addOptionFn(
        "--logInfo", dest="logInfo", action="store_true", default=False,
        help="Turn on logging at INFO level. (default is CRITICAL)")
    addOptionFn(
        "--logDebug", dest="logDebug", action="store_true", default=False,
        help="Turn on logging at DEBUG level. (default is CRITICAL)")
    addOptionFn(
        "--logLevel", dest="logLevel", default='CRITICAL',
        help=("Log at level (may be either OFF/INFO/DEBUG/CRITICAL). "
              "(default is CRITICAL)"))
    addOptionFn("--logFile", dest="logFile", help="File to log in")
    addOptionFn(
        "--rotatingLogging", dest="logRotating", action="store_true",
        default=False, help=("Turn on rotating logging, which prevents log "
                             "files getting too big."))

def setLoggingFromOptions(options):
    """Sets the logging from a dictionary of name/value options.
    """
    #We can now set up the logging info.
    if options.logLevel is not None:
        setLogLevel(options.logLevel) #Use log level, unless flags are set..

    if options.logOff:
        setLogLevel("OFF")
    elif options.logInfo:
        setLogLevel("INFO")
    elif options.logDebug:
        setLogLevel("DEBUG")

    logger.info("Logging set at level: %s" % logLevelString)

    if options.logFile is not None:
        addLoggingFileHandler(options.logFile, options.logRotating)

    logger.info("Logging to file: %s" % options.logFile)


#########################################################
#system wrapper command
#########################################################

def system(command):
    logger.debug("Running the command: %s" % command)
    sts = subprocess.call(command, shell=True, bufsize=-1, stdout=sys.stdout, stderr=sys.stderr)
    if sts != 0:
        raise RuntimeError("Command: %s exited with non-zero status %i" % (command, sts))
    return sts

def popen(command, tempFile):
    """Runs a command and captures standard out in the given temp file.
    """
    fileHandle = open(tempFile, 'w')
    logger.debug("Running the command: %s" % command)
    sts = subprocess.call(command, shell=True, stdout=fileHandle, stderr=sys.stderr, bufsize=-1)
    fileHandle.close()
    if sts != 0:
        raise RuntimeError("Command: %s exited with non-zero status %i" % (command, sts))
    return sts

def popenCatch(command, stdinString=None):
    """Runs a command and return standard out.
    """
    logger.debug("Running the command: %s" % command)
    if stdinString != None:
        process = subprocess.Popen(command, shell=True,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr, bufsize=-1)
        output, nothing = process.communicate(stdinString)
    else:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=sys.stderr, bufsize=-1)
        output, nothing = process.communicate() #process.stdout.read().strip()
    sts = process.wait()
    if sts != 0:
        raise RuntimeError("Command: %s with stdin string '%s' exited with non-zero status %i" % (command, stdinString, sts))
    return output #process.stdout.read().strip()

def popenPush(command, stdinString=None):
    if stdinString == None:
        system(command)
    else:
        process = subprocess.Popen(command, shell=True,
                                   stdin=subprocess.PIPE, stderr=sys.stderr, bufsize=-1)
        process.communicate(stdinString)
        sts = process.wait()
        if sts != 0:
            raise RuntimeError("Command: %s with stdin string '%s' exited with non-zero status %i" % (command, stdinString, sts))

def spawnDaemon(command):
    """Launches a command as a daemon.  It will need to be explicitly killed
    """
    return system("sonLib_daemonize.py \'%s\'" % command)

def getTotalCpuTimeAndMemoryUsage():
    """Gives the total cpu time and memory usage of itself and its children.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    childs = resource.getrusage(resource.RUSAGE_CHILDREN)
    totalCpuTime = me.ru_utime+me.ru_stime+childs.ru_utime+childs.ru_stime
    totalMemoryUsage = me.ru_maxrss+ me.ru_maxrss
    return totalCpuTime, totalMemoryUsage

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
#testing settings
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

def saveInputs(savedInputsDir, listOfFilesAndDirsToSave):
    """Copies the list of files to a directory created in the save inputs dir,
    and returns the name of this directory.
    """
    logger.info("Saving the inputs: %s to the directory: %s" % (" ".join(listOfFilesAndDirsToSave), savedInputsDir))
    assert os.path.isdir(savedInputsDir)
    #savedInputsDir = getTempDirectory(saveInputsDir)
    createdFiles = []
    for fileName in listOfFilesAndDirsToSave:
        if os.path.isfile(fileName):
            copiedFileName = os.path.join(savedInputsDir, os.path.split(fileName)[-1])
            system("cp %s %s" % (fileName, copiedFileName))
        else:
            copiedFileName = os.path.join(savedInputsDir, os.path.split(fileName)[-1]) + ".tar"
            system("tar -cf %s %s" % (copiedFileName, fileName))
        createdFiles.append(copiedFileName)
    return createdFiles


#########################################################
#   options parser functions
#########################################################

def getBasicOptionParser(usage="usage: %prog [options]", version="%prog 0.1", parser=None):
    if parser is None:
        parser = OptionParser(usage=usage, version=version)

    addLoggingOptions(parser)

    parser.add_option("--tempDirRoot", dest="tempDirRoot", type="string",
                      help="Path to where temporary directory containing all temp files are created, by default uses the current working directory as the base.",
                      default=os.getcwd())

    return parser

def parseBasicOptions(parser):
    """Setups the standard things from things added by getBasicOptionParser.
    """
    (options, args) = parser.parse_args()

    setLoggingFromOptions(options)

    #Set up the temp dir root
    if options.tempDirRoot == "None":
        options.tempDirRoot = os.getcwd()

    return options, args

def parseSuiteTestOptions(parser=None):
    if parser is None:
        parser = getBasicOptionParser()

    parser.add_option("--testLength", dest="testLength", type="string",
                     help="Control the length of the tests either SHORT/MEDIUM/LONG/VERY_LONG. default=%default",
                     default="SHORT")

    parser.add_option("--saveError", dest="saveError", type="string",
                     help="Directory in which to store the inputs of failed tests")

    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")

    if options.testLength == "SHORT":
        TestStatus.setTestStatus(TestStatus.TEST_SHORT)
    elif options.testLength == "MEDIUM":
        TestStatus.setTestStatus(TestStatus.TEST_MEDIUM)
    elif options.testLength == "LONG":
        TestStatus.setTestStatus(TestStatus.TEST_LONG)
    elif options.testLength == "VERY_LONG":
        TestStatus.setTestStatus(TestStatus.TEST_VERY_LONG)
    else:
        parser.error('Unrecognised option for --testLength, %s. Options are SHORT, MEDIUM, LONG, VERY_LONG.' %
                     options.testLength)

    if options.saveError is not None:
        TestStatus.setSaveErrorLocation(options.saveError)

    return options, args

def nameValue(name, value, valueType=str, quotes=False):
    """Little function to make it easier to make name value strings for commands.
    """
    if valueType == bool:
        if value:
            return "--%s" % name
        return ""
    if value is None:
        return ""
    if quotes:
        return "--%s '%s'" % (name, valueType(value))
    return "--%s %s" % (name, valueType(value))


#########################################################
#   temp files
#########################################################

def getRandomAlphaNumericString(length=10):
    """Returns a random alpha numeric string of the given length.
    """
    return "".join([ random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') for i in xrange(0, length) ])

def makeSubDir(dirName):
    """Makes a given subdirectory if it doesn't already exist, making sure it us public.
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

def getTempDirectory(rootDir=None):
    """
    returns a temporary directory that must be manually deleted
    """
    if rootDir is None:
        return tempfile.mkdtemp()
    else:
        while True:
            rootDir = os.path.join(rootDir, "tmp_" + getRandomAlphaNumericString())
            if not os.path.exists(rootDir):
                break
        os.mkdir(rootDir)
        os.chmod(rootDir, 0777) #Ensure everyone has access to the file.
        return rootDir


def main():
    pass

def _test():
    import doctest
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
