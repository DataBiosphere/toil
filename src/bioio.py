#!/usr/bin/env python

#Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in
#all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#THE SOFTWARE.

import sys
import os
import re
import logging
import resource
import logging.handlers
import tempfile
import random
import math
from optparse import OptionParser
import subprocess
import array

DEFAULT_DISTANCE = 0.001

#########################################################
#########################################################
#########################################################  
#global logging settings / log functions
#########################################################
#########################################################
#########################################################

loggingFormatter = logging.Formatter('%(asctime)s %(levelname)s %(lineno)s %(message)s')

def __setDefaultLogger():
    l = logging.getLogger()
    #l.setLevel(logging.CRITICAL)
    for handler in l.handlers: #Do not add a duplicate handler unless needed
        if handler.stream == sys.stderr:
            return l
    handler = logging.StreamHandler(sys.stderr)
    #handler.setLevel(logging.CRITICAL) #null logger, to stop annoying error message
    l.addHandler(handler) 
    return l

logger = __setDefaultLogger()
logLevelString = "CRITICAL"

def getLogLevelString():
    return logLevelString

def addLoggingFileHandler(fileName, rotatingLogging=False):
    if rotatingLogging:
        handler = logging.handlers.RotatingFileHandler(fileName, maxBytes=1000000, backupCount=1)
    else:
        handler = logging.FileHandler(fileName)
    logger.addHandler(handler)
    
def setLogLevel(logLevel):
    logLevel = logLevel.upper()
    assert logLevel in [ "CRITICAL", "INFO", "DEBUG" ] #Log level must be one of these strings.
    global logLevelString
    logLevelString = logLevel
    if logLevel == "INFO":
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
    """Adds logging options to an optparse.OptionsParser
    """
    parser.add_option("--logInfo", dest="logInfo", action="store_true",
                     help="Turn on logging at INFO level. default=%default",
                     default=False)
    
    parser.add_option("--logDebug", dest="logDebug", action="store_true",
                     help="Turn on logging at DEBUG level. default=%default",
                     default=False)
    
    parser.add_option("--logLevel", dest="logLevel", type="string",
                      help="Log at level (may be either INFO/DEBUG/CRITICAL)")
    
    parser.add_option("--logFile", dest="logFile", type="string",
                      help="File to log in")
    
    parser.add_option("--noRotatingLogging", dest="logRotating", action="store_false",
                     help="Turn off rotating logging, which prevents log files getting too big. default=%default",
                     default=True)

def setLoggingFromOptions(options):
    """Sets the logging from a dictionary of name/value options.
    """
    #We can now set up the logging info.
    if options.logLevel is not None:
        setLogLevel(options.logLevel) #Use log level, unless flags are set..   
     
    if options.logInfo:
        setLogLevel("INFO")
    elif options.logDebug:
        setLogLevel("DEBUG")
        
    logger.info("Logging set at level: %s" % logLevelString)  
    
    if options.logFile is not None:
        addLoggingFileHandler(options.logFile, options.logRotating)
    
    logger.info("Logging to file: %s" % options.logFile)  
    

#########################################################
#########################################################
#########################################################
#system wrapper command
#########################################################
#########################################################
#########################################################

def system(command):
    logger.debug("Running the command: %s" % command)
    process = subprocess.Popen(command, shell=True, stdout=sys.stdout, stderr=sys.stderr)
    sts = os.waitpid(process.pid, 0)
    i = sts[1]
    if i != 0:
        raise RuntimeError("Command: %s exited with non-zero status %i" % (command, i))
    return i

def popen(command, tempFile):
    """Runs a command and captures standard out in the given temp file.
    """
    fileHandle = open(tempFile, 'w')
    logger.debug("Running the command: %s" % command)
    process = subprocess.Popen(command, shell=True, stdout=fileHandle)
    sts = os.waitpid(process.pid, 0)
    fileHandle.close()
    i = sts[1]
    if i != 0:
        raise RuntimeError("Command: %s exited with non-zero status %i" % (command, i))
    return i

def popenCatch(command):
    """Runs a command and return standard out.
    """
    logger.debug("Running the command: %s" % command)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    sts = os.waitpid(process.pid, 0)
    i = sts[1]
    if i != 0:
        raise RuntimeError("Command: %s exited with non-zero status %i" % (command, i))
    return process.stdout.read().strip()

def getTotalCpuTime():
    """Gives the total cpu time, including the children. 
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    childs = resource.getrusage(resource.RUSAGE_CHILDREN)
    totalCpuTime = me.ru_utime+me.ru_stime+childs.ru_utime+childs.ru_stime
    return totalCpuTime
 
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
#########################################################
#########################################################
#options parser functions
#########################################################
#########################################################
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
    else: 
        assert options.testLength == "VERY_LONG" #Otherwise an unrecognised option
        TestStatus.setTestStatus(TestStatus.TEST_VERY_LONG)
    
    if options.saveError is not None:
        TestStatus.setSaveErrorLocation(options.saveError)
        
    return options, args
    
def nameValue(name, value, valueType=str):
    """Little function to make it easier to make name value strings for commands.
    """
    if valueType == bool:
        if value:
            return "--%s" % name
        return ""
    if value is None:
        return ""
    return "--%s %s" % (name, valueType(value))    

#########################################################
#########################################################
#########################################################
#temp files
#########################################################
#########################################################
#########################################################

def getRandomAlphaNumericString(length=10):
    """Returns a random alpha numeric string of the given length.
    """
    return "".join([ random.choice('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz') for i in xrange(0, length) ])
    
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
    
class TempFileTree:
    """A hierarchical tree structure for storing directories of files/dirs/
    
    The total number of legal files is equal to filesPerDir**levels.
    filesPerDer and levels must both be greater than zero.
    The rootDir may or may not yet exist (and may or may not be empty), though
    if files exist in the dirs of levels 0 ... level-1 then they must be dirs,
    which will be indexed the by tempfile tree.
    """
    def __init__(self, rootDir, filesPerDir=100, levels=4, ):
        #Do basic checks of input
        assert(filesPerDir) >= 1
        assert(levels) >= 1
        if not os.path.isdir(rootDir):
            #Make the root dir
            os.mkdir(rootDir)
        
        #Basic attributes of system at start up.
        self.levelNo = levels
        self.filesPerDir = filesPerDir
        self.rootDir = rootDir
        #Dynamic variables
        self.tempDir = rootDir
        self.level = 0
        #These two variables will only refer to the existance of this class instance.
        self.tempFilesCreated = 0
        self.tempFilesDestroyed = 0
        
        currentFiles = self.listFiles()
        logger.info("We have setup the temp file tree, it contains %s files currently, \
        %s of the possible total" % \
        (len(currentFiles), len(currentFiles)/math.pow(filesPerDir, levels)))
    
    def getTempFile(self, suffix="", makeDir=False):
        while self.level < self.levelNo:
            #Basic checks for start of loop
            assert self.level >= 0
            assert os.path.isdir(self.tempDir)
            fileNames = os.listdir(self.tempDir)
            #If tempDir contains max file number then:
            if len(fileNames) >= self.filesPerDir:
                #if level number is already 0 raise an exception
                if self.level == 0:
                    raise RuntimeError("We ran out of space to make temp files")
                #reduce level number by one, chop off top of tempDir.
                self.level -= 1
                self.tempDir = os.path.split(self.tempDir)[0]
            else:
                if self.level == self.levelNo-1:
                    #make temporary file in dir and return it.
                    if makeDir:
                        return getTempDirectory(rootDir=self.tempDir)
                    else:
                        return getTempFile(suffix=suffix, rootDir=self.tempDir)
                else:
                    #mk new dir, and add to tempDir path, inc the level buy one.
                    self.tempDir = getTempDirectory(rootDir=self.tempDir)
                    self.level += 1
    
    def getTempDirectory(self):
        return self.getTempFile(makeDir=True)
    
    def __destroyFile(self, tempFile):
        #If not part of the current tempDir, from which files are being created.
        baseDir = os.path.split(tempFile)[0]
        if baseDir != self.tempDir:
            while True: #Now remove any parent dirs that are empty.
                if os.listdir(baseDir) == [] and baseDir != self.rootDir:
                    os.rmdir(baseDir)
                    baseDir = os.path.split(baseDir)[0]
                else:
                    break
    
    def destroyTempFile(self, tempFile):
        """Removes the temporary file in the temp file dir, checking its in the temp file tree.
        """
        #Do basic assertions for goodness of the function
        assert os.path.isfile(tempFile)
        assert os.path.commonprefix((self.rootDir, tempFile)) == self.rootDir #Checks file is part of tree
        #Update stats.
        self.tempFilesDestroyed += 1
        #Do the actual removal
        os.remove(tempFile)
        self.__destroyFile(tempFile)
    
    def destroyTempDir(self, tempDir):
        """Removes a temporary directory in the temp file dir, checking its in the temp file tree.
        The dir will be removed regardless of if it is empty.
        """
        #Do basic assertions for goodness of the function
        assert os.path.isdir(tempDir)
        assert os.path.commonprefix((self.rootDir, tempDir)) == self.rootDir #Checks file is part of tree
        #Update stats.
        self.tempFilesDestroyed += 1
        #Do the actual removal
        system("rm -rf %s" % tempDir)
        self.__destroyFile(tempDir)
   
    def listFiles(self):
        """Gets all files in the temp file tree (which may be dirs).
        """
        def fn(dirName, level, files):
            if level == self.levelNo-1:
                for fileName in os.listdir(dirName):
                    absFileName = os.path.join(dirName, fileName)
                    files.append(absFileName)
            else:
                for subDir in os.listdir(dirName):
                    absDirName = os.path.join(dirName, subDir)
                    assert os.path.isdir(absDirName)
                    fn(absDirName, level+1, files)
        files = []
        fn(self.rootDir, 0, files)
        return files
   
    def destroyTempFiles(self):
        """Destroys all temp temp file hierarchy, getting rid of all files.
        """
        os.system("rm -rf %s" % self.rootDir)
        logger.debug("Temp files created: %s, temp files actively destroyed: %s" % (self.tempFilesCreated, self.tempFilesDestroyed))  

    
def workflowRootPath():
    """Function for finding external location.
    """
    import jobTree.scriptTree.target
    i = os.path.abspath(jobTree.scriptTree.target.__file__)
    return os.path.split(os.path.split(i)[0])[0]

def main():
    pass

def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
