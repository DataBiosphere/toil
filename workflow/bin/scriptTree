#!/usr/bin/env python

from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.bioio import getBasicOptionParser 
from workflow.jobTree.lib.bioio import parseBasicOptions

from workflow.jobTree.scriptTree.stack import loadPickleFile

parser = getBasicOptionParser("usage: %prog [options]", "%prog 0.1")

parser.add_option("--job", dest="jobFile", 
                  help="Job file containing command to run")

parser.add_option("--target", dest="target", 
                  help="File containing a pickled, wrapped instance of target classes")

options, args = parseBasicOptions(parser)

assert options.target != None

logger.info("Parsed the input arguments")      

#Naughty stuff to do the import of the target we need
for className in args:
    logger.info("Loading the class name", className)
    l = className.split(".")
    moduleName = ".".join(l[:-1])
    className = l[-1]
    _temp = __import__(moduleName, globals(), locals(), [ className ], -1)
    exec "%s = 1" % className
    vars()[className] = _temp.__dict__[className]

target = loadPickleFile(options.target)

target.execute(options.jobFile) 

logger.info("Finished executing the target")

