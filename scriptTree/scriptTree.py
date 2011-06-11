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

from sonLib.bioio import logger
from sonLib.bioio import getBasicOptionParser 
from sonLib.bioio import parseBasicOptions
 
from jobTree.scriptTree.stack import loadPickleFile

def run(job, target, classes):
    #Naughty stuff to do the import of the target we need
    for className in classes:
        logger.info("Loading the class name %s" % className)
        l = className.split(".")
        moduleName = ".".join(l[:-1])
        className = l[-1]
        _temp = __import__(moduleName, globals(), locals(), [ className ], -1)
        exec "%s = 1" % className
        vars()[className] = _temp.__dict__[className]
    
    target = loadPickleFile(target)
    
    target.execute(job) 
    
    logger.info("Finished executing the target")
