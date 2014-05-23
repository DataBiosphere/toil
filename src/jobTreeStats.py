#!/usr/bin/env python

# Copyright (C) 2011 by Benedict Paten (benedictpaten@gmail.com)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""Reports the state of your given job tree.
"""

import sys
import os

import xml.etree.cElementTree as ET
from xml.dom import minidom  # For making stuff pretty

from sonLib.bioio import logger
from sonLib.bioio import logFile

from sonLib.bioio import getBasicOptionParser
from sonLib.bioio import parseBasicOptions
from sonLib.bioio import TempFileTree

from jobTree.src.master import getEnvironmentFileName, getJobFileDirName
from jobTree.src.master import getStatsFileName, getConfigFileName


def initializeOptions(parser):
    ##########################################
    # Construct the arguments.
    ##########################################
    parser.add_option("--jobTree", dest="jobTree",
                      help="Directory containing the job tree")

    parser.add_option("--outputFile", dest="outputFile", default=None,
                      help="File in which to write results")

def checkOptions(options, args, parser):
    logger.info("Parsed arguments")
    assert len(args) <= 1  # Only jobtree may be specified as argument
    if len(args) == 1:  # Allow jobTree directory as arg
        options.jobTree = args[0]
    logger.info("Checking if we have files for job tree")
    if options.jobTree == None:
        parser.error("Specify --jobTree")
    if not os.path.exists(options.jobTree):
        parser.error("--jobTree %s does not exist"
                     % options.jobTree)
    if not os.path.isdir(options.jobTree):
        parser.error("--jobTree %s is not a directory"
                     % options.jobTree)
    if not os.path.isfile(getConfigFileName(options.jobTree)):
        parser.error("A valid job tree must contain the config file")
    if not os.path.isfile(getStatsFileName(options.jobTree)):
        parser.error("The job-tree was run without the --stats flag, "
                     "so no stats were created")
    logger.info("Checked arguments")

def prettify(elem):
    """Return a pretty-printed XML string for the ElementTree Element.
    """
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def buildElement(element, items, itemName):
    """ Create an element for output.
    """
    def __round(i):
        if i < 0:
            logger.debug("I got a less than 0 value: %s" % i)
            return 0.0
        return i
    itemTimes = [ __round(float(item.attrib["time"])) for item in items ]
    itemTimes.sort()
    itemClocks = [ __round(float(item.attrib["clock"])) for item in items ]
    itemClocks.sort()
    itemWaits = [ __round(__round(float(item.attrib["time"])) -
                          __round(float(item.attrib["clock"])))
                  for item in items ]
    itemWaits.sort()
    itemMemory = [ __round(float(item.attrib["memory"])) for item in items ]
    itemMemory.sort()
    assert len(itemClocks) == len(itemTimes)
    assert len(itemClocks) == len(itemWaits)
    if len(itemTimes) == 0:
        itemTimes.append(0)
        itemClocks.append(0)
        itemWaits.append(0)
        itemMemory.append(0)
    return ET.SubElement(
        element, itemName,
        {"total_number":str(len(items)),
         "total_time":str(sum(itemTimes)),
         "median_time":str(itemTimes[len(itemTimes)/2]),
         "average_time":str(sum(itemTimes)/len(itemTimes)),
         "min_time":str(min(itemTimes)),
         "max_time":str(max(itemTimes)),
         "total_clock":str(sum(itemClocks)),
         "median_clock":str(itemClocks[len(itemClocks)/2]),
         "average_clock":str(sum(itemClocks)/len(itemClocks)),
         "min_clock":str(min(itemClocks)),
         "max_clock":str(max(itemClocks)),
         "total_wait":str(sum(itemWaits)),
         "median_wait":str(itemWaits[len(itemWaits)/2]),
         "average_wait":str(sum(itemWaits)/len(itemWaits)),
         "min_wait":str(min(itemWaits)),
         "max_wait":str(max(itemWaits)),
         "total_memory":str(sum(itemMemory)),
         "median_memory":str(itemMemory[len(itemMemory)/2]),
         "average_memory":str(sum(itemMemory)/len(itemMemory)),
         "min_memory":str(min(itemMemory)),
         "max_memory":str(max(itemMemory))
         })

def createSummary(element, containingItems, containingItemName, getFn):
    itemCounts = [len(getFn(containingItem)) for
                  containingItem in containingItems]
    itemCounts.sort()
    if len(itemCounts) == 0:
        itemCounts.append(0)
    element.attrib["median_number_per_%s" %
                   containingItemName] = str(itemCounts[len(itemCounts) / 2])
    element.attrib["average_number_per_%s" %
                   containingItemName] = str(float(sum(itemCounts)) /
                                             len(itemCounts))
    element.attrib["min_number_per_%s" %
                   containingItemName] = str(min(itemCounts))
    element.attrib["max_number_per_%s" %
                   containingItemName] = str(max(itemCounts))

def processData(config, stats, options):
    ##########################################
    # Collate the stats and report
    ##########################################
    if stats.find("total_time") == None:  # Hack to allow unfinished jobtrees.
        ET.SubElement(stats, "total_time", { "time":"0.0", "clock":"0.0"})

    collatedStatsTag = ET.Element(
        "collated_stats",
        {"total_run_time":stats.find("total_time").attrib["time"],
         "total_clock":stats.find("total_time").attrib["clock"],
         "batch_system":config.attrib["batch_system"],
         "job_time":config.attrib["job_time"],
         "default_memory":config.attrib["default_memory"],
         "default_cpu":config.attrib["default_cpu"],
         "max_cpus":config.attrib["max_cpus"],
         "max_threads":config.attrib["max_threads"] })

    # Add slave info
    slaves = stats.findall("slave")
    buildElement(collatedStatsTag, slaves, "slave")

    # Add aggregated target info
    targets = []
    for slave in slaves:
        targets += slave.findall("target")
    def fn4(job):
        return list(slave.findall("target"))
    createSummary(buildElement(collatedStatsTag, targets, "target"),
                  slaves, "slave", fn4)
    # Get info for each target
    targetNames = set()
    for target in targets:
        targetNames.add(target.attrib["class"])
    targetTypesTag = ET.SubElement(collatedStatsTag, "target_types")
    for targetName in targetNames:
        targetTypes = [ target for target in targets
                        if target.attrib["class"] == targetName ]
        targetTypeTag = buildElement(targetTypesTag, targetTypes, targetName)
    return collatedStatsTag

def reportData(collatedStatsTag, options):
    # Now dump it all out to file
    if options.outputFile != None:
        fileHandle = open(options.outputFile, 'w')
        fileHandle.write(prettify(collatedStatsTag))
        fileHandle.close()
    # Now dump onto the screen
    print prettify(collatedStatsTag)

def main():
    """Reports stats on the job-tree, use with --stats option to jobTree.
    """

    parser = getBasicOptionParser(
        "usage: %prog [--jobTree] JOB_TREE_DIR [options]", "%prog 0.1")
    initializeOptions(parser)
    options, args = parseBasicOptions(parser)
    checkOptions(options, args, parser)
    config = ET.parse(getConfigFileName(options.jobTree)).getroot()
    stats = ET.parse(getStatsFileName(options.jobTree)).getroot()
    collatedStatsTag = processData(config, stats, options)
    reportData(collatedStatsTag, options)

def _test():
    import doctest
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
