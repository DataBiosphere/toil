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

"""Reports the state of your given job tree.
"""

import sys
import os

import xml.etree.ElementTree as ET
from xml.dom import minidom #For making stuff pretty

from workflow.jobTree.lib.bioio import logger
from workflow.jobTree.lib.bioio import logFile 

from workflow.jobTree.lib.bioio import getBasicOptionParser
from workflow.jobTree.lib.bioio import parseBasicOptions
from workflow.jobTree.lib.bioio import TempFileTree

def main():
    """Reports stats on the job-tree, use in conjunction with --stats options to jobTree.
    """
    
    ##########################################
    #Construct the arguments.
    ##########################################  
    
    parser = getBasicOptionParser("usage: %prog", "%prog 0.1")
    
    parser.add_option("--jobTree", dest="jobTree", 
                      help="Directory containing the job tree")
    
    parser.add_option("--outputFile", dest="outputFile", default=None,
                      help="File in which to write results")
    
    
    options, args = parseBasicOptions(parser)
    logger.info("Parsed arguments")
    assert len(args) == 0
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)
    
    ##########################################
    #Do some checks.
    ##########################################
    
    logger.info("Checking if we have files for job tree")
    
    if options.jobTree == None:
        raise RuntimeError("You did not specify the job-tree")
    
    if not os.path.isdir(options.jobTree):
        raise RuntimeError("The given job dir tree does not exist: %s" % options.jobTree)
    
    if not os.path.isfile(os.path.join(options.jobTree, "config.xml")):
        raise RuntimeError("A valid job tree must contain the config file")
    
    if not os.path.isfile(os.path.join(options.jobTree, "stats.xml")):
        raise RuntimeError("The job-tree was run without the --stats flag, so no stats were created")
    
    ##########################################
    #Read the stats and config
    ##########################################  
    
    config = ET.parse(os.path.join(options.jobTree, "config.xml")).getroot()
    stats = ET.parse(os.path.join(options.jobTree, "stats.xml")).getroot()
    
    ##########################################
    #Collate the stats and report
    ##########################################  
    
    def fn(element, items, itemName):
        itemTimes = [ float(item.attrib["time"]) for item in items ]
        itemTimes.sort()
        itemClocks = [ float(item.attrib["clock"]) for item in items ]
        itemClocks.sort()
        itemWaits = [ float(item.attrib["time"]) - float(item.attrib["clock"]) for item in items ]
        itemWaits.sort()
        if len(itemTimes) == 0:
            itemTimes.append(0)
            itemClocks.append(0)
            itemWaits.append(0)
        return ET.SubElement(element, itemName, { "total_number":str(len(items)),
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
                                               "max_wait":str(max(itemWaits))
                                                })
    
    def fn2(element, containingItems, containingItemName, getFn):
        itemCounts = [ len(getFn(containingItem)) for containingItem in containingItems ]
        itemCounts.sort()
        if len(itemCounts) == 0: 
            itemCounts.append(0)
        element.attrib["median_number_per_%s" % containingItemName] = str(itemCounts[len(itemCounts)/2])
        element.attrib["average_number_per_%s" % containingItemName] = str(float(sum(itemCounts))/len(itemCounts))
        element.attrib["min_number_per_%s" % containingItemName] = str(min(itemCounts))
        element.attrib["max_number_per_%s" % containingItemName] = str(max(itemCounts))
    
    if stats.find("total_time") == None: #Hack to allow it to work on unfinished jobtrees.
        ET.SubElement(stats, "total_time", { "time":"0.0", "clock":"0.0"})
    
    collatedStatsTag = ET.Element("collated_stats", { "total_run_time":stats.find("total_time").attrib["time"],
                                                     "total_clock":stats.find("total_time").attrib["clock"],
                                                     "batch_system":config.attrib["batch_system"],
                                                     "job_time":config.attrib["job_time"],
                                                     "default_memory":config.attrib["default_memory"],
                                                     "default_cpu":config.attrib["default_cpu"],
                                                     "max_jobs":config.attrib["max_jobs"],
                                                     "max_threads":config.attrib["max_threads"] })
    
    #Add slave info
    slaves = stats.findall("slave")
    fn(collatedStatsTag, slaves, "slave")
    
    #Add job info
    jobs = []
    for slave in slaves:
        jobs += slave.findall("job")
    def fn3(slave):
        return slave.findall("job")
    fn2(fn(collatedStatsTag, jobs, "job"), slaves, "slave", fn3)
    
    #Add aggregated target info
    targets = []
    for job in jobs:
        for stack in job.findall("stack"):
            targets += stack.findall("target")
    def fn4(job):
        targets = []
        for stack in job.findall("stack"):
            targets += stack.findall("target")
        return targets
    fn2(fn(collatedStatsTag, targets, "target"), jobs, "job", fn4)   
    
    #Get info for each target
    targetNames = set()
    for target in targets:
        targetNames.add(target.attrib["class"])
    
    
    targetTypesTag = ET.SubElement(collatedStatsTag, "target_types")
    for targetName in targetNames:
        targetTypes = [ target for target in targets if target.attrib["class"] == targetName ]
        targetTypeTag = fn(targetTypesTag, targetTypes, targetName)
        estimatedRunTimes = [ float(target.attrib["e_time"]) for target in targetTypes ]
        targetTypeTag.attrib["estimated_time"] = str(sum(estimatedRunTimes)/len(estimatedRunTimes))
    
    def prettify(elem):
        """Return a pretty-printed XML string for the Element.
        """
        rough_string = ET.tostring(elem, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="  ")
    
    #Now dump it all out to file
    if options.outputFile != None:
        fileHandle = open(options.outputFile, 'w')
        #ET.ElementTree(collatedStatsTag).write(fileHandle)
        fileHandle.write(prettify(collatedStatsTag))
        fileHandle.close()
    
    #Now dump onto the screen
    print prettify(collatedStatsTag)
    
def _test():
    import doctest      
    return doctest.testmod()

if __name__ == '__main__':
    _test()
    main()
