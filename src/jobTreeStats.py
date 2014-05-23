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

""" Reports the state of your given job tree.
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

class JTTag(object):
  def __init__(self, tree):
    """ Given an ElementTree tag, build a convenience object.
    """
    for name in ["total_time", "median_clock", "total_memory", "median_wait",
                "total_number", "average_time", "median_memory",
                "min_number_per_slave", "average_wait", "total_clock",
                "median_time", "min_time", "min_wait", "max_clock",
                "max_wait", "total_wait", "min_clock", "average_memory",
                "max_number_per_slave", "max_memory", "min_clock",
                "average_memory", "max_number_per_slave", "max_memory",
                "median_number_per_slave", "average_number_per_slave",
                "max_time", "average_clock", "min_memory"
                ]:
      setattr(self, name, self.__get(tree, name))
      self.name = tree.tag
  def __get(self, tag, name):
    if name in tag.attrib:
      value = tag.attrib[name]
    else:
      return float("nan")
    try:
      a = float(value)
    except ValueError:
      a = float("nan")
    return a

def initializeOptions(parser):
    ##########################################
    # Construct the arguments.
    ##########################################
    parser.add_option("--jobTree", dest="jobTree",
                      help="Directory containing the job tree")
    parser.add_option("--outputFile", dest="outputFile", default=None,
                      help="File in which to write results")
    parser.add_option("--raw", action="store_true", default=False,
                      help="output the raw xml data.")
    parser.add_option("--pretty", action="store_true", default=False,
                      help=("if not raw, prettify the numbers to be "
                            "human readable."))
    parser.add_option("--categories",
                      help=("comma separated list from [time, clock, wait, "
                            "memory]"))
    parser.add_option("--sortby", default="time",
                      help=("how to sort Target list. may be from [alpha, "
                            "time, clock, wait, memory, count]. "
                            "default=%(default)s"))
    parser.add_option("--reverse_sort", default=False, action="store_true",
                      help="reverse sort order.")

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
    defaultCategories = ["time", "clock", "wait", "memory"]
    if options.categories is None:
        options.categories = defaultCategories
    else:
        options.categories = options.categories.split(",")
    for c in options.categories:
        if c not in defaultCategories:
            parser.error("Unknown category %s. Must be from %s"
                         % (c, str(defaultCategories)))
    extraSort = ["count", "alpha"]
    if options.sortby is not None:
        if (options.sortby not in defaultCategories and
            options.sortby not in extraSort):
            parser.error("Unknown --sortby %s. Must be from %s"
                         % (options.sortby, str(defaultCategories)))
    logger.info("Checked arguments")

def prettyXml(elem):
    """Return a pretty-printed XML string for the ElementTree Element.
    """
    roughString = ET.tostring(elem, "utf-8")
    reparsed = minidom.parseString(roughString)
    return reparsed.toprettyxml(indent="  ")

def padStr(s, field=None):
  """ Pad the begining of a string with spaces, if necessary.
  """
  if field is None:
    return s
  else:
    if len(s) >= field:
      return s
    else:
      return " " * (field - len(s)) + s

def prettyMemory(k, field=None, isBytes=False):
  """ Given input k as kilobytes, return a nicely formatted string.
  """
  from math import floor
  if isBytes:
    k /= 1024
  if k < 1024:
    return padStr("%gK" % k, field)
  if k < (1024 * 1024):
    return padStr("%.1fM" % (k / 1024.0), field)
  if k < (1024 * 1024 * 1024):
    return padStr("%.1fG" % (k / 1024.0 / 1024.0), field)
  if k < (1024 * 1024 * 1024 * 1024):
    return padStr("%.1fT" % (k / 1024.0 / 1024.0 / 1024.0), field)
  if k < (1024 * 1024 * 1024 * 1024 * 1024):
    return padStr("%.1fP" % (k / 1024.0 / 1024.0 / 1024.0 / 1024.0), field)

def prettyTime(t, field=None):
  """ Given input t as seconds, return a nicely formatted string.
  """
  from math import floor
  pluralDict = {True: "s", False: ""}
  if t < 120:
    return padStr("%ds" % t, field)
  if t < 120 * 60:
    m = floor(t / 60.)
    s = t % 60
    return padStr("%dm%ds" % (m, s), field)
  if t < 25 * 60 * 60:
    h = floor(t / 60. / 60.)
    m = floor((t - (h * 60. * 60.)) / 60.)
    s = t % 60
    return padStr("%dh%gm%ds" % (h, m, s), field)
  if t < 7 * 24 * 60 * 60:
    d = floor(t / 24. / 60. / 60.)
    h = floor((t - (d * 24. * 60. * 60.)) / 60. / 60.)
    m = floor((t
               - (d * 24. * 60. * 60.)
               - (h * 60. * 60.))
              / 60.)
    s = t % 60
    dPlural = pluralDict[d > 1]
    return padStr("%dday%s%dh%dm%ds" % (d, dPlural, h, m, s), field)
  w = floor(t / 7. / 24. / 60. / 60.)
  d = floor((t - (w * 7 * 24 * 60 * 60)) / 24. / 60. / 60.)
  h = floor((t
             - (w * 7. * 24. * 60. * 60.)
             - (d * 24. * 60. * 60.))
            / 60. / 60.)
  m = floor((t
             - (w * 7. * 24. * 60. * 60.)
             - (d * 24. * 60. * 60.)
             - (h * 60. * 60.))
            / 60.)
  s = t % 60
  wPlural = pluralDict[w > 1]
  dPlural = pluralDict[d > 1]
  return padStr("%dweek%s%dday%s%dh%dm%ds" % (w, wPlural, d,
                                             dPlural, h, m, s), field)

def reportTime(t, options, field=None):
  """ Given t seconds, report back the correct format as string.
  """
  if options.pretty:
    return prettyTime(t, field=field)
  else:
    if field is not None:
      return "%*.2f" % (field, t)
    else:
      return "%.2f" % t

def reportMemory(k, options, field=None, isBytes=False):
  """ Given k kilobytes, report back the correct format as string.
  """
  if options.pretty:
    return prettyMemory(k, field=field, isBytes=isBytes)
  else:
    if isBytes:
      k /= 1024
    if field is not None:
      return "%*gK" % (field, k)
    else:
      return "%gK" % k

def reportNumber(n, options, field=None):
  """ Given n an integer, report back the correct format as string.
  """
  if field is not None:
    return "%*g" % (field, n)
  else:
    return "%g" % n

def refineData(root, options):
  """ walk the root and gather up the important bits.
  """
  slave = JTTag(root.find("slave"))
  target = JTTag(root.find("target"))
  targetTypesTree = root.find("target_types")
  targetTypes = []
  for child in targetTypesTree:
    targetTypes.append(JTTag(child))
  return root, slave, target, targetTypes

def sprintTag(key, tag, options):
  """ Print out a JTTag()
  """
  header = "  %7s " % "Count"
  sub_header = "  %7s " % "n"
  tag_str = "  %s" % reportNumber(tag.total_number, options, field=7)
  out_str = ""
  if key == "target":
    out_str += " %-12s | %7s%7s%7s%7s " % ("Slave Jobs", "min",
                                           "med", "ave", "max")
    slave_str = "%s| \n" % (" " * 14)
    for t in [tag.min_number_per_slave, tag.median_number_per_slave,
              tag.average_number_per_slave, tag.max_number_per_slave]:
      slave_str += reportNumber(t, options, field=7)
    out_str += slave_str + "\n"
  if "time" in options.categories:
    header += "| %40s " % "Time"
    sub_header += "| %10s%10s%10s%10s " % ("min", "med", "ave", "max")
    tag_str += " | "
    for t in [tag.min_time, tag.median_time,
              tag.average_time, tag.max_time]:
      tag_str += reportTime(t, options, field=10)
  if "clock" in options.categories:
    header += "| %40s " % "Clock"
    sub_header += "| %10s%10s%10s%10s " % ("min", "med", "ave", "max")
    tag_str += " | "
    for t in [tag.min_clock, tag.median_clock,
              tag.average_clock, tag.max_clock]:
      tag_str += reportTime(t, options, field=10)
  if "wait" in options.categories:
    header += "| %40s " % "Wait"
    sub_header += "| %10s%10s%10s%10s " % ("min", "med", "ave", "max")
    tag_str += " | "
    for t in [tag.min_wait, tag.median_wait,
              tag.average_wait, tag.max_wait]:
      tag_str += reportTime(t, options, field=10)
  if "memory" in options.categories:
    header += "| %40s " % "Memory"
    sub_header += "| %10s%10s%10s%10s " % ("min", "med", "ave", "max")
    tag_str += " | "
    for t in [tag.min_memory, tag.median_memory,
              tag.average_memory, tag.max_memory]:
      tag_str += reportMemory(t, options, field=10)
  out_str += header + "\n"
  out_str += sub_header + "\n"
  out_str += tag_str + "\n"
  return out_str

def get(tree, name):
  """ Return a float value attribute NAME from TREE.
  """
  if name in tree.attrib:
    value = tree.attrib[name]
  else:
    return float("nan")
  try:
    a = float(value)
  except ValueError:
    a = float("nan")
  return a

def sortTargets(targetTypes, options):
  """ Return a targetTypes all sorted.
  """
  if options.sortby == "time":
    return sorted(targetTypes, key=lambda tag: tag.median_time,
                  reverse=options.reverse_sort)
  elif options.sortby == "alpha":
    return sorted(targetTypes, key=lambda tag: tag.name,
                  reverse=options.reverse_sort)
  elif options.sortby == "clock":
    return sorted(targetTypes, key=lambda tag: tag.median_clock,
                  reverse=options.reverse_sort)
  elif options.sortby == "wait":
    return sorted(targetTypes, key=lambda tag: tag.median_wait,
                  reverse=options.reverse_sort)
  elif options.sortby == "memory":
    return sorted(targetTypes, key=lambda tag: tag.median_memory,
                  reverse=options.reverse_sort)
  elif options.sortby == "count":
    return sorted(targetTypes, key=lambda tag: tag.total_number,
                  reverse=options.reverse_sort)

def reportPrettyData(root, slave, target, target_types, options):
  """ print the important bits out.
  """
  out_str = "Batch System: %s\n" % root.attrib["batch_system"]
  out_str += ("Default CPU: %s  Default Memory: %s\n"
              "Job Time: %s  Max CPUs: %s  Max Threads: %s\n" % (
      reportNumber(get(root, "default_cpu"), options),
      reportMemory(get(root, "default_memory"), options, isBytes=True),
      reportTime(get(root, "job_time"), options),
      reportNumber(get(root, "max_cpus"), options),
      reportNumber(get(root, "max_threads"), options),
      ))
  out_str += ("Total Clock: %s  Total Runtime: %s\n" % (
          reportTime(get(root, "total_clock"), options),
          reportTime(get(root, "total_run_time"), options),
          ))
  out_str += "Slave\n"
  out_str += sprintTag("slave", slave, options)
  out_str += "Target\n"
  out_str += sprintTag("target", target, options)
  target_types = sortTargets(target_types, options)
  for t in target_types:
    out_str += " %s\n" % t.name
    out_str += sprintTag(t.name, t, options)
  return out_str

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

def reportData(xml_tree, options):
    # Now dump it all out to file
    if options.raw:
        out_str = prettyXml(xml_tree)
    else:
        root, slave, target, target_types = refineData(xml_tree, options)
        out_str = reportPrettyData(root, slave, target, target_types, options)
    if options.outputFile != None:
        fileHandle = open(options.outputFile, "w")
        fileHandle.write(out_str)
        fileHandle.close()
    # Now dump onto the screen
    print out_str

def main():
    """ Reports stats on the job-tree, use with --stats option to jobTree.
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

if __name__ == "__main__":
    _test()
    main()
