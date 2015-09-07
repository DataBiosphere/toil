# Copyright (C) 2015 UCSC Computational Genomics Lab
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


""" Reports data about the given toil run.
"""
from __future__ import absolute_import
import logging
import sys
import xml.etree.ElementTree as ET  # not cElementTree so as to allow caching
from xml.dom import minidom  # For making stuff pretty
import os
from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import loadJobStore
from toil.version import version

logger = logging.getLogger( __name__ )

class JTTag(object):
    """ Convenience object that stores xml attributes as object attributes.
    """
    def __init__(self, tree):
        """ Given an ElementTree tag, build a convenience object.
        """
        for name in ["total_time", "median_clock", "total_memory",
                     "median_wait", "total_number", "average_time",
                     "median_memory", "min_number_per_worker", "average_wait",
                     "total_clock", "median_time", "min_time", "min_wait",
                     "max_clock", "max_wait", "total_wait", "min_clock",
                     "average_memory", "max_number_per_worker", "max_memory",
                     "average_memory", "max_number_per_worker", "max_memory",
                     "median_number_per_worker", "average_number_per_worker",
                     "max_time", "average_clock", "min_memory", "min_clock",
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

class ColumnWidths(object):
    """ Convenience object that stores the width of columns for printing.
    Helps make things pretty.
    """
    def __init__(self):
        self.categories = ["time", "clock", "wait", "memory"]
        self.fields_count = ["count", "min", "med", "ave", "max", "total"]
        self.fields = ["min", "med", "ave", "max", "total"]
        self.data = {}
        for category in self.categories:
            for field in self.fields_count:
                self.setWidth(category, field, 8)
    def title(self, category):
        """ Return the total printed length of this category item.
        """
        return sum(
            map(lambda x: self.getWidth(category, x), self.fields))
    def getWidth(self, category, field):
        category = category.lower()
        return self.data["%s_%s" % (category, field)]
    def setWidth(self, category, field, width):
        category = category.lower()
        self.data["%s_%s" % (category, field)] = width
    def report(self):
        for c in self.categories:
            for f in self.fields:
                print '%s %s %d' % (c, f, self.getWidth(c, f))

def initializeOptions(parser):
    ##########################################
    # Construct the arguments.
    ##########################################
    parser.add_argument("jobStore", type=str,
              help=("Store in which to place job management files \
              and the global accessed temporary files"
              "(If this is a file path this needs to be globally accessible "
              "by all machines running jobs).\n"
              "If the store already exists and restart is false an"
              " ExistingJobStoreException exception will be thrown."))
    parser.add_argument("--outputFile", dest="outputFile", default=None,
                      help="File in which to write results")
    parser.add_argument("--raw", action="store_true", default=False,
                      help="output the raw xml data.")
    parser.add_argument("--pretty", "--human", action="store_true", default=False,
                      help=("if not raw, prettify the numbers to be "
                            "human readable."))
    parser.add_argument("--categories",
                      help=("comma separated list from [time, clock, wait, "
                            "memory]"))
    parser.add_argument("--sortCategory", default="time",
                      help=("how to sort Job list. may be from [alpha, "
                            "time, clock, wait, memory, count]. "
                            "default=%(default)s"))
    parser.add_argument("--sortField", default="med",
                      help=("how to sort Job list. may be from [min, "
                            "med, ave, max, total]. "
                            "default=%(default)s"))
    parser.add_argument("--sortReverse", "--reverseSort", default=False,
                      action="store_true",
                      help="reverse sort order.")
    parser.add_argument("--version", action='version', version=version)
    #parser.add_option("--cache", default=False, action="store_true",
    #                  help="stores a cache to speed up data display.")

def checkOptions(options, parser):
    """ Check options, throw parser.error() if something goes wrong
    """
    logger.info("Parsed arguments")
    logger.info("Checking if we have files for toil")
    if options.jobStore == None:
        parser.error("Specify --jobStore")
    defaultCategories = ["time", "clock", "wait", "memory"]
    if options.categories is None:
        options.categories = defaultCategories
    else:
        options.categories = map(lambda x: x.lower(),
                                 options.categories.split(","))
    for c in options.categories:
        if c not in defaultCategories:
            parser.error("Unknown category %s. Must be from %s"
                         % (c, str(defaultCategories)))
    extraSort = ["count", "alpha"]
    if options.sortCategory is not None:
        if (options.sortCategory not in defaultCategories and
            options.sortCategory not in extraSort):
            parser.error("Unknown --sortCategory %s. Must be from %s"
                         % (options.sortCategory,
                            str(defaultCategories + extraSort)))
    sortFields = ["min", "med", "ave", "max", "total"]
    if options.sortField is not None:
        if (options.sortField not in sortFields):
            parser.error("Unknown --sortField %s. Must be from %s"
                         % (options.sortField, str(sortFields)))
    logger.info("Checked arguments")

def prettyXml(elem):
    """ Return a pretty-printed XML string for the ElementTree Element.
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
        return prettyMemory(int(k), field=field, isBytes=isBytes)
    else:
        if isBytes:
            k /= 1024.
        if field is not None:
            return "%*dK" % (field - 1, k)  # -1 for the "K"
        else:
            return "%dK" % int(k)

def reportNumber(n, options, field=None):
    """ Given n an integer, report back the correct format as string.
    """
    if field is not None:
        return "%*g" % (field, n)
    else:
        return "%g" % n

def refineData(root, options):
    """ walk down from the root and gather up the important bits.
    """
    worker = JTTag(root.find("worker"))
    job = JTTag(root.find("job"))
    jobTypesTree = root.find("job_types")
    jobTypes = []
    for child in jobTypesTree:
        jobTypes.append(JTTag(child))
    return root, worker, job, jobTypes

def sprintTag(key, tag, options, columnWidths=None):
    """ Generate a pretty-print ready string from a JTTag().
    """
    if columnWidths is None:
        columnWidths = ColumnWidths()
    header = "  %7s " % decorateTitle("Count", options)
    sub_header = "  %7s " % "n"
    tag_str = "  %s" % reportNumber(tag.total_number, options, field=7)
    out_str = ""
    if key == "job":
        out_str += " %-12s | %7s%7s%7s%7s\n" % ("Worker Jobs", "min",
                                           "med", "ave", "max")
        worker_str = "%s| " % (" " * 14)
        for t in [tag.min_number_per_worker, tag.median_number_per_worker,
                  tag.average_number_per_worker, tag.max_number_per_worker]:
            worker_str += reportNumber(t, options, field=7)
        out_str += worker_str + "\n"
    if "time" in options.categories:
        header += "| %*s " % (columnWidths.title("time"),
                              decorateTitle("Time", options))
        sub_header += decorateSubHeader("Time", columnWidths, options)
        tag_str += " | "
        for t, width in [
            (tag.min_time, columnWidths.getWidth("time", "min")),
            (tag.median_time, columnWidths.getWidth("time", "med")),
            (tag.average_time, columnWidths.getWidth("time", "ave")),
            (tag.max_time, columnWidths.getWidth("time", "max")),
            (tag.total_time, columnWidths.getWidth("time", "total")),
            ]:
            tag_str += reportTime(t, options, field=width)
    if "clock" in options.categories:
        header += "| %*s " % (columnWidths.title("clock"),
                              decorateTitle("Clock", options))
        sub_header += decorateSubHeader("Clock", columnWidths, options)
        tag_str += " | "
        for t, width in [
            (tag.min_clock, columnWidths.getWidth("clock", "min")),
            (tag.median_clock, columnWidths.getWidth("clock", "med")),
            (tag.average_clock, columnWidths.getWidth("clock", "ave")),
            (tag.max_clock, columnWidths.getWidth("clock", "max")),
            (tag.total_clock, columnWidths.getWidth("clock", "total")),
            ]:
            tag_str += reportTime(t, options, field=width)
    if "wait" in options.categories:
        header += "| %*s " % (columnWidths.title("wait"),
                              decorateTitle("Wait", options))
        sub_header += decorateSubHeader("Wait", columnWidths, options)
        tag_str += " | "
        for t, width in [
            (tag.min_wait, columnWidths.getWidth("wait", "min")),
            (tag.median_wait, columnWidths.getWidth("wait", "med")),
            (tag.average_wait, columnWidths.getWidth("wait", "ave")),
            (tag.max_wait, columnWidths.getWidth("wait", "max")),
            (tag.total_wait, columnWidths.getWidth("wait", "total")),
            ]:
            tag_str += reportTime(t, options, field=width)
    if "memory" in options.categories:
        header += "| %*s " % (columnWidths.title("memory"),
                              decorateTitle("Memory", options))
        sub_header += decorateSubHeader("Memory", columnWidths, options)
        tag_str += " | "
        for t, width in [
            (tag.min_memory, columnWidths.getWidth("memory", "min")),
            (tag.median_memory, columnWidths.getWidth("memory", "med")),
            (tag.average_memory, columnWidths.getWidth("memory", "ave")),
            (tag.max_memory, columnWidths.getWidth("memory", "max")),
            (tag.total_memory, columnWidths.getWidth("memory", "total")),
            ]:
            tag_str += reportMemory(t, options, field=width)
    out_str += header + "\n"
    out_str += sub_header + "\n"
    out_str += tag_str + "\n"
    return out_str

def decorateTitle(title, options):
    """ Add a marker to TITLE if the TITLE is sorted on.
    """
    if title.lower() == options.sortCategory:
        return "%s*" % title
    else:
        return title

def decorateSubHeader(title, columnWidths, options):
    """ Add a marker to the correct field if the TITLE is sorted on.
    """
    title = title.lower()
    if title != options.sortCategory:
        s = "| %*s%*s%*s%*s%*s " % (
            columnWidths.getWidth(title, "min"), "min",
            columnWidths.getWidth(title, "med"), "med",
            columnWidths.getWidth(title, "ave"), "ave",
            columnWidths.getWidth(title, "max"), "max",
            columnWidths.getWidth(title, "total"), "total")
        return s
    else:
        s = "| "
        for field, width in [("min", columnWidths.getWidth(title, "min")),
                             ("med", columnWidths.getWidth(title, "med")),
                             ("ave", columnWidths.getWidth(title, "ave")),
                             ("max", columnWidths.getWidth(title, "max")),
                             ("total", columnWidths.getWidth(title, "total"))]:
            if options.sortField == field:
                s += "%*s*" % (width - 1, field)
            else:
                s += "%*s" % (width, field)
        s += " "
        return s

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

def sortJobs(jobTypes, options):
    """ Return a jobTypes all sorted.
    """
    longforms = {"med": "median",
                 "ave": "average",
                 "min": "min",
                 "total": "total",
                 "max": "max",}
    sortField = longforms[options.sortField]
    if (options.sortCategory == "time" or
        options.sortCategory == "clock" or
        options.sortCategory == "wait" or
        options.sortCategory == "memory"
        ):
        return sorted(
            jobTypes,
            key=lambda tag: getattr(tag, "%s_%s"
                                    % (sortField, options.sortCategory)),
            reverse=options.sortReverse)
    elif options.sortCategory == "alpha":
        return sorted(
            jobTypes, key=lambda tag: tag.name,
            reverse=options.sortReverse)
    elif options.sortCategory == "count":
        return sorted(jobTypes, key=lambda tag: tag.total_number,
                      reverse=options.sortReverse)

def reportPrettyData(root, worker, job, job_types, options):
    """ print the important bits out.
    """
    out_str = "Batch System: %s\n" % root.attrib["batch_system"]
    out_str += ("Default Cores: %s  Default Memory: %s\n"
                "Max Cores: %s  Max Threads: %s\n" % (
        reportNumber(get(root, "default_cores"), options),
        reportMemory(get(root, "default_memory"), options, isBytes=True),
        reportNumber(get(root, "max_cores"), options),
        reportNumber(get(root, "max_threads"), options),
        ))
    out_str += ("Total Clock: %s  Total Runtime: %s\n" % (
        reportTime(get(root, "total_clock"), options),
        reportTime(get(root, "total_run_time"), options),
        ))
    job_types = sortJobs(job_types, options)
    columnWidths = computeColumnWidths(job_types, worker, job, options)
    out_str += "Worker\n"
    out_str += sprintTag("worker", worker, options, columnWidths=columnWidths)
    out_str += "Job\n"
    out_str += sprintTag("job", job, options, columnWidths=columnWidths)
    for t in job_types:
        out_str += " %s\n" % t.name
        out_str += sprintTag(t.name, t, options, columnWidths=columnWidths)
    return out_str

def computeColumnWidths(job_types, worker, job, options):
    """ Return a ColumnWidths() object with the correct max widths.
    """
    cw = ColumnWidths()
    for t in job_types:
        updateColumnWidths(t, cw, options)
    updateColumnWidths(worker, cw, options)
    updateColumnWidths(job, cw, options)
    return cw

def updateColumnWidths(tag, cw, options):
    """ Update the column width attributes for this tag's fields.
    """
    longforms = {"med": "median",
                 "ave": "average",
                 "min": "min",
                 "total": "total",
                 "max": "max",}
    for category in ["time", "clock", "wait", "memory"]:
        if category in options.categories:
            for field in ["min", "med", "ave", "max", "total"]:
                t = getattr(tag, "%s_%s" % (longforms[field], category))
                if category in ["time", "clock", "wait"]:
                    s = reportTime(t, options,
                                   field=cw.getWidth(category, field)).strip()
                else:
                    s = reportMemory(t, options,
                                     field=cw.getWidth(category, field)).strip()
                if len(s) >= cw.getWidth(category, field):
                    # this string is larger than max, width must be increased
                    cw.setWidth(category, field, len(s) + 1)

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

def getStats(options):
    """ Collect and return the stats and config data.
    """
    
    jobStore = loadJobStore(options.jobStore)
    try:
        with jobStore.readSharedFileStream("statsAndLogging.xml") as fH:
            stats = ET.parse(fH).getroot() # Try parsing the whole file.
    except ET.ParseError: # If it doesn't work then we build the file incrementally
        sys.stderr.write("The toil stats file is incomplete or corrupt, "
                         "we'll try instead to parse what's in the file "
                         "incrementally until we reach an error.\n")
        with jobStore.readSharedFileStream("statsAndLogging.xml") as fH:
            stats = ET.Element("stats")
            try:
                for event, elem in ET.iterparse(fH):
                    if elem.tag == 'worker':
                        stats.append(elem)
            except ET.ParseError:
                # TODO: Document why parse errors are to be expected
                pass # Do nothing at this point
    return stats

def processData(config, stats, options):
    ##########################################
    # Collate the stats and report
    ##########################################
    if stats.find("total_time") == None:  # Hack to allow unfinished toils.
        ET.SubElement(stats, "total_time", { "time":"0.0", "clock":"0.0"})

    collatedStatsTag = ET.Element(
        "collated_stats",
        {"total_run_time":stats.find("total_time").attrib["time"],
         "total_clock":stats.find("total_time").attrib["clock"],
         "batch_system":config.batchSystem,
         "default_memory":str(config.defaultMemory),
         "default_cores":str(config.defaultCores),
         "max_cores":str(config.maxCores)})

    # Add worker info
    workers = stats.findall("worker")
    buildElement(collatedStatsTag, workers, "worker")

    # Add aggregated job info
    jobs = []
    for worker in workers:
        jobs += worker.findall("job")
    def fn4(job):
        return list(worker.findall("job"))
    createSummary(buildElement(collatedStatsTag, jobs, "job"),
                  workers, "worker", fn4)
    # Get info for each job
    jobNames = set()
    for job in jobs:
        jobNames.add(job.attrib["class"])
    jobTypesTag = ET.SubElement(collatedStatsTag, "job_types")
    for jobName in jobNames:
        jobTypes = [ job for job in jobs
                        if job.attrib["class"] == jobName ]
        # FIXME: unused assignment
        jobTypeTag = buildElement(jobTypesTag, jobTypes, jobName)
    return collatedStatsTag

def reportData(xml_tree, options):
    # Now dump it all out to file
    if options.raw:
        out_str = prettyXml(xml_tree)
    else:
        root, worker, job, job_types = refineData(xml_tree, options)
        out_str = reportPrettyData(root, worker, job, job_types, options)
    if options.outputFile != None:
        fileHandle = open(options.outputFile, "w")
        fileHandle.write(out_str)
        fileHandle.close()
    # Now dump onto the screen
    print out_str

"""
def getNullFile():
    # Guaranteed to return a valid path to a file that does not exist.
    charSet = string.ascii_lowercase + "0123456789"
    prefix = os.getcwd()
    nullFile = "null_%s" % "".join(choice(charSet) for x in xrange(6))
    while os.path.exists(os.path.join(prefix, nullFile)):
        nullFile = "null_%s" % "".join(choice(charSet) for x in xrange(6))
    return os.path.join(os.getcwd(), nullFile)

def getPreferredStatsCacheFileName(options):
    # Determine if the toil or the os.getcwd() version should be used.
    #If no good option exists, return a nonexistent file path.
    #Note you MUST check to see if the return value exists before using.
    
    null_file = getNullFile()
    location_jt = getStatsCacheFileName(options.jobStore)
    location_local = os.path.abspath(os.path.join(os.getcwd(),
                                                  ".stats_cache.pickle"))
    # start by looking for the current directory cache.
    if os.path.exists(location_local):
        loc_file = open(location_local, "r")
        data, loc = cPickle.load(loc_file)
        if getStatsFileName(options.jobStore) != loc:
            # the local cache is from looking up a *different* toil
            location_local = null_file
    if os.path.exists(location_jt) and not os.path.exists(location_local):
        # use the toil directory version
        return location_jt
    elif not os.path.exists(location_jt) and os.path.exists(location_local):
        # use the os.getcwd() version
        return location_local
    elif os.path.exists(location_jt) and os.path.exists(location_local):
        # check file modify times and use the most recent version
        mtime_jt = os.path.getmtime(location_jt)
        mtime_local = os.path.getmtime(location_local)
        if mtime_jt > mtime_local:
            return location_jt
        else:
            return location_local
    else:
        return null_file

def unpackData(options):
    #unpackData() opens up the pickle of the last run and pulls out
    #all the relevant data.
    
    cache_file = getPreferredStatsCacheFileName(options)
    if not os.path.exists(cache_file):
        return None
    if os.path.exists(cache_file):
        f = open(cache_file, "r")
        try:
            data, location = cPickle.load(f)
        except EOFError:
            # bad cache.
            return None
        finally:
            f.close()
        if location == getStatsFileName(options.jobStore):
            return data
    return None

def packData(data, options):
    # packData stores all of the data in the appropriate pickle cache file.
    stats_file = getStatsFileName(options.jobStore)
    cache_file = getStatsCacheFileName(options.jobStore)
    try:
        # try to write to the toil directory
        payload = (data, stats_file)
        f = open(cache_file, "wb")
        cPickle.dump(payload, f, 2)  # 2 is binary format
        f.close()
    except IOError:
        if not options.cache:
            return
        # try to write to the current working directory only if --cache
        cache_file = os.path.abspath(os.path.join(os.getcwd(),
                                                  ".stats_cache.pickle"))
        payload = (data, stats_file)
        f = open(cache_file, "wb")
        cPickle.dump(payload, f, 2)  # 2 is binary format
        f.close()

def cacheAvailable(options):
    # Check to see if a cache is available, return it.
    if not os.path.exists(getStatsFileName(options.jobStore)):
        return None
    cache_file = getPreferredStatsCacheFileName(options)
    if not os.path.exists(cache_file):
        return None
    # check the modify times on the files, see if the cache should be recomputed
    mtime_stats = os.path.getmtime(getStatsFileName(options.jobStore))
    mtime_cache = os.path.getmtime(cache_file)
    if mtime_stats > mtime_cache:
        # recompute cache
        return None
    # cache is fresh, return the cache
    return unpackData(options)
"""

def main():
    """ Reports stats on the job-tree, use with --stats option to toil.
    """

    parser = getBasicOptionParser()
    initializeOptions(parser)
    options = parseBasicOptions(parser)
    checkOptions(options, parser)
    jobStore = loadJobStore(options.jobStore)
    #collatedStatsTag = cacheAvailable(options)
    #if collatedStatsTag is None:
    stats = getStats(options)
    collatedStatsTag = processData(jobStore.config, stats, options)
    reportData(collatedStatsTag, options)
    #packData(collatedStatsTag, options)

def _test():
    import doctest
    return doctest.testmod()
