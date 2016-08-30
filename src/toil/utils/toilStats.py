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

"""
Reports statistical data about a given Toil workflow.
"""

from __future__ import absolute_import
from functools import partial
import logging
import json
from toil.lib.bioio import getBasicOptionParser
from toil.lib.bioio import parseBasicOptions
from toil.common import Toil, jobStoreLocatorHelp, Config
from toil.version import version
from bd2k.util.expando import Expando

logger = logging.getLogger( __name__ )


class ColumnWidths(object):
    """
    Convenience object that stores the width of columns for printing. Helps make things pretty.
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
    parser.add_argument("jobStore", type=str,
                        help="The location of the job store used by the workflow for which "
                             "statistics should be reported. " + jobStoreLocatorHelp)
    parser.add_argument("--outputFile", dest="outputFile", default=None,
                      help="File in which to write results")
    parser.add_argument("--raw", action="store_true", default=False,
                      help="output the raw json data.")
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

def printJson(elem):
    """ Return a JSON formatted string
    """
    prettyString = json.dumps(elem, indent=4, separators=(',',': '))
    return prettyString

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
    worker = root.worker
    job = root.jobs
    jobTypesTree = root.job_types
    jobTypes = []
    for childName in jobTypesTree:
        jobTypes.append(jobTypesTree[childName])
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
            tag_str += reportMemory(t, options, field=width, isBytes=True)
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
    if name in tree:
        value = tree[name]
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
    out_str = "Batch System: %s\n" % root.batch_system
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
                                     field=cw.getWidth(category, field), isBytes=True).strip()
                if len(s) >= cw.getWidth(category, field):
                    # this string is larger than max, width must be increased
                    cw.setWidth(category, field, len(s) + 1)

def buildElement(element, items, itemName):
    """ Create an element for output.
    """
    def assertNonnegative(i,name):
        if i < 0:
            raise RuntimeError("Negative value %s reported for %s" %(i,name) )
        else:
            return float(i)

    itemTimes = []
    itemClocks = []
    itemMemory = []
    for item in items:
        itemTimes.append(assertNonnegative(item["time"], "time"))
        itemClocks.append(assertNonnegative(item["clock"], "clock"))
        itemMemory.append(assertNonnegative(item["memory"], "memory"))
    assert len(itemClocks) == len(itemTimes) == len(itemMemory)

    itemWaits=[]
    for index in range(0,len(itemTimes)):
        itemWaits.append(itemClocks[index]-itemTimes[index])

    itemWaits.sort()
    itemTimes.sort()
    itemClocks.sort()
    itemMemory.sort()

    if len(itemTimes) == 0:
        itemTimes.append(0)
        itemClocks.append(0)
        itemWaits.append(0)
        itemMemory.append(0)

    element[itemName]=Expando(
        total_number=float(len(items)),
        total_time=float(sum(itemTimes)),
        median_time=float(itemTimes[len(itemTimes)/2]),
        average_time=float(sum(itemTimes)/len(itemTimes)),
        min_time=float(min(itemTimes)),
        max_time=float(max(itemTimes)),
        total_clock=float(sum(itemClocks)),
        median_clock=float(itemClocks[len(itemClocks)/2]),
        average_clock=float(sum(itemClocks)/len(itemClocks)),
        min_clock=float(min(itemClocks)),
        max_clock=float(max(itemClocks)),
        total_wait=float(sum(itemWaits)),
        median_wait=float(itemWaits[len(itemWaits)/2]),
        average_wait=float(sum(itemWaits)/len(itemWaits)),
        min_wait=float(min(itemWaits)),
        max_wait=float(max(itemWaits)),
        total_memory=float(sum(itemMemory)),
        median_memory=float(itemMemory[len(itemMemory)/2]),
        average_memory=float(sum(itemMemory)/len(itemMemory)),
        min_memory=float(min(itemMemory)),
        max_memory=float(max(itemMemory)),
        name=itemName
    )
    return element[itemName]

def createSummary(element, containingItems, containingItemName, getFn):
    itemCounts = [len(getFn(containingItem)) for
                  containingItem in containingItems]
    itemCounts.sort()
    if len(itemCounts) == 0:
        itemCounts.append(0)
    element["median_number_per_%s" % containingItemName] = itemCounts[len(itemCounts) / 2]
    element["average_number_per_%s" % containingItemName] = float(sum(itemCounts)) / len(itemCounts)
    element["min_number_per_%s" % containingItemName] = min(itemCounts)
    element["max_number_per_%s" % containingItemName] = max(itemCounts)


def getStats(jobStore):
    """ Collect and return the stats and config data.
    """
    def aggregateStats(fileHandle,aggregateObject):
        try:
            stats = json.load(fileHandle, object_hook=Expando)
            for key in stats.keys():
                if key in aggregateObject:
                    aggregateObject[key].append(stats[key])
                else:
                    aggregateObject[key]=[stats[key]]
        except ValueError:
            logger.critical("File %s contains corrupted json. Skipping file." % fileHandle)
            pass  # The file is corrupted.

    aggregateObject = Expando()
    callBack = partial(aggregateStats, aggregateObject=aggregateObject)
    jobStore.readStatsAndLogging(callBack, readAll=True)
    return aggregateObject


def processData(config, stats):
    ##########################################
    # Collate the stats and report
    ##########################################
    if stats.get("total_time", None) is None:  # Hack to allow unfinished toils.
        stats.total_time = {"total_time": "0.0", "total_clock": "0.0"}
    else:
        stats.total_time = sum([float(number) for number in stats.total_time])
        stats.total_clock = sum([float(number) for number in stats.total_clock])

    collatedStatsTag = Expando(total_run_time=stats.total_time,
                               total_clock=stats.total_clock,
                               batch_system=config.batchSystem,
                               default_memory=str(config.defaultMemory),
                               default_cores=str(config.defaultCores),
                               max_cores=str(config.maxCores)
                               )

    # Add worker info
    worker = filter(None, stats.workers)
    jobs = filter(None, stats.jobs)
    jobs = [item for sublist in jobs for item in sublist]

    def fn4(job):
        try:
            return list(jobs)
        except TypeError:
            return []

    buildElement(collatedStatsTag, worker, "worker")
    createSummary(buildElement(collatedStatsTag, jobs, "jobs"),
                  stats.workers, "worker", fn4)
    # Get info for each job
    jobNames = set()
    for job in jobs:
        jobNames.add(job.class_name)
    jobTypesTag = Expando()
    collatedStatsTag.job_types = jobTypesTag
    for jobName in jobNames:
        jobTypes = [ job for job in jobs if job.class_name == jobName ]
        buildElement(jobTypesTag, jobTypes, jobName)
    collatedStatsTag.name = "collatedStatsTag"
    return collatedStatsTag

def reportData(tree, options):
    # Now dump it all out to file
    if options.raw:
        out_str = printJson(tree)
    else:
        root, worker, job, job_types = refineData(tree, options)
        out_str = reportPrettyData(root, worker, job, job_types, options)
    if options.outputFile is not None:
        fileHandle = open(options.outputFile, "w")
        fileHandle.write(out_str)
        fileHandle.close()
    # Now dump onto the screen
    print out_str

def main():
    """ Reports stats on the workflow, use with --stats option to toil.
    """
    parser = getBasicOptionParser()
    initializeOptions(parser)
    options = parseBasicOptions(parser)
    checkOptions(options, parser)
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)
    stats = getStats(jobStore)
    collatedStatsTag = processData(jobStore.config, stats)
    reportData(collatedStatsTag, options)
