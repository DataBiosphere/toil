# Copyright (C) 2015-2021 Regents of the University of California
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
"""Reports statistical data about a given Toil workflow."""
import json
import logging
from functools import partial
from argparse import ArgumentParser, Namespace
from typing import Optional, Dict, List, Callable, TextIO, Any

from toil.job import Job
from toil.common import Config, Toil, parser_with_common_options
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.lib.expando import Expando
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)


class ColumnWidths:
    """
    Convenience object that stores the width of columns for printing. Helps make things pretty.
    """
    def __init__(self) -> None:
        self.categories = ["time", "clock", "wait", "memory"]
        self.fields_count = ["count", "min", "med", "ave", "max", "total"]
        self.fields = ["min", "med", "ave", "max", "total"]
        self.data: Dict[str, int] = {}
        for category in self.categories:
            for field in self.fields_count:
                self.setWidth(category, field, 8)

    def title(self, category: str) -> int:
        """ Return the total printed length of this category item.
        """
        return sum([self.getWidth(category, x) for x in self.fields])

    def getWidth(self, category: str, field: str ) -> int:
        category = category.lower()
        return self.data["%s_%s" % (category, field)]

    def setWidth(self, category: str, field: str, width: int) -> None:
        category = category.lower()
        self.data["%s_%s" % (category, field)] = width

    def report(self) -> None:
        for c in self.categories:
            for f in self.fields:
                print('%s %s %d' % (c, f, self.getWidth(c, f)))


def padStr(s: str, field: Optional[int] = None) -> str:
    """Pad the beginning of a string with spaces, if necessary."""
    if field is None or len(s) >= field:
        return s
    else:
        return " " * (field - len(s)) + s


def prettyMemory(k: float, field: Optional[int] = None, isBytes: bool = False) -> str:
    """Given input k as kilobytes, return a nicely formatted string."""
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

    # due to https://stackoverflow.com/questions/47149154
    assert False


def prettyTime(t: float, field: Optional[int] = None) -> str:
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
                   - (h * 60. * 60.)) / 60.)
        s = t % 60
        dPlural = pluralDict[d > 1]
        return padStr("%dday%s%dh%dm%ds" % (d, dPlural, h, m, s), field)
    w = floor(t / 7. / 24. / 60. / 60.)
    d = floor((t - (w * 7 * 24 * 60 * 60)) / 24. / 60. / 60.)
    h = floor((t - (w * 7. * 24. * 60. * 60.)
                 - (d * 24. * 60. * 60.))
              / 60. / 60.)
    m = floor((t - (w * 7. * 24. * 60. * 60.)
                 - (d * 24. * 60. * 60.)
                 - (h * 60. * 60.)) / 60.)
    s = t % 60
    wPlural = pluralDict[w > 1]
    dPlural = pluralDict[d > 1]
    return padStr("%dweek%s%dday%s%dh%dm%ds" % (w, wPlural, d,
                                                dPlural, h, m, s), field)


def reportTime(t: float, options: Namespace, field: Optional[int] = None) -> str:
    """Given t seconds, report back the correct format as string."""
    if options.pretty:
        return prettyTime(t, field=field)
    elif field is not None:
        return "%*.2f" % (field, t)
    return "%.2f" % t


def reportMemory(k: float, options: Namespace, field: Optional[int] = None, isBytes: bool = False) -> str:
    """Given k kilobytes, report back the correct format as string."""
    if options.pretty:
        return prettyMemory(int(k), field=field, isBytes=isBytes)
    else:
        if isBytes:
            k /= 1024.
        if field is not None:
            return "%*dK" % (field - 1, k)  # -1 for the "K"
        else:
            return "%dK" % int(k)


def reportNumber(n: float, field: Optional[int] = None) -> str:
    """Given n an integer, report back the correct format as string."""
    return "%*g" % (field, n) if field else "%g" % n


def sprintTag(key: str, tag: Expando, options: Namespace, columnWidths: Optional[ColumnWidths] = None) -> str:
    """ Generate a pretty-print ready string from a JTTag().
    """
    if columnWidths is None:
        columnWidths = ColumnWidths()
    header = "  %7s " % decorateTitle("Count", options)
    sub_header = "  %7s " % "n"
    tag_str = f"  {reportNumber(n=tag.total_number, field=7)}"
    out_str = ""
    if key == "job":
        out_str += " %-12s | %7s%7s%7s%7s\n" % ("Worker Jobs", "min",
                                           "med", "ave", "max")
        worker_str = "%s| " % (" " * 14)
        for t in [tag.min_number_per_worker, tag.median_number_per_worker,
                  tag.average_number_per_worker, tag.max_number_per_worker]:
            worker_str += reportNumber(n=t, field=7)
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

def decorateTitle(title: str, options: Namespace) -> str:
    """ Add a marker to TITLE if the TITLE is sorted on.
    """
    if title.lower() == options.sortCategory:
        return "%s*" % title
    else:
        return title

def decorateSubHeader(title: str, columnWidths: ColumnWidths, options: Namespace) -> str:
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


def get(tree: Expando, name: str) -> float:
    """Return a float value attribute NAME from TREE."""
    try:
        return float(tree.get(name, "nan"))
    except ValueError:
        return float("nan")


def sortJobs(jobTypes: List[Any], options: Namespace) -> List[Any]:
    """Return a jobTypes all sorted."""
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
            # due to https://github.com/python/mypy/issues/9656
            key=lambda tag: getattr(tag, "%s_%s" # type: ignore
                                    % (sortField, options.sortCategory)),
            reverse=options.sortReverse)
    elif options.sortCategory == "alpha":
        return sorted(
            jobTypes, key=lambda tag: tag.name, # type: ignore
            reverse=options.sortReverse)
    elif options.sortCategory == "count":
        return sorted(jobTypes, key=lambda tag: tag.total_number, # type: ignore
                      reverse=options.sortReverse)

    # due to https://stackoverflow.com/questions/47149154
    assert False


def reportPrettyData(root: Expando, worker: List[Job], job: List[Job], job_types: List[Any], options: Namespace) -> str:
    """Print the important bits out."""
    out_str = "Batch System: %s\n" % root.batch_system
    out_str += ("Default Cores: %s  Default Memory: %s\n"
                "Max Cores: %s\n" % (
        reportNumber(n=get(root, "default_cores")),
        reportMemory(get(root, "default_memory"), options, isBytes=True),
        reportNumber(n=get(root, "max_cores")),
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


def computeColumnWidths(job_types: List[Any], worker: List[Job], job: List[Job], options: Expando) -> ColumnWidths:
    """ Return a ColumnWidths() object with the correct max widths.
    """
    cw = ColumnWidths()
    for t in job_types:
        updateColumnWidths(t, cw, options)
    updateColumnWidths(worker, cw, options)
    updateColumnWidths(job, cw, options)
    return cw


def updateColumnWidths(tag: Expando, cw: ColumnWidths, options: Expando) -> None:
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


def buildElement(element: Expando, items: List[Job], itemName: str) -> Expando:
    """ Create an element for output.
    """
    def assertNonnegative(i: float, name: str) -> float:
        if i < 0:
            raise RuntimeError("Negative value %s reported for %s" %(i,name) )
        else:
            return float(i)

    itemTimes = []
    itemClocks = []
    itemMemory = []
    for item in items:
        # If something lacks an entry, assume it used none of that thing.
        # This avoids crashing when jobs e.g. aren't done.
        itemTimes.append(assertNonnegative(float(item.get("time", 0)), "time"))
        itemClocks.append(assertNonnegative(float(item.get("clock", 0)), "clock"))
        itemMemory.append(assertNonnegative(float(item.get("memory", 0)), "memory"))
    assert len(itemClocks) == len(itemTimes) == len(itemMemory)

    itemWaits=[]
    for index in range(0,len(itemTimes)):
        itemWaits.append(itemTimes[index] - itemClocks[index])

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
        median_time=float(itemTimes[len(itemTimes) // 2]),
        average_time=float(sum(itemTimes) / len(itemTimes)),
        min_time=float(min(itemTimes)),
        max_time=float(max(itemTimes)),
        total_clock=float(sum(itemClocks)),
        median_clock=float(itemClocks[len(itemClocks) // 2]),
        average_clock=float(sum(itemClocks) / len(itemClocks)),
        min_clock=float(min(itemClocks)),
        max_clock=float(max(itemClocks)),
        total_wait=float(sum(itemWaits)),
        median_wait=float(itemWaits[len(itemWaits) // 2]),
        average_wait=float(sum(itemWaits) / len(itemWaits)),
        min_wait=float(min(itemWaits)),
        max_wait=float(max(itemWaits)),
        total_memory=float(sum(itemMemory)),
        median_memory=float(itemMemory[len(itemMemory) // 2]),
        average_memory=float(sum(itemMemory) / len(itemMemory)),
        min_memory=float(min(itemMemory)),
        max_memory=float(max(itemMemory)),
        name=itemName
    )
    return element[itemName]


def createSummary(element: Expando, containingItems: List[Job], containingItemName: str, getFn: Callable[[Job], List[Optional[Job]]]) -> None:
    itemCounts = [len(getFn(containingItem)) for
                  containingItem in containingItems]
    itemCounts.sort()
    if len(itemCounts) == 0:
        itemCounts.append(0)
    element["median_number_per_%s" % containingItemName] = itemCounts[len(itemCounts) // 2]
    element["average_number_per_%s" % containingItemName] = float(sum(itemCounts) / len(itemCounts))
    element["min_number_per_%s" % containingItemName] = min(itemCounts)
    element["max_number_per_%s" % containingItemName] = max(itemCounts)


def getStats(jobStore: AbstractJobStore) -> Expando:
    """ Collect and return the stats and config data.
    """
    def aggregateStats(fileHandle: TextIO, aggregateObject: Expando) -> None:
        try:
            stats = json.load(fileHandle, object_hook=Expando)
            for key in list(stats.keys()):
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


def processData(config: Config, stats: Expando) -> Expando:
    """
    Collate the stats and report
    """
    if 'total_time' not in stats or 'total_clock' not in stats:
        # toil job not finished yet
        stats.total_time = [0.0]
        stats.total_clock = [0.0]

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
    worker = [_f for _f in getattr(stats, 'workers', []) if _f]
    jobs = [_f for _f in getattr(stats, 'jobs', []) if _f]
    jobs = [item for sublist in jobs for item in sublist]

    def fn4(job: Job) -> List[Optional[Job]]:
        try:
            return list(jobs)
        except TypeError:
            return []

    buildElement(collatedStatsTag, worker, "worker")
    createSummary(buildElement(collatedStatsTag, jobs, "jobs"),
                  getattr(stats, 'workers', []), "worker", fn4)
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


def reportData(tree: Expando, options: Namespace) -> None:
    # Now dump it all out to file
    if options.raw:
        out_str = json.dumps(tree, indent=4, separators=(',', ': '))
    else:
        out_str = reportPrettyData(tree, tree.worker, tree.jobs, tree.job_types.values(), options)
    if options.outputFile is not None:
        fileHandle = open(options.outputFile, "w")
        fileHandle.write(out_str)
        fileHandle.close()
    # Now dump onto the screen
    print(out_str)


category_choices = ["time", "clock", "wait", "memory"]
sort_category_choices = ["time", "clock", "wait", "memory", "alpha", "count"]
sort_field_choices = ['min', 'med', 'ave', 'max', 'total']


def add_stats_options(parser: ArgumentParser) -> None:
    parser.add_argument("--outputFile", dest="outputFile", default=None, help="File in which to write results.")
    parser.add_argument("--raw", action="store_true", default=False, help="Return raw json data.")
    parser.add_argument("--pretty", "--human", action="store_true", default=False,
                        help="if not raw, prettify the numbers to be human readable.")
    parser.add_argument("--sortReverse", "--reverseSort", default=False, action="store_true", help="Reverse sort.")
    parser.add_argument("--categories", default=','.join(category_choices), type=str,
                        help=f"Comma separated list of any of the following: {category_choices}.")
    parser.add_argument("--sortCategory", default="time", choices=sort_category_choices,
                        help=f"How to sort job categories.  Choices: {sort_category_choices}. Default: time.")
    parser.add_argument("--sortField", default="med", choices=sort_field_choices,
                        help=f"How to sort job fields.  Choices: {sort_field_choices}. Default: med.")


def main() -> None:
    """Reports stats on the workflow, use with --stats option to toil."""
    parser = parser_with_common_options()
    add_stats_options(parser)
    options = parser.parse_args()

    for c in options.categories.split(","):
        if c.strip() not in category_choices:
            raise ValueError(f'{c} not in {category_choices}!')
    options.categories = [x.strip().lower() for x in options.categories.split(",")]

    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)
    stats = getStats(jobStore)
    collatedStatsTag = processData(jobStore.config, stats)
    reportData(collatedStatsTag, options)
