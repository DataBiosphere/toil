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
from argparse import ArgumentParser, Namespace
from functools import partial
from typing import Any, Callable, Dict, List, Optional, TextIO

from toil.common import Config, Toil, parser_with_common_options
from toil.job import Job
from toil.jobStores.abstractJobStore import AbstractJobStore
from toil.lib.expando import Expando
from toil.statsAndLogging import set_logging_from_options

logger = logging.getLogger(__name__)

# These categories of stat will be reported
CATEGORIES = ["time", "clock", "wait", "memory", "disk"]
# Of those, these are in time
TIME_CATEGORIES = {"time", "clock", "wait"}
# And these are in space
SPACE_CATEGORIES = {"memory", "disk"}
# And of the space ones, these are stored in bytes. The others are stored in KiB.
BYTES_CATEGORIES = {"disk"}
# These categories aren't stored and need to be computed
COMPUTED_CATEGORIES = {"wait"}

LONG_FORMS = {
    "med": "median",
    "ave": "average",
    "min": "min",
    "total": "total",
    "max": "max",
}

class ColumnWidths:
    """
    Convenience object that stores the width of columns for printing. Helps make things pretty.
    """

    def __init__(self) -> None:
        self.categories = CATEGORIES
        self.fields_count = ["count", "min", "med", "ave", "max", "total"]
        self.fields = ["min", "med", "ave", "max", "total"]
        self.data: Dict[str, int] = {}
        for category in self.categories:
            for field in self.fields_count:
                self.setWidth(category, field, 8)

    def title(self, category: str) -> int:
        """Return the total printed length of this category item."""
        return sum(self.getWidth(category, x) for x in self.fields)

    def getWidth(self, category: str, field: str) -> int:
        category = category.lower()
        return self.data[f"{category}_{field}"]

    def setWidth(self, category: str, field: str, width: int) -> None:
        category = category.lower()
        self.data[f"{category}_{field}"] = width

    def report(self) -> None:
        for c in self.categories:
            for f in self.fields:
                print("%s %s %d" % (c, f, self.getWidth(c, f)))


def padStr(s: str, field: Optional[int] = None) -> str:
    """Pad the beginning of a string with spaces, if necessary."""
    if field is None or len(s) >= field:
        return s
    else:
        return " " * (field - len(s)) + s


def prettySpace(k: float, field: Optional[int] = None, isBytes: bool = False) -> str:
    """Given input k as kibibytes, return a nicely formatted string."""
    if isBytes:
        k /= 1024
    if k < 1024:
        return padStr("%gKi" % k, field)
    if k < (1024 * 1024):
        return padStr("%.1fMi" % (k / 1024.0), field)
    if k < (1024 * 1024 * 1024):
        return padStr("%.1fGi" % (k / 1024.0 / 1024.0), field)
    if k < (1024 * 1024 * 1024 * 1024):
        return padStr("%.1fTi" % (k / 1024.0 / 1024.0 / 1024.0), field)
    if k < (1024 * 1024 * 1024 * 1024 * 1024):
        return padStr("%.1fPi" % (k / 1024.0 / 1024.0 / 1024.0 / 1024.0), field)

    # due to https://stackoverflow.com/questions/47149154
    assert False


def prettyTime(t: float, field: Optional[int] = None) -> str:
    """Given input t as seconds, return a nicely formatted string."""
    from math import floor

    pluralDict = {True: "s", False: ""}
    if t < 120:
        return padStr("%ds" % t, field)
    if t < 120 * 60:
        m = floor(t / 60.0)
        s = t % 60
        return padStr("%dm%ds" % (m, s), field)
    if t < 25 * 60 * 60:
        h = floor(t / 60.0 / 60.0)
        m = floor((t - (h * 60.0 * 60.0)) / 60.0)
        s = t % 60
        return padStr("%dh%gm%ds" % (h, m, s), field)
    if t < 7 * 24 * 60 * 60:
        d = floor(t / 24.0 / 60.0 / 60.0)
        h = floor((t - (d * 24.0 * 60.0 * 60.0)) / 60.0 / 60.0)
        m = floor((t - (d * 24.0 * 60.0 * 60.0) - (h * 60.0 * 60.0)) / 60.0)
        s = t % 60
        dPlural = pluralDict[d > 1]
        return padStr("%dday%s%dh%dm%ds" % (d, dPlural, h, m, s), field)
    w = floor(t / 7.0 / 24.0 / 60.0 / 60.0)
    d = floor((t - (w * 7 * 24 * 60 * 60)) / 24.0 / 60.0 / 60.0)
    h = floor(
        (t - (w * 7.0 * 24.0 * 60.0 * 60.0) - (d * 24.0 * 60.0 * 60.0)) / 60.0 / 60.0
    )
    m = floor(
        (
            t
            - (w * 7.0 * 24.0 * 60.0 * 60.0)
            - (d * 24.0 * 60.0 * 60.0)
            - (h * 60.0 * 60.0)
        )
        / 60.0
    )
    s = t % 60
    wPlural = pluralDict[w > 1]
    dPlural = pluralDict[d > 1]
    return padStr("%dweek%s%dday%s%dh%dm%ds" % (w, wPlural, d, dPlural, h, m, s), field)


def reportTime(t: float, options: Namespace, field: Optional[int] = None) -> str:
    """Given t seconds, report back the correct format as string."""
    if options.pretty:
        return prettyTime(t, field=field)
    elif field is not None:
        return "%*.2f" % (field, t)
    return "%.2f" % t


def reportSpace(
    k: float, options: Namespace, field: Optional[int] = None, isBytes: bool = False
) -> str:
    """Given k kibibytes, report back the correct format as string."""
    if options.pretty:
        return prettySpace(int(k), field=field, isBytes=isBytes)
    else:
        if isBytes:
            k /= 1024.0
        if field is not None:
            return "%*dKi" % (field - 2, k)  # -1 for the "K"
        else:
            return "%dKi" % int(k)


def reportNumber(n: float, field: Optional[int] = None) -> str:
    """Given n an integer, report back the correct format as string."""
    return "%*g" % (field, n) if field else "%g" % n


def sprintTag(
    key: str,
    tag: Expando,
    options: Namespace,
    columnWidths: Optional[ColumnWidths] = None,
) -> str:
    """Generate a pretty-print ready string from a JTTag()."""
    if columnWidths is None:
        columnWidths = ColumnWidths()
    header = "  %7s " % decorateTitle("count", "Count", options)
    sub_header = "  %7s " % "n"
    tag_str = f"  {reportNumber(n=tag.total_number, field=7)}"
    out_str = ""
    if key == "job":
        out_str += " {:<12} | {:>7}{:>7}{:>7}{:>7}\n".format(
            "Worker Jobs", "min", "med", "ave", "max"
        )
        worker_str = "%s| " % (" " * 14)
        for t in [
            tag.min_number_per_worker,
            tag.median_number_per_worker,
            tag.average_number_per_worker,
            tag.max_number_per_worker,
        ]:
            worker_str += reportNumber(n=t, field=7)
        out_str += worker_str + "\n"

    TITLES = {
        "time": "Real Time",
        "clock": "CPU Clock",
        "wait": "CPU Wait",
        "memory": "Memory",
        "disk": "Disk"
    }

    for category in CATEGORIES:
        if category not in options.categories:
            continue

        header += "| %*s " % (
            columnWidths.title(category),
            decorateTitle(category, TITLES[category], options),
        )
        sub_header += decorateSubHeader(category, columnWidths, options)
        tag_str += " | "

        for field in ["min", "med", "ave", "max", "total"]:
            t = getattr(tag, f"{LONG_FORMS[field]}_{category}")
            width = columnWidths.getWidth(category, field)
            if category in TIME_CATEGORIES:
                s = reportTime(
                    t, options, field=width
                )
            elif category in SPACE_CATEGORIES:
                s = reportSpace(
                    t, options, field=width, isBytes=(category in BYTES_CATEGORIES)
                )
            else:
                raise RuntimeError(f"Unknown category {category}")
            tag_str += s

    out_str += header + "\n"
    out_str += sub_header + "\n"
    out_str += tag_str + "\n"
    return out_str


def decorateTitle(category: str, title: str, options: Namespace) -> str:
    """Add a marker to TITLE if the TITLE is sorted on."""
    if category.lower() == options.sortCategory:
        return "%s*" % title
    else:
        return title


def decorateSubHeader(
    category: str, columnWidths: ColumnWidths, options: Namespace
) -> str:
    """Add a marker to the correct field if the TITLE is sorted on."""
    if category != options.sortCategory:
        s = "| %*s%*s%*s%*s%*s " % (
            columnWidths.getWidth(category, "min"),
            "min",
            columnWidths.getWidth(category, "med"),
            "med",
            columnWidths.getWidth(category, "ave"),
            "ave",
            columnWidths.getWidth(category, "max"),
            "max",
            columnWidths.getWidth(category, "total"),
            "total",
        )
        return s
    else:
        s = "| "
        for field, width in [
            ("min", columnWidths.getWidth(category, "min")),
            ("med", columnWidths.getWidth(category, "med")),
            ("ave", columnWidths.getWidth(category, "ave")),
            ("max", columnWidths.getWidth(category, "max")),
            ("total", columnWidths.getWidth(category, "total")),
        ]:
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
    sortField = LONG_FORMS[options.sortField]
    if (
        options.sortCategory in CATEGORIES
    ):
        return sorted(
            jobTypes,
            key=lambda tag: getattr(tag, "%s_%s" % (sortField, options.sortCategory)),
            reverse=options.sortReverse,
        )
    elif options.sortCategory == "alpha":
        return sorted(
            jobTypes,
            key=lambda tag: tag.name,  # type: ignore
            reverse=options.sortReverse,
        )
    elif options.sortCategory == "count":
        return sorted(
            jobTypes,
            key=lambda tag: tag.total_number,  # type: ignore
            reverse=options.sortReverse,
        )

    # due to https://stackoverflow.com/questions/47149154
    assert False


def reportPrettyData(
    root: Expando,
    worker: List[Job],
    job: List[Job],
    job_types: List[Any],
    options: Namespace,
) -> str:
    """Print the important bits out."""
    out_str = "Batch System: %s\n" % root.batch_system
    out_str += "Default Cores: %s  Default Memory: %s\n" "Max Cores: %s\n" % (
        reportNumber(n=get(root, "default_cores")),
        # Although per-job memory usage is in KiB, our default is stored in bytes.
        reportSpace(get(root, "default_memory"), options, isBytes=True),
        reportNumber(n=get(root, "max_cores")),
    )
    out_str += "Total Clock: {}  Total Runtime: {}\n".format(
        reportTime(get(root, "total_clock"), options),
        reportTime(get(root, "total_run_time"), options),
    )
    job_types = sortJobs(job_types, options)
    columnWidths = computeColumnWidths(job_types, worker, job, options)
    out_str += "Worker\n"
    out_str += sprintTag("worker", worker, options, columnWidths=columnWidths)
    out_str += "Job\n"
    out_str += sprintTag("job", job, options, columnWidths=columnWidths)
    for t in job_types:
        out_str += f" {t.name}\n"
        out_str += f"    Total Cores: {t.total_cores}\n"
        out_str += sprintTag(t.name, t, options, columnWidths=columnWidths)
    return out_str


def computeColumnWidths(
    job_types: List[Any], worker: List[Job], job: List[Job], options: Expando
) -> ColumnWidths:
    """Return a ColumnWidths() object with the correct max widths."""
    cw = ColumnWidths()
    for t in job_types:
        updateColumnWidths(t, cw, options)
    updateColumnWidths(worker, cw, options)
    updateColumnWidths(job, cw, options)
    return cw


def updateColumnWidths(tag: Expando, cw: ColumnWidths, options: Expando) -> None:
    """Update the column width attributes for this tag's fields."""
    # TODO: Deduplicate with actual printing code!
    for category in CATEGORIES:
        if category in options.categories:
            for field in ["min", "med", "ave", "max", "total"]:
                t = getattr(tag, f"{LONG_FORMS[field]}_{category}")
                width = cw.getWidth(category, field)
                if category in TIME_CATEGORIES:
                    s = reportTime(
                        t, options, field=width
                    ).strip()
                elif category in SPACE_CATEGORIES:
                    s = reportSpace(
                        t, options, field=width, isBytes=(category in BYTES_CATEGORIES)
                    ).strip()
                else:
                    raise RuntimeError(f"Unknown category {category}")
                if len(s) >= cw.getWidth(category, field):
                    # this string is larger than max, width must be increased
                    cw.setWidth(category, field, len(s) + 1)


def buildElement(element: Expando, items: List[Job], item_name: str) -> Expando:
    """Create an element for output."""

    def assertNonnegative(i: float, name: str) -> float:
        if i < 0:
            raise RuntimeError("Negative value %s reported for %s" % (i, name))
        else:
            return float(i)


    # Make lists of all values for all items in each category, plus requested cores.
    item_values = {category: [] for category in (CATEGORIES + ["cores"])}

    for item in items:
        # If something lacks an entry, assume it used none of that thing.
        # This avoids crashing when jobs e.g. aren't done.
        for category, values in item_values.items():
            if category in COMPUTED_CATEGORIES:
                continue
            category_key = category if category != "cores" else "requested_cores"
            category_value = assertNonnegative(float(item.get(category_key, 0)), category)
            values.append(category_value)

    for index in range(0, len(item_values[CATEGORIES[0]])):
        # For each item, compute the computed categories
        item_values["wait"].append(item_values["time"][index] * item_values["cores"][index] - item_values["clock"][index])

    for category, values in item_values.items():
        values.sort()

    if len(item_values[CATEGORIES[0]]) == 0:
        # Nothing actually there so make a 0 value
        for k, v in item_values.items():
            v.append(0)

    item_element = Expando(
        total_number=float(len(items)),
        name=item_name
    )

    for category, values in item_values.items():
        item_element["total_" + category] = float(sum(values))
        item_element["median_" + category] = float(values[len(values) // 2])
        item_element["average_" + category] = float(sum(values) / len(values))
        item_element["min_" + category] = float(min(values))
        item_element["max_" + category] = float(max(values))

    element[item_name] = item_element

    return item_element


def createSummary(
    element: Expando,
    containingItems: List[Job],
    containingItemName: str,
    getFn: Callable[[Job], List[Optional[Job]]],
) -> None:
    itemCounts = [len(getFn(containingItem)) for containingItem in containingItems]
    itemCounts.sort()
    if len(itemCounts) == 0:
        itemCounts.append(0)
    element["median_number_per_%s" % containingItemName] = itemCounts[
        len(itemCounts) // 2
    ]
    element["average_number_per_%s" % containingItemName] = float(
        sum(itemCounts) / len(itemCounts)
    )
    element["min_number_per_%s" % containingItemName] = min(itemCounts)
    element["max_number_per_%s" % containingItemName] = max(itemCounts)


def getStats(jobStore: AbstractJobStore) -> Expando:
    """Collect and return the stats and config data."""

    def aggregateStats(fileHandle: TextIO, aggregateObject: Expando) -> None:
        try:
            stats = json.load(fileHandle, object_hook=Expando)
            for key in list(stats.keys()):
                if key in aggregateObject:
                    aggregateObject[key].append(stats[key])
                else:
                    aggregateObject[key] = [stats[key]]
        except ValueError:
            logger.critical(
                "File %s contains corrupted json. Skipping file." % fileHandle
            )
            pass  # The file is corrupted.

    aggregateObject = Expando()
    callBack = partial(aggregateStats, aggregateObject=aggregateObject)
    jobStore.read_logs(callBack, read_all=True)
    return aggregateObject


def processData(config: Config, stats: Expando) -> Expando:
    """
    Collate the stats and report
    """
    if "total_time" not in stats or "total_clock" not in stats:
        # toil job not finished yet
        stats.total_time = [0.0]
        stats.total_clock = [0.0]

    stats.total_time = sum(float(number) for number in stats.total_time)
    stats.total_clock = sum(float(number) for number in stats.total_clock)

    collatedStatsTag = Expando(
        total_run_time=stats.total_time,
        total_clock=stats.total_clock,
        batch_system=config.batchSystem,
        default_memory=str(config.defaultMemory),
        default_cores=str(config.defaultCores),
        max_cores=str(config.maxCores),
    )

    # Add worker info
    worker = [_f for _f in getattr(stats, "workers", []) if _f]
    jobs = [_f for _f in getattr(stats, "jobs", []) if _f]
    jobs = [item for sublist in jobs for item in sublist]

    def fn4(job: Job) -> List[Optional[Job]]:
        try:
            return list(jobs)
        except TypeError:
            return []

    buildElement(collatedStatsTag, worker, "worker")
    createSummary(
        buildElement(collatedStatsTag, jobs, "jobs"),
        getattr(stats, "workers", []),
        "worker",
        fn4,
    )
    # Get info for each job
    jobNames = set()
    for job in jobs:
        jobNames.add(job.class_name)
    jobTypesTag = Expando()
    collatedStatsTag.job_types = jobTypesTag
    for jobName in jobNames:
        jobTypes = [job for job in jobs if job.class_name == jobName]
        buildElement(jobTypesTag, jobTypes, jobName)
    collatedStatsTag.name = "collatedStatsTag"
    return collatedStatsTag


def reportData(tree: Expando, options: Namespace) -> None:
    # Now dump it all out to file
    if options.raw:
        out_str = json.dumps(tree, indent=4, separators=(",", ": "))
    else:
        out_str = reportPrettyData(
            tree, tree.worker, tree.jobs, tree.job_types.values(), options
        )
    if options.outputFile is not None:
        with open(options.outputFile, "w") as f:
            f.write(out_str)
    # Now dump onto the screen
    print(out_str)


sort_category_choices = CATEGORIES + ["alpha", "count"]
sort_field_choices = ["min", "med", "ave", "max", "total"]


def add_stats_options(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--outputFile",
        dest="outputFile",
        default=None,
        help="File in which to write results.",
    )
    parser.add_argument(
        "--raw", action="store_true", default=False, help="Return raw json data."
    )
    parser.add_argument(
        "--pretty",
        "--human",
        action="store_true",
        default=False,
        help="if not raw, prettify the numbers to be human readable.",
    )
    parser.add_argument(
        "--sortReverse",
        "--reverseSort",
        default=False,
        action="store_true",
        help="Reverse sort.",
    )
    parser.add_argument(
        "--categories",
        default=",".join(CATEGORIES),
        type=str,
        help=f"Comma separated list of any of the following: {CATEGORIES}.",
    )
    parser.add_argument(
        "--sortCategory",
        default="time",
        choices=sort_category_choices,
        help=f"How to sort job categories.  Choices: {sort_category_choices}. Default: time.",
    )
    parser.add_argument(
        "--sortField",
        default="med",
        choices=sort_field_choices,
        help=f"How to sort job fields.  Choices: {sort_field_choices}. Default: med.",
    )


def main() -> None:
    """Reports stats on the workflow, use with --stats option to toil."""
    parser = parser_with_common_options(prog="toil stats")
    add_stats_options(parser)
    options = parser.parse_args()

    for c in options.categories.split(","):
        if c.strip() not in CATEGORIES:
            raise ValueError(f"{c} not in {category_choices}!")
    options.categories = [x.strip().lower() for x in options.categories.split(",")]

    set_logging_from_options(options)
    config = Config()
    config.setOptions(options)
    jobStore = Toil.resumeJobStore(config.jobStore)
    stats = getStats(jobStore)
    collatedStatsTag = processData(jobStore.config, stats)
    reportData(collatedStatsTag, options)
