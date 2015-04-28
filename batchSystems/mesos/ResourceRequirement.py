__author__ = 'CJ'
from collections import namedtuple


class ResourceRequirement(namedtuple("ResourceSummary", ["memory", "cpu"])):
    pass
# used to describe resource requirements. Used as key in queue dictionary