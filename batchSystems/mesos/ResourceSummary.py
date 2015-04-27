__author__ = 'CJ'
from collections import namedtuple

# TODO: rename to ResourceRequirement

class ResourceSummary(namedtuple("ResourceSummary", ["memory", "cpu"])):
    pass
# used to describe resource requirements. Used as key in queue dictionary