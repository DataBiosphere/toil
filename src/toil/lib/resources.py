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
import resource


def get_total_cpu_time_and_memory_usage():
    """
    Gives the total cpu time of itself and all its children, and the maximum RSS memory usage of
    itself and its single largest child.
    """
    me = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    total_cpu_time = me.ru_utime + me.ru_stime + children.ru_utime + children.ru_stime
    total_memory_usage = me.ru_maxrss + children.ru_maxrss
    return total_cpu_time, total_memory_usage


def get_total_cpu_time():
    """Gives the total cpu time, including the children."""
    me = resource.getrusage(resource.RUSAGE_SELF)
    childs = resource.getrusage(resource.RUSAGE_CHILDREN)
    return me.ru_utime + me.ru_stime + childs.ru_utime + childs.ru_stime
