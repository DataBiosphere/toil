"""Implemented support for Common Workflow Language (CWL) for Toil."""
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

import logging
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    MutableMapping,
    MutableSequence,
    Tuple,
    TypeVar,
)

logger = logging.getLogger(__name__)

# Customized CWL utilities

def visit_top_cwl_class(
    rec: MutableMapping,
    classes: Iterable[str],
    op: Callable[[MutableMapping], Any]
) -> None:
    """
    Apply the given operation to all top-level CWL objects with the given named CWL class.
    Like cwltool's visit_class but doesn't look inside any object visited.
    """
    if isinstance(rec, MutableMapping):
        if rec.get("class", None) in classes:
            # This is one of the classes requested
            # So process it
            op(rec)
        else:
            # Look inside it instead
            for key in rec:
                visit_top_cwl_class(rec[key], classes, op)
    elif isinstance(rec, MutableSequence):
        # This item is actually a list of things, so look at all of them.
        for key in rec:
            visit_top_cwl_class(key, classes, op)

DownReturnType = TypeVar('DownReturnType')
def visit_cwl_class_and_reduce(
    rec: MutableMapping,
    classes: Iterable[str],
    op_down: Callable[[MutableMapping], DownReturnType],
    op_up: Callable[[MutableMapping, DownReturnType, MutableSequence], Any]
) -> List:
    """
    Apply the given operations to all CWL objects with the given named CWL class.
    Applies the down operation top-down, and the up operation bottom-up, and
    passes the down operation's result and a list of the up operation results
    for all child keys (flattening across lists and collapsing nodes of
    non-matching classes) to the up operation.
    """

    results = []

    if isinstance(rec, MutableMapping):
        down_result = None
        child_results = []
        if rec.get("class", None) in classes:
            # Apply the down operation
            down_result = op_down(rec)
        for key in rec:
            # Look inside and collect child results
            for result in visit_cwl_class_and_reduce(rec[key], classes, op_down, op_up):
                child_results.append(result)
        if rec.get("class", None) in classes:
            # Apply the up operation
            results.append(op_up(rec, down_result, child_results))
        else:
            # We aren't processing here so pass up all the child results
            results += child_results
    elif isinstance(rec, MutableSequence):
        # This item is actually a list of things, so look at all of them.
        for key in rec:
            for result in visit_cwl_class_and_reduce(key, classes, op_down, op_up):
                # And flatten together all their results.
                results.append(result)
    return results
