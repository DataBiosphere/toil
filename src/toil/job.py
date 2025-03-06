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
from __future__ import annotations

import collections
import copy
import importlib
import inspect
import itertools
import logging
import math
import os
import pickle
import sys
import time
import uuid
from abc import ABCMeta, abstractmethod
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from contextlib import contextmanager
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
    TypedDict,
    Literal,
)
from urllib.error import HTTPError
from urllib.parse import urlsplit, unquote, urljoin

from toil import memoize

import dill
from configargparse import ArgParser

from toil.lib.io import is_remote_url

if sys.version_info < (3, 11):
    from typing_extensions import NotRequired
else:
    from typing import NotRequired

from toil.bus import Names
from toil.common import Config, Toil, addOptions, safeUnpickleFromStream
from toil.deferred import DeferredFunction
from toil.fileStores import FileID
from toil.lib.compatibility import deprecated
from toil.lib.conversions import bytes2human, human2bytes
from toil.lib.expando import Expando
from toil.lib.resources import ResourceMonitor
from toil.resource import ModuleDescriptor
from toil.statsAndLogging import set_logging_from_options

from toil.lib.exceptions import UnimplementedURLException

if TYPE_CHECKING:
    from optparse import OptionParser

    from toil.batchSystems.abstractBatchSystem import (
        BatchJobExitReason
    )
    from toil.fileStores.abstractFileStore import AbstractFileStore
    from toil.jobStores.abstractJobStore import AbstractJobStore

logger = logging.getLogger(__name__)


class JobPromiseConstraintError(RuntimeError):
    """
    Error for job being asked to promise its return value, but it not available.

    (Due to the return value not yet been hit in the topological order of the job graph.)
    """

    def __init__(
        self, promisingJob: "Job", recipientJob: Optional["Job"] = None
    ) -> None:
        """
        Initialize this error.

        :param promisingJob: The job being asked for its return value.
        :param recipientJob: The job receiving the return value, if any.
        """
        self.promisingJob = promisingJob
        self.recipientJob = recipientJob

        if recipientJob is None:
            # Come up with a vaguer error message
            super().__init__(
                f"Job {promisingJob.description} cannot promise its "
                "return value to a job that is not its successor"
            )
        else:
            # Write a full error message
            super().__init__(
                f"Job {promisingJob.description} cannot promise its "
                f"return value to non-successor {recipientJob.description}"
            )


class ConflictingPredecessorError(Exception):
    def __init__(self, predecessor: "Job", successor: "Job") -> None:
        super().__init__(
            f'The given job: "{predecessor.description}" is already a predecessor of job: "{successor.description}".'
        )


class DebugStoppingPointReached(BaseException):
    """
    Raised when a job reaches a point at which it has been instructed to stop for debugging.
    """


class FilesDownloadedStoppingPointReached(DebugStoppingPointReached):
    """
    Raised when a job stops because it was asked to download its files, and the files are downloaded.
    """

    def __init__(
        self, message, host_and_job_paths: Optional[list[tuple[str, str]]] = None
    ):
        super().__init__(message)

        # Save the host and user-code-visible paths of files, in case we're
        # using a container and they are different.
        self.host_and_job_paths = host_and_job_paths


class TemporaryID:
    """
    Placeholder for a unregistered job ID used by a JobDescription.

    Needs to be held:
        * By JobDescription objects to record normal relationships.
        * By Jobs to key their connected-component registries and to record
          predecessor relationships to facilitate EncapsulatedJob adding
          itself as a child.
        * By Services to tie back to their hosting jobs, so the service
          tree can be built up from Service objects.
    """

    def __init__(self) -> None:
        """Assign a unique temporary ID that won't collide with anything."""
        self._value = uuid.uuid4()

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return f"TemporaryID({self._value})"

    def __hash__(self) -> int:
        return hash(self._value)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, TemporaryID) and self._value == other._value

    def __ne__(self, other: Any) -> bool:
        return not isinstance(other, TemporaryID) or self._value != other._value


class AcceleratorRequirement(TypedDict):
    """Requirement for one or more computational accelerators, like a GPU or FPGA."""

    count: int
    """
    How many of the accelerator are needed to run the job.
    """
    kind: str
    """
    What kind of accelerator is required. Can be "gpu". Other kinds defined in
    the future might be "fpga", etc.
    """
    model: NotRequired[str]
    """
    What model of accelerator is needed. The exact set of values available
    depends on what the backing scheduler calls its accelerators; strings like
    "nvidia-tesla-k80" might be expected to work. If a specific model of
    accelerator is not required, this should be absent.
    """
    brand: NotRequired[str]
    """
    What brand or manufacturer of accelerator is required. The exact set of
    values available depends on what the backing scheduler calls the brands of
    its accleerators; strings like "nvidia" or "amd" might be expected to work.
    If a specific brand of accelerator is not required (for example, because
    the job can use multiple brands of accelerator that support a given API)
    this should be absent.
    """
    api: NotRequired[str]
    """
    What API is to be used to communicate with the accelerator. This can be
    "cuda". Other APIs supported in the future might be "rocm", "opencl",
    "metal", etc. If the job does not need a particular API to talk to the
    accelerator, this should be absent.
    """

    # TODO: support requesting any GPU with X amount of vram


def parse_accelerator(
    spec: Union[int, str, dict[str, Union[str, int]]]
) -> AcceleratorRequirement:
    """
    Parse an AcceleratorRequirement specified by user code.

    Supports formats like:

    >>> parse_accelerator(8)
    {'count': 8, 'kind': 'gpu'}

    >>> parse_accelerator("1")
    {'count': 1, 'kind': 'gpu'}

    >>> parse_accelerator("nvidia-tesla-k80")
    {'count': 1, 'kind': 'gpu', 'brand': 'nvidia', 'model': 'nvidia-tesla-k80'}

    >>> parse_accelerator("nvidia-tesla-k80:2")
    {'count': 2, 'kind': 'gpu', 'brand': 'nvidia', 'model': 'nvidia-tesla-k80'}

    >>> parse_accelerator("gpu")
    {'count': 1, 'kind': 'gpu'}

    >>> parse_accelerator("cuda:1")
    {'count': 1, 'kind': 'gpu', 'brand': 'nvidia', 'api': 'cuda'}

    >>> parse_accelerator({"kind": "gpu"})
    {'count': 1, 'kind': 'gpu'}

    >>> parse_accelerator({"brand": "nvidia", "count": 5})
    {'count': 5, 'kind': 'gpu', 'brand': 'nvidia'}

    Assumes that if not specified, we are talking about GPUs, and about one
    of them. Knows that "gpu" is a kind, and "cuda" is an API, and "nvidia"
    is a brand.

    :raises ValueError: if it gets something it can't parse
    :raises TypeError: if it gets something it can't parse because it's the wrong type.
    """
    KINDS = {"gpu"}
    BRANDS = {"nvidia", "amd"}
    APIS = {"cuda", "rocm", "opencl"}

    parsed: AcceleratorRequirement = {"count": 1, "kind": "gpu"}

    if isinstance(spec, int):
        parsed["count"] = spec
    elif isinstance(spec, str):
        parts = spec.split(":")

        if len(parts) > 2:
            raise ValueError("Could not parse AcceleratorRequirement: " + spec)

        possible_count = parts[-1]

        try:
            # If they have : and then a count, or just a count, handle that.
            parsed["count"] = int(possible_count)
            if len(parts) > 1:
                # Then we take whatever was before the colon as text
                possible_description = parts[0]
            else:
                possible_description = None
        except ValueError:
            # It doesn't end with a number
            if len(parts) == 2:
                # We should have a number though.
                raise ValueError(
                    "Could not parse AcceleratorRequirement count in: " + spec
                )
            else:
                # Must be just the description
                possible_description = possible_count

        # Determine if we have a kind, brand, API, or (by default) model
        if possible_description in KINDS:
            parsed["kind"] = possible_description
        elif possible_description in BRANDS:
            parsed["brand"] = possible_description
        elif possible_description in APIS:
            parsed["api"] = possible_description
        else:
            if possible_description is not None:
                parsed["model"] = possible_description
    elif isinstance(spec, dict):
        # It's a dict, so merge with the defaults.
        parsed.update(spec)
        # TODO: make sure they didn't misspell keys or something
    else:
        raise TypeError(
            f"Cannot parse value of type {type(spec)} as an AcceleratorRequirement"
        )

    if parsed["kind"] == "gpu":
        # Use some smarts about what current GPUs are like to elaborate the
        # description.

        if "brand" not in parsed and "model" in parsed:
            # Try to guess the brand from the model
            for brand in BRANDS:
                if parsed["model"].startswith(brand):
                    # The model often starts with the brand
                    parsed["brand"] = brand
                    break

        if "brand" not in parsed and "api" in parsed:
            # Try to guess the brand from the API
            if parsed["api"] == "cuda":
                # Only nvidia makes cuda cards
                parsed["brand"] = "nvidia"
            elif parsed["api"] == "rocm":
                # Only amd makes rocm cards
                parsed["brand"] = "amd"

    return parsed


def accelerator_satisfies(
    candidate: AcceleratorRequirement,
    requirement: AcceleratorRequirement,
    ignore: list[str] = [],
) -> bool:
    """
    Test if candidate partially satisfies the given requirement.

    :returns: True if the given candidate at least partially satisfies the
              given requirement (i.e. check all fields other than count).
    """
    for key in ["kind", "brand", "api", "model"]:
        if key in ignore:
            # Skip this aspect.
            continue
        if key in requirement:
            if key not in candidate:
                logger.debug(
                    "Candidate %s does not satisfy requirement %s because it does not have a %s",
                    candidate,
                    requirement,
                    key,
                )
                return False
            if candidate[key] != requirement[key]:
                logger.debug(
                    "Candidate %s does not satisfy requirement %s because it does not have the correct %s",
                    candidate,
                    requirement,
                    key,
                )
                return False
    # If all these match or are more specific than required, we match!
    return True


def accelerators_fully_satisfy(
    candidates: Optional[list[AcceleratorRequirement]],
    requirement: AcceleratorRequirement,
    ignore: list[str] = [],
) -> bool:
    """
    Determine if a set of accelerators satisfy a requirement.

    Ignores fields specified in ignore.

    :returns:  True if the requirement AcceleratorRequirement is
               fully satisfied by the ones in the list, taken
               together (i.e. check all fields including count).
    """

    count_remaining = requirement["count"]

    if candidates:
        for candidate in candidates:
            if accelerator_satisfies(candidate, requirement, ignore=ignore):
                if candidate["count"] > count_remaining:
                    # We found all the matching accelerators we need
                    count_remaining = 0
                    break
                else:
                    count_remaining -= candidate["count"]

    # If we have no count left we are fully satisfied
    return count_remaining == 0


class RequirementsDict(TypedDict):
    """
    Typed storage for requirements for a job.

    Where requirement values are of different types depending on the requirement.
    """

    cores: NotRequired[Union[int, float]]
    memory: NotRequired[int]
    disk: NotRequired[int]
    accelerators: NotRequired[list[AcceleratorRequirement]]
    preemptible: NotRequired[bool]


# These must be all the key names in RequirementsDict
REQUIREMENT_NAMES = ["disk", "memory", "cores", "accelerators", "preemptible"]

# This is the supertype of all value types in RequirementsDict
ParsedRequirement = Union[int, float, bool, list[AcceleratorRequirement]]

# We define some types for things we can parse into different kind of requirements
ParseableIndivisibleResource = Union[str, int]
ParseableDivisibleResource = Union[str, int, float]
ParseableFlag = Union[str, int, bool]
ParseableAcceleratorRequirement = Union[
    str,
    int,
    Mapping[str, Any],
    AcceleratorRequirement,
    Sequence[Union[str, int, Mapping[str, Any], AcceleratorRequirement]],
]

ParseableRequirement = Union[
    ParseableIndivisibleResource,
    ParseableDivisibleResource,
    ParseableFlag,
    ParseableAcceleratorRequirement,
]


class Requirer:
    """
    Base class implementing the storage and presentation of requirements.

    Has cores, memory, disk, and preemptability as properties.
    """

    _requirementOverrides: RequirementsDict

    def __init__(self, requirements: Mapping[str, ParseableRequirement]) -> None:
        """
        Parse and save the given requirements.

        :param dict requirements: Dict from string to value
            describing a set of resource requirments. 'cores', 'memory',
            'disk', 'preemptible', and 'accelerators' fields, if set, are
            parsed and broken out into properties. If unset, the relevant
            property will be unspecified, and will be pulled from the assigned
            Config object if queried (see
            :meth:`toil.job.Requirer.assignConfig`). If unspecified and no
            Config object is assigned, an AttributeError will be raised at
            query time.
        """
        super().__init__()

        # We can have a toil.common.Config assigned to fill in default values
        # for e.g. job requirements not explicitly specified.
        self._config: Optional[Config] = None

        # Save requirements, parsing and validating anything that needs parsing
        # or validating. Don't save Nones.
        self._requirementOverrides = {
            k: Requirer._parseResource(k, v)
            for (k, v) in requirements.items()
            if v is not None
        }

    def assignConfig(self, config: Config) -> None:
        """
        Assign the given config object to be used to provide default values.

        Must be called exactly once on a loaded JobDescription before any
        requirements are queried.

        :param config: Config object to query
        """
        if self._config is not None:
            raise RuntimeError(f"Config assigned multiple times to {self}")
        self._config = config

    def __getstate__(self) -> dict[str, Any]:
        """Return the dict to use as the instance's __dict__ when pickling."""
        # We want to exclude the config from pickling.
        state = self.__dict__.copy()
        state["_config"] = None
        return state

    def __copy__(self) -> "Requirer":
        """Return a semantically-shallow copy of the object, for :meth:`copy.copy`."""
        # The hide-the-method-and-call-the-copy-module approach from
        # <https://stackoverflow.com/a/40484215> doesn't seem to work for
        # __copy__. So we try the method of
        # <https://stackoverflow.com/a/51043609>. But we go through the
        # pickling state hook.

        clone = type(self).__new__(self.__class__)
        clone.__dict__.update(self.__getstate__())

        if self._config is not None:
            # Share a config reference
            clone.assignConfig(self._config)

        return clone

    def __deepcopy__(self, memo: Any) -> "Requirer":
        """Return a semantically-deep copy of the object, for :meth:`copy.deepcopy`."""
        # We used to use <https://stackoverflow.com/a/40484215> and
        # <https://stackoverflow.com/a/71125311> but that would result in
        # copies sometimes resurrecting weirdly old job versions. So now we
        # just actually implement __deepcopy__.

        clone = type(self).__new__(self.__class__)
        state = self.__getstate__()
        clone_state = copy.deepcopy(state, memo)
        clone.__dict__.update(clone_state)

        if self._config is not None:
            # Share a config reference
            clone.assignConfig(self._config)

        return clone

    @overload
    @staticmethod
    def _parseResource(
        name: Union[Literal["memory"], Literal["disks"]],
        value: ParseableIndivisibleResource,
    ) -> int: ...

    @overload
    @staticmethod
    def _parseResource(
        name: Literal["cores"], value: ParseableDivisibleResource
    ) -> Union[int, float]: ...

    @overload
    @staticmethod
    def _parseResource(
        name: Literal["accelerators"], value: ParseableAcceleratorRequirement
    ) -> list[AcceleratorRequirement]: ...

    @overload
    @staticmethod
    def _parseResource(name: str, value: ParseableRequirement) -> ParsedRequirement: ...

    @overload
    @staticmethod
    def _parseResource(name: str, value: None) -> None: ...

    @staticmethod
    def _parseResource(
        name: str, value: Optional[ParseableRequirement]
    ) -> Optional[ParsedRequirement]:
        """
        Parse a Toil resource requirement value and apply resource-specific type checks.

        If the value is a string, a binary or metric unit prefix in it will be
        evaluated and the corresponding integral value will be returned.

        :param name: The name of the resource
        :param value: The resource value

        >>> Requirer._parseResource('cores', None)
        >>> Requirer._parseResource('cores', 1), Requirer._parseResource('disk', 1), \
        Requirer._parseResource('memory', 1)
        (1, 1, 1)
        >>> Requirer._parseResource('cores', '1G'), Requirer._parseResource('disk', '1G'), \
        Requirer._parseResource('memory', '1G')
        (1073741824, 1073741824, 1073741824)
        >>> Requirer._parseResource('cores', 1.1)
        1.1
        >>> Requirer._parseResource('disk', 1.1) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        TypeError: The 'disk' requirement does not accept values that are of <type 'float'>
        >>> Requirer._parseResource('memory', object()) # doctest: +IGNORE_EXCEPTION_DETAIL
        Traceback (most recent call last):
        ...
        TypeError: The 'memory' requirement does not accept values that are of ...
        """
        if value is None:
            # Anything can be None.
            return value

        if name in ("memory", "disk", "cores"):
            # These should be numbers that accept things like "5G".
            if isinstance(value, (str, bytes)):
                value = human2bytes(value)
            if isinstance(value, int):
                return value
            elif isinstance(value, float) and name == "cores":
                # But only cores can be fractional.
                return value
            else:
                raise TypeError(
                    f"The '{name}' requirement does not accept values that are of type {type(value)}"
                )
        elif name == "preemptible":
            if isinstance(value, str):
                if value.lower() == "true":
                    return True
                elif value.lower() == "false":
                    return False
                else:
                    raise ValueError(
                        f"The '{name}' requirement, as a string, must be 'true' or 'false' but is {value}"
                    )
            elif isinstance(value, int):
                if value == 1:
                    return True
                if value == 0:
                    return False
                else:
                    raise ValueError(
                        f"The '{name}' requirement, as an int, must be 1 or 0 but is {value}"
                    )
            elif isinstance(value, bool):
                return value
            else:
                raise TypeError(
                    f"The '{name}' requirement does not accept values that are of type {type(value)}"
                )
        elif name == "accelerators":
            # The type checking for this is delegated to the
            # AcceleratorRequirement class.
            if isinstance(value, list):
                return [
                    parse_accelerator(v) for v in value
                ]  # accelerators={'kind': 'gpu', 'brand': 'nvidia', 'count': 2}
            else:
                return [parse_accelerator(value)]  # accelerators=1
        else:
            # Anything else we just pass along without opinons
            return cast(ParsedRequirement, value)

    def _fetchRequirement(self, requirement: str) -> Optional[ParsedRequirement]:
        """
        Get the value of the specified requirement ('blah').

        Done by looking it up in our requirement storage and querying 'defaultBlah'
        on the config if it isn't set. If the config would be queried but isn't
        associated, raises AttributeError.

        :param requirement: The name of the resource
        """
        if requirement in self._requirementOverrides:
            value = self._requirementOverrides[requirement]
            if value is None:
                raise AttributeError(
                    f"Encountered explicit None for '{requirement}' requirement of {self}"
                )
            return value
        elif self._config is not None:
            values = [
                getattr(self._config, "default_" + requirement, None),
                getattr(self._config, "default" + requirement.capitalize(), None),
            ]
            value = values[0] if values[0] is not None else values[1]
            if value is None:
                raise AttributeError(
                    f"Encountered None for default '{requirement}' requirement "
                    f"in config: {self._config}"
                )
            return value
        else:
            raise AttributeError(
                f"Default value for '{requirement}' requirement of {self} cannot be determined"
            )

    @property
    def requirements(self) -> RequirementsDict:
        """Get dict containing all non-None, non-defaulted requirements."""
        return dict(self._requirementOverrides)

    @property
    def disk(self) -> int:
        """Get the maximum number of bytes of disk required."""
        return cast(int, self._fetchRequirement("disk"))

    @disk.setter
    def disk(self, val: ParseableIndivisibleResource) -> None:
        self._requirementOverrides["disk"] = Requirer._parseResource("disk", val)

    @property
    def memory(self) -> int:
        """Get the maximum number of bytes of memory required."""
        return cast(int, self._fetchRequirement("memory"))

    @memory.setter
    def memory(self, val: ParseableIndivisibleResource) -> None:
        self._requirementOverrides["memory"] = Requirer._parseResource("memory", val)

    @property
    def cores(self) -> Union[int, float]:
        """Get the number of CPU cores required."""
        return cast(Union[int, float], self._fetchRequirement("cores"))

    @cores.setter
    def cores(self, val: ParseableDivisibleResource) -> None:
        self._requirementOverrides["cores"] = Requirer._parseResource("cores", val)

    @property
    def preemptible(self) -> bool:
        """Whether a preemptible node is permitted, or a nonpreemptible one is required."""
        return cast(bool, self._fetchRequirement("preemptible"))

    @preemptible.setter
    def preemptible(self, val: ParseableFlag) -> None:
        self._requirementOverrides["preemptible"] = Requirer._parseResource(
            "preemptible", val
        )

    @deprecated(new_function_name="preemptible")
    def preemptable(self, val: ParseableFlag) -> None:
        self._requirementOverrides["preemptible"] = Requirer._parseResource(
            "preemptible", val
        )

    @property
    def accelerators(self) -> list[AcceleratorRequirement]:
        """Any accelerators, such as GPUs, that are needed."""
        return cast(
            list[AcceleratorRequirement], self._fetchRequirement("accelerators")
        )

    @accelerators.setter
    def accelerators(self, val: ParseableAcceleratorRequirement) -> None:
        self._requirementOverrides["accelerators"] = Requirer._parseResource(
            "accelerators", val
        )

    def scale(self, requirement: str, factor: float) -> "Requirer":
        """
        Return a copy of this object with the given requirement scaled up or down.

        Only works on requirements where that makes sense.
        """
        # Make a shallow copy
        scaled = copy.copy(self)
        # But make sure it has its own override dictionary
        scaled._requirementOverrides = dict(scaled._requirementOverrides)

        original_value = getattr(scaled, requirement)
        if isinstance(original_value, (int, float)):
            # This is something we actually can scale up and down
            new_value = original_value * factor
            if requirement in ("memory", "disk"):
                # Must round to an int
                new_value = math.ceil(new_value)
            setattr(scaled, requirement, new_value)
            return scaled
        else:
            # We can't scale some requirements.
            raise ValueError(f"Cannot scale {requirement} requirements!")

    def requirements_string(self) -> str:
        """Get a nice human-readable string of our requirements."""
        parts = []
        for k in REQUIREMENT_NAMES:
            v = self._fetchRequirement(k)
            if v is not None:
                if isinstance(v, (int, float)) and v > 1000:
                    # Make large numbers readable
                    v = bytes2human(v)
                parts.append(f"{k}: {v}")
        if len(parts) == 0:
            parts = ["no requirements"]
        return ", ".join(parts)


class JobBodyReference(NamedTuple):
    """
    Reference from a job description to its body.
    """

    file_store_id: str
    """File ID (or special shared file name for the root job) of the job's body."""
    module_string: str
    """Stringified description of the module needed to load the body."""


class JobDescription(Requirer):
    """
    Stores all the information that the Toil Leader ever needs to know about a Job.

    This includes:
        * Resource requirements.
        * Which jobs are children or follow-ons or predecessors of this job.
        * A reference to the Job object in the job store.

    Can be obtained from an actual (i.e. executable) Job object, and can be
    used to obtain the Job object from the JobStore.

    Never contains other Jobs or JobDescriptions: all reference is by ID.

    Subclassed into variants for checkpoint jobs and service jobs that have
    their specific parameters.
    """

    def __init__(
        self,
        requirements: Mapping[str, Union[int, str, bool]],
        jobName: str,
        unitName: Optional[str] = "",
        displayName: Optional[str] = "",
        local: Optional[bool] = None,
        files: Optional[set[FileID]] = None,
    ) -> None:
        """
        Create a new JobDescription.

        :param requirements: Dict from string to number, string, or bool
            describing the resource requirements of the job. 'cores', 'memory',
            'disk', and 'preemptible' fields, if set, are parsed and broken out
            into properties. If unset, the relevant property will be
            unspecified, and will be pulled from the assigned Config object if
            queried (see :meth:`toil.job.Requirer.assignConfig`).
        :param jobName: Name of the kind of job this is. May be used in job
            store IDs and logging. Also used to let the cluster scaler learn a
            model for how long the job will take. Ought to be the job class's
            name if no real user-defined name is available.
        :param unitName: Name of this instance of this kind of job. May
            appear with jobName in logging.
        :param displayName: A human-readable name to identify this
            particular job instance. Ought to be the job class's name
            if no real user-defined name is available.
        :param local: If True, the job is meant to use minimal resources but is
            sensitive to execution latency, and so should be executed by the
            leader.
        :param files: Set of FileID objects that the job plans to use.
        """
        # Set requirements
        super().__init__(requirements)

        # Set local-ness flag, which is not (yet?) a requirement
        self.local: bool = local or False

        # Save names, making sure they are strings and not e.g. bytes or None.
        def makeString(x: Union[str, bytes, None]) -> str:
            if isinstance(x, bytes):
                return x.decode("utf-8", errors="replace")
            if x is None:
                return ""
            return x

        self.jobName = makeString(jobName)
        self.unitName = makeString(unitName)
        self.displayName = makeString(displayName)

        # Set properties that are not fully filled in on creation.

        # ID of this job description in the JobStore.
        self.jobStoreID: Union[str, TemporaryID] = TemporaryID()

        # Information that encodes how to find the Job body data that this
        # JobDescription describes, and the module(s) needed to unpickle it.
        # None if no body needs to run.
        self._body: Optional[JobBodyReference] = None

        # Set scheduling properties that the leader read to think about scheduling.

        # The number of times the job should be attempted. Includes the initial
        # try, plus the number of times to retry if the job fails. This number
        # is reduced each time the job is run, until it is zero, and then no
        # further attempts to run the job are made. If None, taken as the
        # default value for this workflow execution.
        self._remainingTryCount = None

        # Holds FileStore FileIDs of the files that should be seen as deleted,
        # as part of a transaction with the writing of this version of the job
        # to the job store. Used to journal deletions of files and recover from
        # a worker crash between committing a JobDescription update (for
        # example, severing the body of a completed job from the
        # JobDescription) and actually executing the requested deletions (i.e.
        # the deletions made by executing the body).
        #
        # Since the files being deleted might be required to execute the job
        # body, we can't delete them first, but we also don't want to leave
        # them behind if we die right after saving the JobDescription.
        #
        # This will be empty at all times except when a new version of a job is
        # in the process of being committed.
        self.filesToDelete = []

        # Holds job names and IDs of the jobs that have been chained into this
        # job, and which should be deleted when this job finally is deleted
        # (but not before). The successor relationships with them will have
        # been cut, so we need to hold onto them somehow. Includes each
        # chained-in job with its original ID, and also this job's ID with its
        # original names, or is empty if no chaining has happened.
        # The first job in the chain comes first in the list.
        self._merged_job_names: list[Names] = []

        # The number of direct predecessors of the job. Needs to be stored at
        # the JobDescription to support dynamically-created jobs with multiple
        # predecessors. Otherwise, we could reach a job by one path down from
        # the root and decide to schedule it without knowing that it is also
        # reachable from other paths down from the root.
        self.predecessorNumber = 0

        # The IDs of predecessor jobs that have finished. Managed by the Leader
        # and ToilState, and just stored in the JobDescription. Never changed
        # after the job is scheduled, so we don't have to worry about
        # conflicting updates from workers.
        # TODO: Move into ToilState itself so leader stops mutating us so much?
        self.predecessorsFinished = set()

        # Note that we don't hold IDs of our predecessors. Predecessors know
        # about us, and not the other way around. Otherwise we wouldn't be able
        # to save ourselves to the job store until our predecessors were saved,
        # but they'd also be waiting on us.

        # The IDs of all child jobs of the described job.
        # Children which are done must be removed with filterSuccessors.
        self.childIDs: set[str] = set()

        # The IDs of all follow-on jobs of the described job.
        # Follow-ons which are done must be removed with filterSuccessors.
        self.followOnIDs: set[str] = set()

        # We keep our own children and follow-ons in a list of successor
        # phases, along with any successors adopted from jobs we have chained
        # from. When we finish our own children and follow-ons, we may have to
        # go back and finish successors for those jobs.
        self.successor_phases: list[set[str]] = [self.followOnIDs, self.childIDs]

        # Dict from ServiceHostJob ID to list of child ServiceHostJobs that start after it.
        # All services must have an entry, if only to an empty list.
        self.serviceTree = {}

        # A jobStoreFileID of the log file for a job. This will be None unless
        # the job failed and the logging has been captured to be reported on the leader.
        self.logJobStoreFileID = None

        # Every time we update a job description in place in the job store, we
        # increment this.
        self._job_version = 0
        # And we log who made the version (by PID)
        self._job_version_writer = 0

        # Store FileIDs that the Job will want to use
        # This currently does not serve much of a purpose except for debugging
        # In the future, this can be used to improve job scheduling, see https://github.com/DataBiosphere/toil/issues/3071
        self.files_to_use = files or set()

    def get_names(self) -> Names:
        """
        Get the names and ID of this job as a named tuple.
        """
        return Names(
            self.jobName,
            self.unitName,
            self.displayName,
            self.displayName,
            str(self.jobStoreID),
        )

    def get_chain(self) -> list[Names]:
        """
        Get all the jobs that executed in this job's chain, in order.

        For each job, produces a named tuple with its various names and its
        original job store ID. The jobs in the chain are in execution order.

        If the job hasn't run yet or it didn't chain, produces a one-item list.
        """
        if len(self._merged_job_names) == 0:
            # We haven't merged so we're just ourselves.
            return [self.get_names()]
        else:
            return list(self._merged_job_names)

    def serviceHostIDsInBatches(self) -> Iterator[list[str]]:
        """
        Find all batches of service host job IDs that can be started at the same time.

        (in the order they need to start in)
        """
        # First start all the jobs with no parent
        roots = set(self.serviceTree.keys())
        for _parent, children in self.serviceTree.items():
            for child in children:
                roots.remove(child)
        batch = list(roots)
        if len(batch) > 0:
            # If there's a first batch, yield it
            yield batch

        while len(batch) > 0:
            nextBatch = []
            for started in batch:
                # Go find all the children that can start now that we have started.
                for child in self.serviceTree[started]:
                    nextBatch.append(child)

            batch = nextBatch
            if len(batch) > 0:
                # Emit the batch if nonempty
                yield batch

    def successorsAndServiceHosts(self) -> Iterator[str]:
        """Get an iterator over all child, follow-on, and service job IDs."""

        return itertools.chain(self.allSuccessors(), self.serviceTree.keys())

    def allSuccessors(self) -> Iterator[str]:
        """
        Get an iterator over all child, follow-on, and chained, inherited successor job IDs.

        Follow-ons will come before children.
        """

        for phase in self.successor_phases:
            yield from phase

    def successors_by_phase(self) -> Iterator[tuple[int, str]]:
        """
        Get an iterator over all child/follow-on/chained inherited successor job IDs, along with their phase number on the stack.

        Phases execute higher numbers to lower numbers.
        """

        for i, phase in enumerate(self.successor_phases):
            for successor in phase:
                yield i, successor

    @property
    def services(self):
        """
        Get a collection of the IDs of service host jobs for this job, in arbitrary order.

        Will be empty if the job has no unfinished services.
        """
        return list(self.serviceTree.keys())

    def has_body(self) -> bool:
        """
        Returns True if we have a job body associated, and False otherwise.
        """
        return self._body is not None

    def attach_body(self, file_store_id: str, user_script: ModuleDescriptor) -> None:
        """
        Attach a job body to this JobDescription.

        Takes the file store ID that the body is stored at, and the required
        user script module.

        The file store ID can also be "firstJob" for the root job, stored as a
        shared file instead.
        """

        self._body = JobBodyReference(file_store_id, user_script.toCommand())

    def detach_body(self) -> None:
        """
        Drop the body reference from a JobDescription.
        """
        self._body = None

    def get_body(self) -> tuple[str, ModuleDescriptor]:
        """
        Get the information needed to load the job body.

        :returns: a file store ID (or magic shared file name "firstJob") and a
            user script module.

        Fails if no body is attached; check has_body() first.
        """

        if not self.has_body():
            raise RuntimeError(f"Cannot load the body of a job {self} without one")

        return self._body.file_store_id, ModuleDescriptor.fromCommand(
            self._body.module_string
        )

    def nextSuccessors(self) -> Optional[set[str]]:
        """
        Return the collection of job IDs for the successors of this job that are ready to run.

        If those jobs have multiple predecessor relationships, they may still
        be blocked on other jobs.

        Returns None when at the final phase (all successors done), and an
        empty collection if there are more phases but they can't be entered yet
        (e.g. because we are waiting for the job itself to run).
        """
        if self.has_body():
            # We ourselves need to run. So there's not nothing to do
            # but no successors are ready.
            return set()
        else:
            for phase in reversed(self.successor_phases):
                if len(phase) > 0:
                    # Rightmost phase that isn't empty
                    return phase
        # If no phase isn't empty, we're done.
        return None

    def filterSuccessors(self, predicate: Callable[[str], bool]) -> None:
        """
        Keep only successor jobs for which the given predicate function approves.

        The predicate function is called with the job's ID.

        Treats all other successors as complete and forgets them.
        """

        for phase in self.successor_phases:
            for successor_id in list(phase):
                if not predicate(successor_id):
                    phase.remove(successor_id)
        self.successor_phases = [p for p in self.successor_phases if len(p) > 0]

    def filterServiceHosts(self, predicate: Callable[[str], bool]) -> None:
        """
        Keep only services for which the given predicate approves.

        The predicate function is called with the service host job's ID.

        Treats all other services as complete and forgets them.
        """
        # Get all the services we shouldn't have anymore
        toRemove = set()
        for serviceID in self.services:
            if not predicate(serviceID):
                toRemove.add(serviceID)

        # Drop everything from that set as a value and a key
        self.serviceTree = {
            k: [x for x in v if x not in toRemove]
            for k, v in self.serviceTree.items()
            if k not in toRemove
        }

    def clear_nonexistent_dependents(self, job_store: "AbstractJobStore") -> None:
        """
        Remove all references to child, follow-on, and associated service jobs that do not exist.

        That is to say, all those that have been completed and removed.
        """
        predicate = lambda j: job_store.job_exists(j)
        self.filterSuccessors(predicate)
        self.filterServiceHosts(predicate)

    def clear_dependents(self) -> None:
        """Remove all references to successor and service jobs."""
        self.childIDs = set()
        self.followOnIDs = set()
        self.successor_phases = [self.followOnIDs, self.childIDs]
        self.serviceTree = {}

    def is_subtree_done(self) -> bool:
        """
        Check if the subtree is done.

        :returns: True if the job appears to be done, and all related child,
                  follow-on, and service jobs appear to be finished and removed.
        """
        return (
            not self.has_body() and next(self.successorsAndServiceHosts(), None) is None
        )

    def replace(self, other: "JobDescription") -> None:
        """
        Take on the ID of another JobDescription, retaining our own state and type.

        When updated in the JobStore, we will save over the other JobDescription.

        Useful for chaining jobs: the chained-to job can replace the parent job.

        Merges cleanup state and successors other than this job from the job
        being replaced into this one.

        :param other: Job description to replace.
        """

        # TODO: We can't join the job graphs with Job._jobGraphsJoined, is that a problem?

        # Take all the successors other than this one
        old_phases = [
            {i for i in p if i != self.jobStoreID} for p in other.successor_phases
        ]
        # And drop empty phases
        old_phases = [p for p in old_phases if len(p) > 0]
        # And put in front of our existing phases
        logger.debug(
            "%s is adopting successor phases from %s of: %s", self, other, old_phases
        )
        self.successor_phases = old_phases + self.successor_phases

        # When deleting, we need to delete the files for our old ID, and also
        # anything that needed to be deleted for the job we are replacing. And
        # we need to keep track of all the names of jobs involved for logging.

        # We need first the job we are merging into if nothing has merged into
        # it yet, then anything that already merged into it (including it),
        # then us if nothing has yet merged into us, then anything that merged
        # into us (inclusing us)
        _merged_job_names = []
        if len(other._merged_job_names) == 0:
            _merged_job_names.append(other.get_names())
        _merged_job_names += other._merged_job_names
        if len(self._merged_job_names) == 0:
            _merged_job_names.append(self.get_names())
        _merged_job_names += self._merged_job_names
        self._merged_job_names = _merged_job_names

        # Now steal its ID.
        self.jobStoreID = other.jobStoreID

        if len(other.filesToDelete) > 0:
            raise RuntimeError(
                "Trying to take on the ID of a job that is in the process of being committed!"
            )
        if len(self.filesToDelete) > 0:
            raise RuntimeError(
                "Trying to take on the ID of anothe job while in the process of being committed!"
            )

        self._job_version = other._job_version
        self._job_version_writer = os.getpid()

    def assert_is_not_newer_than(self, other: "JobDescription") -> None:
        """
        Make sure this JobDescription is not newer than a prospective new version of the JobDescription.
        """
        if other._job_version < self._job_version:
            raise RuntimeError(
                f"Cannot replace {self} from PID {self._job_version_writer} with older version {other} from PID {other._job_version_writer}"
            )

    def is_updated_by(self, other: "JobDescription") -> bool:
        """
        Return True if the passed JobDescription is a distinct, newer version of this one.
        """

        if self.jobStoreID != other.jobStoreID:
            # Not the same job
            logger.warning(
                "Found ID %s in job %s from PID %s but expected ID %s to "
                "update job %s from PID %s",
                other.jobStoreID,
                other,
                other._job_version_writer,
                self.jobStoreID,
                self,
                self._job_version_writer,
            )
            return False

        if self._job_version >= other._job_version:
            # Version isn't strictly newer
            logger.debug(
                "Expected newer version in job %s from PID %s but it is no "
                "newer than job %s from PID %s",
                other,
                other._job_version_writer,
                self,
                self._job_version_writer,
            )
            return False

        return True

    def addChild(self, childID: str) -> None:
        """Make the job with the given ID a child of the described job."""
        self.childIDs.add(childID)

    def addFollowOn(self, followOnID: str) -> None:
        """Make the job with the given ID a follow-on of the described job."""
        self.followOnIDs.add(followOnID)

    def addServiceHostJob(self, serviceID, parentServiceID=None):
        """
        Make the ServiceHostJob with the given ID a service of the described job.

        If a parent ServiceHostJob ID is given, that parent service will be started
        first, and must have already been added.
        """
        # Make sure we aren't clobbering something
        if serviceID in self.serviceTree:
            raise RuntimeError("Job is already in the service tree.")
        self.serviceTree[serviceID] = []
        if parentServiceID is not None:
            self.serviceTree[parentServiceID].append(serviceID)

    def hasChild(self, childID: str) -> bool:
        """Return True if the job with the given ID is a child of the described job."""
        return childID in self.childIDs

    def hasFollowOn(self, followOnID: str) -> bool:
        """Test if the job with the given ID is a follow-on of the described job."""
        return followOnID in self.followOnIDs

    def hasServiceHostJob(self, serviceID) -> bool:
        """Test if the ServiceHostJob is a service of the described job."""
        return serviceID in self.serviceTree

    def renameReferences(self, renames: dict[TemporaryID, str]) -> None:
        """
        Apply the given dict of ID renames to all references to jobs.

        Does not modify our own ID or those of finished predecessors.
        IDs not present in the renames dict are left as-is.

        :param renames: Rename operations to apply.
        """
        for phase in self.successor_phases:
            items = list(phase)
            for item in items:
                if isinstance(item, TemporaryID) and item in renames:
                    # Replace each renamed item one at a time to preserve set identity
                    phase.remove(item)
                    phase.add(renames[item])
        self.serviceTree = {
            renames.get(parent, parent): [
                renames.get(child, child) for child in children
            ]
            for parent, children in self.serviceTree.items()
        }

    def addPredecessor(self) -> None:
        """Notify the JobDescription that a predecessor has been added to its Job."""
        self.predecessorNumber += 1

    def onRegistration(self, jobStore: "AbstractJobStore") -> None:
        """
        Perform setup work that requires the JobStore.

        Called by the Job saving logic when this JobDescription meets the JobStore and has its ID assigned.

        Overridden to perform setup work (like hooking up flag files for service
        jobs) that requires the JobStore.

        :param jobStore: The job store we are being placed into
        """

    def setupJobAfterFailure(
        self,
        exit_status: Optional[int] = None,
        exit_reason: Optional["BatchJobExitReason"] = None,
    ) -> None:
        """
        Configure job after a failure.

        Reduce the remainingTryCount if greater than zero and set the memory
        to be at least as big as the default memory (in case of exhaustion of memory,
        which is common).

        Requires a configuration to have been assigned (see :meth:`toil.job.Requirer.assignConfig`).

        :param exit_status: The exit code from the job.
        :param exit_reason: The reason the job stopped, if available from the batch system.
        """
        # Avoid potential circular imports
        from toil.batchSystems.abstractBatchSystem import BatchJobExitReason

        # Old version of this function used to take a config. Make sure that isn't happening.
        if isinstance(exit_status, Config):
            raise RuntimeError("Passing a Config as an exit status.")
        # Make sure we have an assigned config.
        if self._config is None:
            raise RuntimeError("The job's config is not assigned.")

        if (
            self._config.enableUnlimitedPreemptibleRetries
            and exit_reason == BatchJobExitReason.LOST
        ):
            logger.info(
                "*Not* reducing try count (%s) of job %s with ID %s",
                self.remainingTryCount,
                self,
                self.jobStoreID,
            )
        else:
            self.remainingTryCount = max(0, self.remainingTryCount - 1)
            logger.warning(
                "Due to failure we are reducing the remaining try count of job %s with ID %s to %s",
                self,
                self.jobStoreID,
                self.remainingTryCount,
            )
        # Set the default memory to be at least as large as the default, in
        # case this was a malloc failure (we do this because of the combined
        # batch system)
        if exit_reason == BatchJobExitReason.MEMLIMIT and self._config.doubleMem:
            self.memory = self.memory * 2
            logger.warning(
                "We have doubled the memory of the failed job %s to %s bytes due to doubleMem flag",
                self,
                self.memory,
            )
        if self.memory < self._config.defaultMemory:
            self.memory = self._config.defaultMemory
            logger.warning(
                "We have increased the default memory of the failed job %s to %s bytes",
                self,
                self.memory,
            )

        if self.disk < self._config.defaultDisk:
            self.disk = self._config.defaultDisk
            logger.warning(
                "We have increased the disk of the failed job %s to the default of %s bytes",
                self,
                self.disk,
            )

    def getLogFileHandle(self, jobStore):
        """
        Create a context manager that yields a file handle to the log file.

        Assumes logJobStoreFileID is set.
        """
        return jobStore.read_file_stream(self.logJobStoreFileID)

    @property
    def remainingTryCount(self):
        """
        Get the number of tries remaining.

        The try count set on the JobDescription, or the default based on the
        retry count from the config if none is set.
        """
        if self._remainingTryCount is not None:
            return self._remainingTryCount
        elif self._config is not None:
            # Our try count should be the number of retries in the config, plus
            # 1 for the initial try
            return self._config.retryCount + 1
        else:
            raise AttributeError(f"Try count for {self} cannot be determined")

    @remainingTryCount.setter
    def remainingTryCount(self, val):
        self._remainingTryCount = val

    def clearRemainingTryCount(self) -> bool:
        """
        Clear remainingTryCount and set it back to its default value.

        :returns: True if a modification to the JobDescription was made, and
                  False otherwise.
        """
        if self._remainingTryCount is not None:
            # We had a value stored
            self._remainingTryCount = None
            return True
        else:
            # No change needed
            return False

    def __str__(self) -> str:
        """Produce a useful logging string identifying this job."""
        printedName = "'" + self.jobName + "'"
        if self.unitName:
            printedName += " " + self.unitName

        if self.jobStoreID is not None:
            printedName += " " + str(self.jobStoreID)

        printedName += " v" + str(self._job_version)

        return printedName

    # Not usable as a key (not hashable) and doesn't have any value-equality.
    # There really should only ever be one true version of a JobDescription at
    # a time, keyed by jobStoreID.

    def __repr__(self):
        return f"{self.__class__.__name__}( **{self.__dict__!r} )"

    def reserve_versions(self, count: int) -> None:
        """
        Reserve a job version number for later, for journaling asynchronously.
        """
        self._job_version += count
        self._job_version_writer = os.getpid()
        logger.debug("Skip ahead to job version: %s", self)

    def pre_update_hook(self) -> None:
        """
        Run before pickling and saving a created or updated version of this job.

        Called by the job store.
        """
        self._job_version += 1
        self._job_version_writer = os.getpid()
        logger.debug("New job version: %s", self)


class ServiceJobDescription(JobDescription):
    """A description of a job that hosts a service."""

    def __init__(self, *args, **kwargs):
        """Create a ServiceJobDescription to describe a ServiceHostJob."""
        # Make the base JobDescription
        super().__init__(*args, **kwargs)

        # Set service-specific properties

        # An empty file in the jobStore which when deleted is used to signal that the service
        # should cease.
        self.terminateJobStoreID: Optional[str] = None

        # Similarly a empty file which when deleted is used to signal that the service is
        # established
        self.startJobStoreID: Optional[str] = None

        # An empty file in the jobStore which when deleted is used to signal that the service
        # should terminate signaling an error.
        self.errorJobStoreID: Optional[str] = None

    def onRegistration(self, jobStore):
        """
        Setup flag files.

        When a ServiceJobDescription first meets the JobStore, it needs to set up its flag files.
        """
        super().onRegistration(jobStore)

        self.startJobStoreID = jobStore.getEmptyFileStoreID()
        self.terminateJobStoreID = jobStore.getEmptyFileStoreID()
        self.errorJobStoreID = jobStore.getEmptyFileStoreID()


class CheckpointJobDescription(JobDescription):
    """
    A description of a job that is a checkpoint.
    """

    def __init__(self, *args, **kwargs):
        """Create a CheckpointJobDescription to describe a checkpoint job."""
        # Make the base JobDescription
        super().__init__(*args, **kwargs)

        # Set checkpoint-specific properties

        # None, or a copy of the original self._body used to reestablish the job after failure.
        self.checkpoint: Optional[JobBodyReference] = None

        # Files that can not be deleted until the job and its successors have completed
        self.checkpointFilesToDelete = []

    def set_checkpoint(self) -> str:
        """
        Save a body checkpoint into self.checkpoint
        """

        if not self.has_body():
            raise RuntimeError(f"Cannot snapshot the body of a job {self} without one")
        self.checkpoint = self._body

    def restore_checkpoint(self) -> None:
        """
        Restore the body checkpoint from self.checkpoint
        """
        if self.checkpoint is None:
            raise RuntimeError(f"Cannot restore an empty checkpoint for a job {self}")
        self._body = self.checkpoint

    def restartCheckpoint(self, jobStore: "AbstractJobStore") -> list[str]:
        """
        Restart a checkpoint after the total failure of jobs in its subtree.

        Writes the changes to the jobStore immediately. All the
        checkpoint's successors will be deleted, but its try count
        will *not* be decreased.

        Returns a list with the IDs of any successors deleted.
        """
        if self.checkpoint is None:
            raise RuntimeError(
                "Cannot restart a checkpoint job. The checkpoint was never set."
            )
        successorsDeleted = []
        all_successors = list(self.allSuccessors())
        if len(all_successors) > 0 or self.serviceTree or self.has_body():
            if self.has_body():
                if self._body != self.checkpoint:
                    raise RuntimeError(
                        "The stored body reference and checkpoint are not the same."
                    )
                logger.debug("Checkpoint job already has body set to run")
            else:
                self.restore_checkpoint()

            jobStore.update_job(self)  # Update immediately to ensure that checkpoint
            # is made before deleting any remaining successors

            if len(all_successors) > 0 or self.serviceTree:
                # If the subtree of successors is not complete restart everything
                logger.debug(
                    "Checkpoint job has unfinished successor jobs, deleting successors: %s, services: %s "
                    % (all_successors, self.serviceTree.keys())
                )

                # Delete everything on the stack, as these represent successors to clean
                # up as we restart the queue
                def recursiveDelete(jobDesc):
                    # Recursive walk the stack to delete all remaining jobs
                    for otherJobID in jobDesc.successorsAndServiceHosts():
                        if jobStore.job_exists(otherJobID):
                            recursiveDelete(jobStore.load_job(otherJobID))
                        else:
                            logger.debug("Job %s has already been deleted", otherJobID)
                    if jobDesc.jobStoreID != self.jobStoreID:
                        # Delete everything under us except us.
                        logger.debug(
                            "Checkpoint is deleting old successor job: %s",
                            jobDesc.jobStoreID,
                        )
                        jobStore.delete_job(jobDesc.jobStoreID)
                        successorsDeleted.append(jobDesc.jobStoreID)

                recursiveDelete(self)

                # Cut links to the jobs we deleted.
                self.clear_dependents()

                # Update again to commit the removal of successors.
                jobStore.update_job(self)
        return successorsDeleted


class Job:
    """
    Class represents a unit of work in toil.
    """

    def __init__(
        self,
        memory: Optional[ParseableIndivisibleResource] = None,
        cores: Optional[ParseableDivisibleResource] = None,
        disk: Optional[ParseableIndivisibleResource] = None,
        accelerators: Optional[ParseableAcceleratorRequirement] = None,
        preemptible: Optional[ParseableFlag] = None,
        preemptable: Optional[ParseableFlag] = None,
        unitName: Optional[str] = "",
        checkpoint: Optional[bool] = False,
        displayName: Optional[str] = "",
        descriptionClass: Optional[type] = None,
        local: Optional[bool] = None,
        files: Optional[set[FileID]] = None,
    ) -> None:
        """
        Job initializer.

        This method must be called by any overriding constructor.

        :param memory: the maximum number of bytes of memory the job will require to run.
        :param cores: the number of CPU cores required.
        :param disk: the amount of local disk space required by the job, expressed in bytes.
        :param accelerators: the computational accelerators required by the job. If a string, can be a string of a number, or a string specifying a model, brand, or API (with optional colon-delimited count).
        :param preemptible: if the job can be run on a preemptible node.
        :param preemptable: legacy preemptible parameter, for backwards compatibility with workflows not using the preemptible keyword
        :param unitName: Human-readable name for this instance of the job.
        :param checkpoint: if any of this job's successor jobs completely fails,
            exhausting all their retries, remove any successor jobs and rerun this job to restart the
            subtree. Job must be a leaf vertex in the job graph when initially defined, see
            :func:`toil.job.Job.checkNewCheckpointsAreCutVertices`.
        :param displayName: Human-readable job type display name.
        :param descriptionClass: Override for the JobDescription class used to describe the job.
        :param local: if the job can be run on the leader.
        :param files: Set of Files that the job will want to use.

        :type memory: int or string convertible by toil.lib.conversions.human2bytes to an int
        :type cores: float, int, or string convertible by toil.lib.conversions.human2bytes to an int
        :type disk: int or string convertible by toil.lib.conversions.human2bytes to an int
        :type accelerators: int, string, dict, or list of those. Strings and dicts must be parseable by parse_accelerator.
        :type preemptible: bool, int in {0, 1}, or string in {'false', 'true'} in any case
        :type unitName: str
        :type checkpoint: bool
        :type displayName: str
        :type descriptionClass: class
        """
        # Fill in our various names
        jobName = self.__class__.__name__
        displayName = displayName if displayName else jobName

        # Some workflows use preemptable instead of preemptible
        if preemptable and not preemptible:
            logger.warning(
                "Preemptable as a keyword has been deprecated, please use preemptible."
            )
            preemptible = preemptable
        # Build a requirements dict for the description
        requirements = {
            "memory": memory,
            "cores": cores,
            "disk": disk,
            "accelerators": accelerators,
            "preemptible": preemptible,
        }
        if descriptionClass is None:
            if checkpoint:
                # Actually describe as a checkpoint job
                descriptionClass = CheckpointJobDescription
            else:
                # Use the normal default
                descriptionClass = JobDescription
        # Create the JobDescription that owns all the scheduling information.
        # Make it with a temporary ID until we can be assigned a real one by
        # the JobStore.
        self._description = descriptionClass(
            requirements,
            jobName,
            unitName=unitName,
            displayName=displayName,
            local=local,
            files=files,
        )

        # Private class variables needed to actually execute a job, in the worker.
        # Also needed for setting up job graph structures before saving to the JobStore.

        # This dict holds a mapping from TemporaryIDs to the job objects they represent.
        # Will be shared among all jobs in a disconnected piece of the job
        # graph that hasn't been registered with a JobStore yet.
        # Make sure to initially register ourselves.
        # After real IDs are assigned, this maps from real ID to the job objects.
        self._registry = {self._description.jobStoreID: self}

        # Job relationships are all stored exactly once in the JobDescription.
        # Except for predecessor relationships which are stored here, just
        # while the user is creating the job graphs, to check for duplicate
        # relationships and to let EncapsulatedJob magically add itself as a
        # child. Note that this stores actual Job objects, to call addChild on.
        self._directPredecessors = set()

        # Note that self.__module__ is not necessarily this module, i.e. job.py. It is the module
        # defining the class self is an instance of, which may be a subclass of Job that may be
        # defined in a different module.
        self.userModule: ModuleDescriptor = ModuleDescriptor.forModule(
            self.__module__
        ).globalize()
        # Maps index paths into composite return values to lists of IDs of files containing
        # promised values for those return value items. An index path is a tuple of indices that
        # traverses a nested data structure of lists, dicts, tuples or any other type supporting
        # the __getitem__() protocol.. The special key `()` (the empty tuple) represents the
        # entire return value.
        self._rvs = collections.defaultdict(list)
        self._promiseJobStore = None
        self._fileStore = None
        self._defer = None
        self._tempDir = None

        # Holds flags set by set_debug_flag()
        self._debug_flags: set[str] = set()

    def __str__(self):
        """
        Produce a useful logging string to identify this Job and distinguish it
        from its JobDescription.
        """
        if self.description is None:
            return repr(self)
        else:
            return "Job(" + str(self.description) + ")"

    def check_initialized(self) -> None:
        """
        Ensure that Job.__init__() has been called by any subclass __init__().

        This uses the fact that the self._description instance variable should always
        be set after __init__().

        If __init__() has not been called, raise an error.
        """
        if not hasattr(self, "_description"):
            raise ValueError(
                f"Job instance of type {type(self)} has not been initialized. super().__init__() may not "
                f"have been called."
            )

    @property
    def jobStoreID(self) -> Union[str, TemporaryID]:
        """Get the ID of this Job."""
        # This is managed by the JobDescription.
        return self._description.jobStoreID

    @property
    def description(self) -> JobDescription:
        """Expose the JobDescription that describes this job."""
        return self._description

    # Instead of being a Requirer ourselves, we pass anything about
    # requirements through to the JobDescription.

    @property
    def disk(self) -> int:
        """The maximum number of bytes of disk the job will require to run."""
        return self.description.disk

    @disk.setter
    def disk(self, val):
        self.description.disk = val

    @property
    def memory(self):
        """The maximum number of bytes of memory the job will require to run."""
        return self.description.memory

    @memory.setter
    def memory(self, val):
        self.description.memory = val

    @property
    def cores(self) -> Union[int, float]:
        """The number of CPU cores required."""
        return self.description.cores

    @cores.setter
    def cores(self, val):
        self.description.cores = val

    @property
    def accelerators(self) -> list[AcceleratorRequirement]:
        """Any accelerators, such as GPUs, that are needed."""
        return self.description.accelerators

    @accelerators.setter
    def accelerators(self, val: list[ParseableAcceleratorRequirement]) -> None:
        self.description.accelerators = val

    @property
    def preemptible(self) -> bool:
        """Whether the job can be run on a preemptible node."""
        return self.description.preemptible

    @deprecated(new_function_name="preemptible")
    def preemptable(self):
        return self.description.preemptible

    @preemptible.setter
    def preemptible(self, val):
        self.description.preemptible = val

    @property
    def checkpoint(self) -> bool:
        """Determine if the job is a checkpoint job or not."""
        return isinstance(self._description, CheckpointJobDescription)

    @property
    def files_to_use(self) -> set[FileID]:
        return self.description.files_to_use

    @files_to_use.setter
    def files_to_use(self, val: set[FileID]):
        self.description.files_to_use = val

    def add_to_files_to_use(self, val: FileID):
        self.description.files_to_use.add(val)

    def remove_from_files_to_use(self, val: FileID):
        self.description.files_to_use.remove(val)

    def assignConfig(self, config: Config) -> None:
        """
        Assign the given config object.

        It will be used by various actions implemented inside the Job class.

        :param config: Config object to query
        """
        self.description.assignConfig(config)

    def run(self, fileStore: "AbstractFileStore") -> Any:
        """
        Override this function to perform work and dynamically create successor jobs.

        :param fileStore: Used to create local and globally sharable temporary
            files and to send log messages to the leader process.

        :return: The return value of the function can be passed to other jobs by means of
                 :func:`toil.job.Job.rv`.
        """

    def _jobGraphsJoined(self, other: "Job") -> None:
        """
        Called whenever the job graphs of this job and the other job may have been merged into one connected component.

        Ought to be called on the bigger registry first.

        Merges TemporaryID registries if needed.

        :param other: A job possibly from the other connected component
        """
        # Maintain the invariant that a whole connected component has a config
        # assigned if any job in it does.
        if self.description._config is None and other.description._config is not None:
            # The other component has a config assigned but this component doesn't.
            for job in self._registry.values():
                job.assignConfig(other.description._config)
        elif other.description._config is None and self.description._config is not None:
            # We have a config assigned but the other component doesn't.
            for job in other._registry.values():
                job.assignConfig(self.description._config)

        if len(self._registry) < len(other._registry):
            # Merge into the other component instead
            other._jobGraphsJoined(self)
        else:
            if self._registry != other._registry:
                # We are in fact joining connected components.

                # Steal everything from the other connected component's registry
                self._registry.update(other._registry)

                for job in other._registry.values():
                    # Point all their jobs at the new combined registry
                    job._registry = self._registry

    def addChild(self, childJob: "Job") -> "Job":
        """
        Add a childJob to be run as child of this job.

        Child jobs will be run directly after this job's
        :func:`toil.job.Job.run` method has completed.

        :return: childJob: for call chaining
        """
        if not isinstance(childJob, Job):
            raise RuntimeError("The type of the child job is not a job.")

        # Check that both jobs have been initialized
        self.check_initialized()
        childJob.check_initialized()

        # Join the job graphs
        self._jobGraphsJoined(childJob)
        # Remember the child relationship
        self._description.addChild(childJob.jobStoreID)
        # Record the temporary back-reference
        childJob._addPredecessor(self)

        return childJob

    def hasChild(self, childJob: "Job") -> bool:
        """
        Check if childJob is already a child of this job.

        :return: True if childJob is a child of the job, else False.
        """
        return self._description.hasChild(childJob.jobStoreID)

    def addFollowOn(self, followOnJob: "Job") -> "Job":
        """
        Add a follow-on job.

        Follow-on jobs will be run after the child jobs and their successors have been run.

        :return: followOnJob for call chaining
        """
        if not isinstance(followOnJob, Job):
            raise RuntimeError("The type of the follow-on job is not a job.")

        # Check that both jobs have been initialized
        self.check_initialized()
        followOnJob.check_initialized()

        # Join the job graphs
        self._jobGraphsJoined(followOnJob)
        # Remember the follow-on relationship
        self._description.addFollowOn(followOnJob.jobStoreID)
        # Record the temporary back-reference
        followOnJob._addPredecessor(self)

        return followOnJob

    def hasPredecessor(self, job: "Job") -> bool:
        """Check if a given job is already a predecessor of this job."""
        return job in self._directPredecessors

    def hasFollowOn(self, followOnJob: "Job") -> bool:
        """
        Check if given job is already a follow-on of this job.

        :return: True if the followOnJob is a follow-on of this job, else False.
        """
        return self._description.hasChild(followOnJob.jobStoreID)

    def addService(
        self, service: "Job.Service", parentService: Optional["Job.Service"] = None
    ) -> "Promise":
        """
        Add a service.

        The :func:`toil.job.Job.Service.start` method of the service will be called
        after the run method has completed but before any successors are run.
        The service's :func:`toil.job.Job.Service.stop` method will be called once
        the successors of the job have been run.

        Services allow things like databases and servers to be started and accessed
        by jobs in a workflow.

        :raises toil.job.JobException: If service has already been made the child
            of a job or another service.
        :param service: Service to add.
        :param parentService: Service that will be started before 'service' is
            started. Allows trees of services to be established. parentService must be a service
            of this job.
        :return: a promise that will be replaced with the return value from
            :func:`toil.job.Job.Service.start` of service in any successor of the job.
        """
        if parentService is not None:
            if not self.hasService(parentService):
                raise JobException("Parent service is not a service of the given job")

        if service.hostID is not None:
            raise JobException("Service has already been added to a job")

        # Create a host job for the service, ad get it an ID
        hostingJob = ServiceHostJob(service)
        self._jobGraphsJoined(hostingJob)

        # Record the relationship to the hosting job, with its parent if any.
        self._description.addServiceHostJob(
            hostingJob.jobStoreID,
            parentService.hostID if parentService is not None else None,
        )

        # For compatibility with old Cactus versions that tinker around with
        # our internals, we need to make the hosting job available as
        # self._services[-1]. TODO: Remove this when Cactus has updated.
        self._services = [hostingJob]

        # Return the promise for the service's startup result
        return hostingJob.rv()

    def hasService(self, service: "Job.Service") -> bool:
        """Return True if the given Service is a service of this job, and False otherwise."""
        return service.hostID is None or self._description.hasServiceHostJob(
            service.hostID
        )

    # Convenience functions for creating jobs

    def addChildFn(self, fn: Callable, *args, **kwargs) -> "FunctionWrappingJob":
        """
        Add a function as a child job.

        :param fn: Function to be run as a child job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.FunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new child job that wraps fn.
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addChild(
                PromisedRequirementFunctionWrappingJob.create(fn, *args, **kwargs)
            )
        else:
            return self.addChild(FunctionWrappingJob(fn, *args, **kwargs))

    def addFollowOnFn(self, fn: Callable, *args, **kwargs) -> "FunctionWrappingJob":
        """
        Add a function as a follow-on job.

        :param fn: Function to be run as a follow-on job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.FunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new follow-on job that wraps fn.
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addFollowOn(
                PromisedRequirementFunctionWrappingJob.create(fn, *args, **kwargs)
            )
        else:
            return self.addFollowOn(FunctionWrappingJob(fn, *args, **kwargs))

    def addChildJobFn(self, fn: Callable, *args, **kwargs) -> "FunctionWrappingJob":
        """
        Add a job function as a child job.

        See :class:`toil.job.JobFunctionWrappingJob` for a definition of a job function.

        :param fn: Job function to be run as a child job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.JobFunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new child job that wraps fn.
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addChild(
                PromisedRequirementJobFunctionWrappingJob.create(fn, *args, **kwargs)
            )
        else:
            return self.addChild(JobFunctionWrappingJob(fn, *args, **kwargs))

    def addFollowOnJobFn(self, fn: Callable, *args, **kwargs) -> "FunctionWrappingJob":
        """
        Add a follow-on job function.

        See :class:`toil.job.JobFunctionWrappingJob` for a definition of a job function.

        :param fn: Job function to be run as a follow-on job with ``*args`` and ``**kwargs`` as \
        arguments to this function. See toil.job.JobFunctionWrappingJob for reserved \
        keyword arguments used to specify resource requirements.
        :return: The new follow-on job that wraps fn.
        """
        if PromisedRequirement.convertPromises(kwargs):
            return self.addFollowOn(
                PromisedRequirementJobFunctionWrappingJob.create(fn, *args, **kwargs)
            )
        else:
            return self.addFollowOn(JobFunctionWrappingJob(fn, *args, **kwargs))

    @property
    def tempDir(self) -> str:
        """
        Shortcut to calling :func:`job.fileStore.getLocalTempDir`.

        Temp dir is created on first call and will be returned for first and future calls
        :return: Path to tempDir. See `job.fileStore.getLocalTempDir`
        """
        if self._tempDir is None:
            self._tempDir = self._fileStore.getLocalTempDir()
        return self._tempDir

    def log(self, text: str, level=logging.INFO) -> None:
        """Log using :func:`fileStore.log_to_leader`."""
        self._fileStore.log_to_leader(text, level)

    @staticmethod
    def wrapFn(fn, *args, **kwargs) -> "FunctionWrappingJob":
        """
        Makes a Job out of a function.

        Convenience function for constructor of :class:`toil.job.FunctionWrappingJob`.

        :param fn: Function to be run with ``*args`` and ``**kwargs`` as arguments. \
        See toil.job.JobFunctionWrappingJob for reserved keyword arguments used \
        to specify resource requirements.
        :return: The new function that wraps fn.
        """
        if PromisedRequirement.convertPromises(kwargs):
            return PromisedRequirementFunctionWrappingJob.create(fn, *args, **kwargs)
        else:
            return FunctionWrappingJob(fn, *args, **kwargs)

    @staticmethod
    def wrapJobFn(fn, *args, **kwargs) -> "JobFunctionWrappingJob":
        """
        Makes a Job out of a job function.

        Convenience function for constructor of :class:`toil.job.JobFunctionWrappingJob`.

        :param fn: Job function to be run with ``*args`` and ``**kwargs`` as arguments. \
        See toil.job.JobFunctionWrappingJob for reserved keyword arguments used \
        to specify resource requirements.
        :return: The new job function that wraps fn.
        """
        if PromisedRequirement.convertPromises(kwargs):
            return PromisedRequirementJobFunctionWrappingJob.create(fn, *args, **kwargs)
        else:
            return JobFunctionWrappingJob(fn, *args, **kwargs)

    def encapsulate(self, name: Optional[str] = None) -> "EncapsulatedJob":
        """
        Encapsulates the job, see :class:`toil.job.EncapsulatedJob`.
        Convenience function for constructor of :class:`toil.job.EncapsulatedJob`.

        :param name: Human-readable name for the encapsulated job.

        :return: an encapsulated version of this job.
        """
        return EncapsulatedJob(self, unitName=name)

    ####################################################
    # The following function is used for passing return values between
    # job run functions
    ####################################################

    def rv(self, *path) -> "Promise":
        """
        Create a *promise* (:class:`toil.job.Promise`).

        The "promise" representing a return value of the job's run method, or,
        in case of a function-wrapping job, the wrapped function's return value.

        :param (Any) path: Optional path for selecting a component of the promised return value.
               If absent or empty, the entire return value will be used. Otherwise, the first
               element of the path is used to select an individual item of the return value. For
               that to work, the return value must be a list, dictionary or of any other type
               implementing the `__getitem__()` magic method. If the selected item is yet another
               composite value, the second element of the path can be used to select an item from
               it, and so on. For example, if the return value is `[6,{'a':42}]`, `.rv(0)` would
               select `6` , `rv(1)` would select `{'a':3}` while `rv(1,'a')` would select `3`. To
               select a slice from a return value that is slicable, e.g. tuple or list, the path
               element should be a `slice` object. For example, assuming that the return value is
               `[6, 7, 8, 9]` then `.rv(slice(1, 3))` would select `[7, 8]`. Note that slicing
               really only makes sense at the end of path.

        :return: A promise representing the return value of this jobs :meth:`toil.job.Job.run`
                 method.
        """
        return Promise(self, path)

    def registerPromise(self, path):
        if self._promiseJobStore is None:
            # We haven't had a job store set to put our return value into, so
            # we must not have been hit yet in job topological order.
            raise JobPromiseConstraintError(self)
        # TODO: can we guarantee self.jobStoreID is populated and so pass that here?
        with self._promiseJobStore.write_file_stream() as (fileHandle, jobStoreFileID):
            promise = UnfulfilledPromiseSentinel(
                str(self.description), jobStoreFileID, False
            )
            logger.debug(
                "Issuing promise %s for result of %s", jobStoreFileID, self.description
            )
            pickle.dump(promise, fileHandle, pickle.HIGHEST_PROTOCOL)
        self._rvs[path].append(jobStoreFileID)
        return self._promiseJobStore.config.jobStore, jobStoreFileID

    def prepareForPromiseRegistration(self, jobStore: "AbstractJobStore") -> None:
        """
        Set up to allow this job's promises to register themselves.

        Prepare this job (the promisor) so that its promises can register
        themselves with it, when the jobs they are promised to (promisees) are
        serialized.

        The promissee holds the reference to the promise (usually as part of the
        job arguments) and when it is being pickled, so will the promises it refers
        to. Pickling a promise triggers it to be registered with the promissor.
        """
        self._promiseJobStore = jobStore

    def _disablePromiseRegistration(self):
        """
        Called when the job data is about to be saved in the JobStore.

        No promises should attempt to register with the job after this has been
        called, because that registration would not be persisted.
        """
        self._promiseJobStore = None

    ####################################################
    # Cycle/connectivity checking
    ####################################################

    def checkJobGraphForDeadlocks(self):
        """
        Ensures that a graph of Jobs (that hasn't yet been saved to the
        JobStore) doesn't contain any pathological relationships between jobs
        that would result in deadlocks if we tried to run the jobs.

        See :func:`toil.job.Job.checkJobGraphConnected`,
        :func:`toil.job.Job.checkJobGraphAcyclic` and
        :func:`toil.job.Job.checkNewCheckpointsAreLeafVertices` for more info.

        :raises toil.job.JobGraphDeadlockException: if the job graph
            is cyclic, contains multiple roots or contains checkpoint jobs that are
            not leaf vertices when defined (see :func:`toil.job.Job.checkNewCheckpointsAreLeaves`).
        """
        self.checkJobGraphConnected()
        self.checkJobGraphAcylic()
        self.checkNewCheckpointsAreLeafVertices()

    def getRootJobs(self) -> set["Job"]:
        """
        Return the set of root job objects that contain this job.

        A root job is a job with no predecessors (i.e. which are not children, follow-ons, or services).

        Only deals with jobs created here, rather than loaded from the job store.
        """
        # Start assuming all jobs are roots
        roots = set(self._registry.keys())

        for job in self._registry.values():
            for otherID in job.description.successorsAndServiceHosts():
                # If anything is a successor or service of anything else, it isn't a root.
                if otherID in roots:
                    # Remove it if we still think it is
                    roots.remove(otherID)

        return {self._registry[jid] for jid in roots}

    def checkJobGraphConnected(self):
        """
        :raises toil.job.JobGraphDeadlockException: if :func:`toil.job.Job.getRootJobs` does \
        not contain exactly one root job.

        As execution always starts from one root job, having multiple root jobs will \
        cause a deadlock to occur.

        Only deals with jobs created here, rather than loaded from the job store.
        """
        rootJobs = self.getRootJobs()
        if len(rootJobs) != 1:
            raise JobGraphDeadlockException(
                "Graph does not contain exactly one" " root job: %s" % rootJobs
            )

    def checkJobGraphAcylic(self):
        """
        :raises toil.job.JobGraphDeadlockException: if the connected component \
        of jobs containing this job contains any cycles of child/followOn dependencies \
        in the *augmented job graph* (see below). Such cycles are not allowed \
        in valid job graphs.

        A follow-on edge (A, B) between two jobs A and B is equivalent \
        to adding a child edge to B from (1) A, (2) from each child of A, \
        and (3) from the successors of each child of A. We call each such edge \
        an edge an "implied" edge. The augmented job graph is a job graph including \
        all the implied edges.

        For a job graph G = (V, E) the algorithm is ``O(|V|^2)``. It is ``O(|V| + |E|)`` for \
        a graph with no follow-ons. The former follow-on case could be improved!

        Only deals with jobs created here, rather than loaded from the job store.
        """
        # Get the root jobs
        roots = self.getRootJobs()
        if len(roots) == 0:
            raise JobGraphDeadlockException("Graph contains no root jobs due to cycles")

        # Get implied edges
        extraEdges = self._getImpliedEdges(roots)

        # Check for directed cycles in the augmented graph
        visited = set()
        for root in roots:
            root._checkJobGraphAcylicDFS([], visited, extraEdges)

    def _checkJobGraphAcylicDFS(self, stack, visited, extraEdges):
        """DFS traversal to detect cycles in augmented job graph."""
        if self not in visited:
            visited.add(self)
            stack.append(self)
            for successor in [
                self._registry[jID]
                for jID in self.description.allSuccessors()
                if jID in self._registry
            ] + extraEdges[self]:
                # Grab all the successors in the current registry (i.e. added form this node) and look at them.
                successor._checkJobGraphAcylicDFS(stack, visited, extraEdges)
            if stack.pop() != self:
                raise RuntimeError("The stack ordering/elements was changed.")
        if self in stack:
            stack.append(self)
            raise JobGraphDeadlockException(
                "A cycle of job dependencies has been detected '%s'" % stack
            )

    @staticmethod
    def _getImpliedEdges(roots) -> dict["Job", list["Job"]]:
        """
        Gets the set of implied edges (between children and follow-ons of a common job).

        Used in Job.checkJobGraphAcylic.

        Only deals with jobs created here, rather than loaded from the job store.

        :returns: dict from Job object to list of Job objects that must be done before it can start.
        """
        # Get nodes (Job objects) in job graph
        nodes = set()
        for root in roots:
            root._collectAllSuccessors(nodes)

        ##For each follow-on edge calculate the extra implied edges
        # Adjacency list of implied edges, i.e. map of jobs to lists of jobs
        # connected by an implied edge
        extraEdges = {n: [] for n in nodes}
        for job in nodes:
            # Get all the nonempty successor phases
            phases = [p for p in job.description.successor_phases if len(p) > 0]
            for depth in range(1, len(phases)):
                # Add edges from all jobs in the earlier/upper subtrees to all
                # the roots of the later/lower subtrees

                upper = phases[depth]
                lower = phases[depth - 1]

                # Find everything in the upper subtree
                reacheable = set()
                for upperID in upper:
                    if upperID in job._registry:
                        # This is a locally added job, not an already-saved job
                        upperJob = job._registry[upperID]
                        upperJob._collectAllSuccessors(reacheable)

                for inUpper in reacheable:
                    # Add extra edges to the roots of all the lower subtrees
                    # But skip anything in the lower subtree not in the current _registry (i.e. not created hear)
                    extraEdges[inUpper] += [
                        job._registry[lowerID]
                        for lowerID in lower
                        if lowerID in job._registry
                    ]

        return extraEdges

    def checkNewCheckpointsAreLeafVertices(self) -> None:
        """
        A checkpoint job is a job that is restarted if either it fails, or if any of \
        its successors completely fails, exhausting their retries.

        A job is a leaf it is has no successors.

        A checkpoint job must be a leaf when initially added to the job graph. When its \
        run method is invoked it can then create direct successors. This restriction is made
        to simplify implementation.

        Only works on connected components of jobs not yet added to the JobStore.

        :raises toil.job.JobGraphDeadlockException: if there exists a job being added to the graph for which \
        checkpoint=True and which is not a leaf.
        """
        roots = (
            self.getRootJobs()
        )  # Roots jobs of component, these are preexisting jobs in the graph

        # All jobs in the component of the job graph containing self
        jobs = set()
        list(map(lambda x: x._collectAllSuccessors(jobs), roots))

        # Check for each job for which checkpoint is true that it is a cut vertex or leaf
        for y in [x for x in jobs if x.checkpoint]:
            if y not in roots:  # The roots are the prexisting jobs
                if not Job._isLeafVertex(y):
                    raise JobGraphDeadlockException(
                        "New checkpoint job %s is not a leaf in the job graph" % y
                    )

    ####################################################
    # Deferred function system
    ####################################################

    def defer(self, function, *args, **kwargs) -> None:
        """
        Register a deferred function, i.e. a callable that will be invoked after the current
        attempt at running this job concludes. A job attempt is said to conclude when the job
        function (or the :meth:`toil.job.Job.run` method for class-based jobs) returns, raises an
        exception or after the process running it terminates abnormally. A deferred function will
        be called on the node that attempted to run the job, even if a subsequent attempt is made
        on another node. A deferred function should be idempotent because it may be called
        multiple times on the same node or even in the same process. More than one deferred
        function may be registered per job attempt by calling this method repeatedly with
        different arguments. If the same function is registered twice with the same or different
        arguments, it will be called twice per job attempt.

        Examples for deferred functions are ones that handle cleanup of resources external to
        Toil, like Docker containers, files outside the work directory, etc.

        :param callable function: The function to be called after this job concludes.

        :param list args: The arguments to the function

        :param dict kwargs: The keyword arguments to the function
        """
        if self._defer is None:
            raise Exception(
                "A deferred function may only be registered with a job while that job is running."
            )
        self._defer(DeferredFunction.create(function, *args, **kwargs))

    ####################################################
    # The following nested classes are used for
    # creating jobtrees (Job.Runner),
    # and defining a service (Job.Service)
    ####################################################

    class Runner:
        """Used to setup and run Toil workflow."""

        @staticmethod
        def getDefaultArgumentParser(jobstore_as_flag: bool = False) -> ArgumentParser:
            """
            Get argument parser with added toil workflow options.

            :param jobstore_as_flag: make the job store option a --jobStore flag instead of a required jobStore positional argument.
            :returns: The argument parser used by a toil workflow with added Toil options.
            """
            parser = ArgParser(formatter_class=ArgumentDefaultsHelpFormatter)
            Job.Runner.addToilOptions(parser, jobstore_as_flag=jobstore_as_flag)
            return parser

        @staticmethod
        def getDefaultOptions(
            jobStore: Optional[str] = None, jobstore_as_flag: bool = False
        ) -> Namespace:
            """
            Get default options for a toil workflow.

            :param jobStore: A string describing the jobStore \
            for the workflow.
            :param jobstore_as_flag: make the job store option a --jobStore flag instead of a required jobStore positional argument.
            :returns: The options used by a toil workflow.
            """
            # setting jobstore_as_flag to True allows the user to declare the jobstore in the config file instead
            if not jobstore_as_flag and jobStore is None:
                raise RuntimeError(
                    "The jobstore argument cannot be missing if the jobstore_as_flag argument is set "
                    "to False!"
                )
            parser = Job.Runner.getDefaultArgumentParser(
                jobstore_as_flag=jobstore_as_flag
            )
            arguments = []
            if jobstore_as_flag and jobStore is not None:
                arguments = ["--jobstore", jobStore]
            if not jobstore_as_flag and jobStore is not None:
                arguments = [jobStore]
            return parser.parse_args(args=arguments)

        @staticmethod
        def addToilOptions(
            parser: Union["OptionParser", ArgumentParser],
            jobstore_as_flag: bool = False,
        ) -> None:
            """
            Adds the default toil options to an :mod:`optparse` or :mod:`argparse`
            parser object.

            :param parser: Options object to add toil options to.
            :param jobstore_as_flag: make the job store option a --jobStore flag instead of a required jobStore positional argument.
            """
            addOptions(parser, jobstore_as_flag=jobstore_as_flag)

        @staticmethod
        def startToil(job: "Job", options) -> Any:
            """
            Run the toil workflow using the given options.

            Deprecated by toil.common.Toil.start.

            (see Job.Runner.getDefaultOptions and Job.Runner.addToilOptions) starting with this
            job.
            :param job: root job of the workflow
            :raises: toil.exceptions.FailedJobsException if at the end of function \
            there remain failed jobs.
            :return: The return value of the root job's run function.
            """
            set_logging_from_options(options)
            with Toil(options) as toil:
                if not options.restart:
                    return toil.start(job)
                else:
                    return toil.restart()

    class Service(Requirer, metaclass=ABCMeta):
        """
        Abstract class used to define the interface to a service.

        Should be subclassed by the user to define services.

        Is not executed as a job; runs within a ServiceHostJob.
        """

        def __init__(
            self,
            memory=None,
            cores=None,
            disk=None,
            accelerators=None,
            preemptible=None,
            unitName=None,
        ):
            """
            Memory, core and disk requirements are specified identically to as in \
            :func:`toil.job.Job.__init__`.
            """
            # Save the requirements in ourselves so they are visible on `self` to user code.
            super().__init__(
                {
                    "memory": memory,
                    "cores": cores,
                    "disk": disk,
                    "accelerators": accelerators,
                    "preemptible": preemptible,
                }
            )

            # And the unit name
            self.unitName = unitName

            # And the name for the hosting job
            self.jobName = self.__class__.__name__

            # Record that we have as of yet no ServiceHostJob
            self.hostID = None

        @abstractmethod
        def start(self, job: "Job") -> Any:
            """
            Start the service.

            :param job: The underlying host job that the service is being run in.
                                     Can be used to register deferred functions, or to access
                                     the fileStore for creating temporary files.

            :returns: An object describing how to access the service. The object must be pickleable
                      and will be used by jobs to access the service (see :func:`toil.job.Job.addService`).
            """

        @abstractmethod
        def stop(self, job: "Job") -> None:
            """
            Stops the service. Function can block until complete.

            :param job: The underlying host job that the service is being run in.
                        Can be used to register deferred functions, or to access
                        the fileStore for creating temporary files.
            """

        def check(self) -> bool:
            """
            Checks the service is still running.

            :raise exceptions.RuntimeError: If the service failed, this will cause the service job to be labeled failed.
            :returns: True if the service is still running, else False. If False then the service job will be terminated,
                and considered a success. Important point: if the service job exits due to a failure, it should raise a
                RuntimeError, not return False!
            """

    def _addPredecessor(self, predecessorJob):
        """Adds a predecessor job to the set of predecessor jobs."""
        if predecessorJob in self._directPredecessors:
            raise ConflictingPredecessorError(predecessorJob, self)
        self._directPredecessors.add(predecessorJob)

        # Record the need for the predecessor to finish
        self._description.addPredecessor()

    @staticmethod
    def _isLeafVertex(job):
        return next(job.description.successorsAndServiceHosts(), None) is None

    @classmethod
    def _loadUserModule(cls, userModule: ModuleDescriptor):
        """
        Imports and returns the module object represented by the given module descriptor.
        """
        return userModule.load()

    @classmethod
    def _unpickle(cls, userModule, fileHandle, requireInstanceOf=None):
        """
        Unpickles an object graph from the given file handle while loading symbols \
        referencing the __main__ module from the given userModule instead.

        :param userModule:
        :param fileHandle: An open, binary-mode file handle.
        :param requireInstanceOf: If set, require result to be an instance of this class.
        :returns:
        """

        def filter_main(module_name, class_name):
            try:
                if module_name == "__main__":
                    return getattr(userModule, class_name)
                else:
                    return getattr(importlib.import_module(module_name), class_name)
            except:
                if module_name == "__main__":
                    logger.debug(
                        "Failed getting %s from module %s.", class_name, userModule
                    )
                else:
                    logger.debug(
                        "Failed getting %s from module %s.", class_name, module_name
                    )
                raise

        class FilteredUnpickler(pickle.Unpickler):
            def find_class(self, module, name):
                return filter_main(module, name)

        unpickler = FilteredUnpickler(fileHandle)

        runnable = unpickler.load()
        if requireInstanceOf is not None and not isinstance(
            runnable, requireInstanceOf
        ):
            raise RuntimeError(f"Did not find a {requireInstanceOf} when expected")

        return runnable

    def getUserScript(self) -> ModuleDescriptor:
        return self.userModule

    def _fulfillPromises(self, returnValues, jobStore):
        """
        Set the values for promises using the return values from this job's run() function.
        """
        for path, promiseFileStoreIDs in self._rvs.items():
            if not path:
                # Note that its possible for returnValues to be a promise, not an actual return
                # value. This is the case if the job returns a promise from another job. In
                # either case, we just pass it on.
                promisedValue = returnValues
            else:
                # If there is a path ...
                if isinstance(returnValues, Promise):
                    # ... and the value itself is a Promise, we need to created a new, narrower
                    # promise and pass it on.
                    promisedValue = Promise(returnValues.job, path)
                else:
                    # Otherwise, we just select the desired component of the return value.
                    promisedValue = returnValues
                    for index in path:
                        promisedValue = promisedValue[index]
            for promiseFileStoreID in promiseFileStoreIDs:
                # File may be gone if the job is a service being re-run and the accessing job is
                # already complete.
                if jobStore.file_exists(promiseFileStoreID):
                    logger.debug(
                        "Resolve promise %s from %s with a %s",
                        promiseFileStoreID,
                        self,
                        type(promisedValue),
                    )
                    with jobStore.update_file_stream(promiseFileStoreID) as fileHandle:
                        try:
                            pickle.dump(
                                promisedValue, fileHandle, pickle.HIGHEST_PROTOCOL
                            )
                        except AttributeError:
                            logger.exception(
                                "Could not pickle promise result %s", promisedValue
                            )
                            raise
                else:
                    logger.debug(
                        "Do not resolve promise %s from %s because it is no longer needed",
                        promiseFileStoreID,
                        self,
                    )

    # Functions associated with Job.checkJobGraphAcyclic to establish that the job graph does not
    # contain any cycles of dependencies:

    def _collectAllSuccessors(self, visited):
        """
        Add the job and all jobs reachable on a directed path from current node to the given set.

        Only considers jobs in this job's subgraph that are newly added, not loaded from the job store.
        """
        # Keep our own stack since we may have a stick in the graph long enough
        # to exhaust the real stack
        todo = [self]

        while len(todo) > 0:
            job = todo[-1]
            todo.pop()
            if job not in visited:
                visited.add(job)
                for successorID in job.description.allSuccessors():
                    if successorID in self._registry:
                        # We added this successor locally
                        todo.append(self._registry[successorID])

    def getTopologicalOrderingOfJobs(self) -> list["Job"]:
        """
        :returns: a list of jobs such that for all pairs of indices i, j for which i < j, \
        the job at index i can be run before the job at index j.

        Only considers jobs in this job's subgraph that are newly added, not loaded from the job store.

        Ignores service jobs.
        """
        # List of Job objects in order.
        ordering = []
        # Set of IDs of visited jobs.
        visited = set()

        # We need to recurse and traverse the graph without exhausting Python's
        # stack, so we keep our own stack of Job objects
        todo = [self]

        while len(todo) > 0:
            job = todo[-1]
            todo.pop()

            # Do not add the job to the ordering until all its predecessors have been
            # added to the ordering
            outstandingPredecessor = False
            for predJob in job._directPredecessors:
                if predJob.jobStoreID not in visited:
                    outstandingPredecessor = True
                    break
            if outstandingPredecessor:
                continue

            if job.jobStoreID not in visited:
                visited.add(job.jobStoreID)
                ordering.append(job)

                for otherID in job.description.allSuccessors():
                    if otherID in self._registry:
                        # Stack up descendants so we process children and then follow-ons.
                        # So stack up follow-ons deeper
                        todo.append(self._registry[otherID])

        return ordering

    ####################################################
    # Storing Jobs into the JobStore
    ####################################################

    def _register(self, jobStore) -> list[tuple[TemporaryID, str]]:
        """
        If this job lacks a JobStore-assigned ID, assign this job an ID.
        Must be called for each job before it is saved to the JobStore for the first time.

        :returns: A list with either one old ID, new ID pair, or an empty list
        """

        # TODO: This doesn't really have much to do with the registry. Rename
        # the registry.

        if isinstance(self.jobStoreID, TemporaryID):
            # We need to get an ID.

            # Save our fake ID
            fake = self.jobStoreID

            # Replace it with a real ID
            jobStore.assign_job_id(self.description)

            # Make sure the JobDescription can do its JobStore-related setup.
            self.description.onRegistration(jobStore)

            # Return the fake to real mapping
            return [(fake, self.description.jobStoreID)]
        else:
            # We already have an ID. No assignment or reference rewrite necessary.
            return []

    def _renameReferences(self, renames: dict[TemporaryID, str]) -> None:
        """
        Apply the given dict of ID renames to all references to other jobs.

        Ignores the registry, which is shared and assumed to already be updated.

        IDs not present in the renames dict are left as-is.

        :param renames: Rename operations to apply.
        """

        # Do renames in the description
        self._description.renameReferences(renames)

    def saveBody(self, jobStore: "AbstractJobStore") -> None:
        """
        Save the execution data for just this job to the JobStore, and fill in
        the JobDescription with the information needed to retrieve it.

        The Job's JobDescription must have already had a real jobStoreID assigned to it.

        Does not save the JobDescription.

        :param jobStore: The job store to save the job body into.
        """

        # We can't save the job in the right place for cleanup unless the
        # description has a real ID.
        if isinstance(self.jobStoreID, TemporaryID):
            raise RuntimeError(f"Tried to save job {self} without ID assigned!")

        # Note that we can't accept any more requests for our return value
        self._disablePromiseRegistration()

        # Clear out old Cactus compatibility fields that don't need to be
        # preserved and shouldn't be serialized.
        if hasattr(self, "_services"):
            delattr(self, "_services")

        # Remember fields we will overwrite
        description = self._description
        registry = self._registry
        directPredecessors = self._directPredecessors

        try:
            try:
                # Drop out the description, which the job store will manage separately
                self._description = None
                # Fix up the registry and direct predecessors for when the job is
                # loaded to be run: the registry should contain just the job itself and
                # there should be no predecessors available when the job actually runs.
                self._registry = {description.jobStoreID: self}
                self._directPredecessors = set()

                # Save the body of the job
                with jobStore.write_file_stream(
                    description.jobStoreID, cleanup=True
                ) as (fileHandle, fileStoreID):
                    pickle.dump(self, fileHandle, pickle.HIGHEST_PROTOCOL)
            finally:
                # Restore important fields (before handling errors)
                self._directPredecessors = directPredecessors
                self._registry = registry
                self._description = description
        except JobPromiseConstraintError as e:
            # The user is passing promises without regard to predecessor constraints.
            if e.recipientJob is None:
                # Add ourselves as the recipient job that wanted the promise.
                e = JobPromiseConstraintError(e.promisingJob, self)
            raise e

        # Find the user script.
        # Note that getUserScript() may have been overridden. This is intended. If we used
        # self.userModule directly, we'd be getting a reference to job.py if the job was
        # specified as a function (as opposed to a class) since that is where FunctionWrappingJob
        #  is defined. What we really want is the module that was loaded as __main__,
        # and FunctionWrappingJob overrides getUserScript() to give us just that. Only then can
        # filter_main() in _unpickle( ) do its job of resolving any user-defined type or function.
        userScript = self.getUserScript().globalize()

        # Connect the body of the job to the JobDescription
        self._description.attach_body(fileStoreID, userScript)

    def _saveJobGraph(
        self,
        jobStore: "AbstractJobStore",
        saveSelf: bool = False,
        returnValues: bool = None,
    ):
        """
        Save job data and new JobDescriptions to the given job store for this
        job and all descending jobs, including services.

        Used to save the initial job graph containing the root job of the workflow.

        :param jobStore: The job store to save the jobs into.
        :param saveSelf: Set to True to save this job along with its children,
            follow-ons, and services, or False to just save the children, follow-ons,
            and services and to populate the return value.
        :param returnValues: The collection of values returned when executing
            the job (or starting the service the job is hosting). If saveSelf
            is not set, will be used to fulfill the job's return value promises.
        """

        # Prohibit cycles and multiple roots
        self.checkJobGraphForDeadlocks()

        # Make sure everybody in the registry is registered with the job store
        # and has an ID. Also rewrite ID references.
        allJobs = list(self._registry.values())
        # We use one big dict from fake ID to corresponding real ID to rewrite references.
        fakeToReal = {}
        for job in allJobs:
            # Register the job, get the old ID to new ID pair if any, and save that in the fake to real mapping
            fakeToReal.update(job._register(jobStore))
        if len(fakeToReal) > 0:
            # Some jobs changed ID. We need to rebuild the registry and apply the reference rewrites.

            # Remake the registry in place
            self._registry.clear()
            self._registry.update({job.jobStoreID: job for job in allJobs})

            for job in allJobs:
                # Tell all the jobs (and thus their descriptions and services)
                # about the renames.
                job._renameReferences(fakeToReal)

        # Make sure the whole component is ready for promise registration
        for job in allJobs:
            job.prepareForPromiseRegistration(jobStore)

        # Get an ordering on the non-service jobs which we use for pickling the
        # jobs in the correct order to ensure the promises are properly
        # established
        ordering = self.getTopologicalOrderingOfJobs()

        # Set up to save last job first, so promises flow the right way
        ordering.reverse()

        logger.debug(
            "Saving graph of %d jobs, %d non-service, %d new",
            len(allJobs),
            len(ordering),
            len(fakeToReal),
        )

        # Make sure we're the root
        if ordering[-1] != self:
            raise RuntimeError("The current job is not the root.")

        # Don't verify the ordering length: it excludes service host jobs.
        ordered_ids = {o.jobStoreID for o in ordering}
        for j in allJobs:
            # But do ensure all non-service-host jobs are in it.
            if not isinstance(j, ServiceHostJob) and j.jobStoreID not in ordered_ids:
                raise RuntimeError(f"{j} not found in ordering {ordering}")

        if not saveSelf:
            # Fulfil promises for return values (even if value is None)
            self._fulfillPromises(returnValues, jobStore)

        for job in ordering:
            logger.debug("Processing job %s", job.description)
            for serviceBatch in reversed(
                list(job.description.serviceHostIDsInBatches())
            ):
                # For each batch of service host jobs in reverse order they start
                for serviceID in serviceBatch:
                    logger.debug("Processing service %s", serviceID)
                    if serviceID in self._registry:
                        # It's a new service

                        # Find the actual job
                        serviceJob = self._registry[serviceID]
                        logger.debug("Saving service %s", serviceJob.description)
                        # Pickle the service body, which triggers all the promise stuff
                        serviceJob.saveBody(jobStore)
            if job != self or saveSelf:
                # Now pickle the job itself
                job.saveBody(jobStore)

        # Now that the job data is on disk, commit the JobDescriptions in
        # reverse execution order, in a batch if supported.
        with jobStore.batch():
            for job in ordering:
                for serviceBatch in job.description.serviceHostIDsInBatches():
                    for serviceID in serviceBatch:
                        if serviceID in self._registry:
                            jobStore.create_job(self._registry[serviceID].description)
                if job != self or saveSelf:
                    jobStore.create_job(job.description)

    def saveAsRootJob(self, jobStore: "AbstractJobStore") -> JobDescription:
        """
        Save this job to the given jobStore as the root job of the workflow.

        :return: the JobDescription describing this job.
        """

        # Check if the workflow root is a checkpoint but not a leaf vertex.
        # All other job vertices in the graph are checked by checkNewCheckpointsAreLeafVertices
        if self.checkpoint and not Job._isLeafVertex(self):
            raise JobGraphDeadlockException(
                "New checkpoint job %s is not a leaf in the job graph" % self
            )

        # Save the root job and all descendants and services
        self._saveJobGraph(jobStore, saveSelf=True)

        # Store the name of the first job in a file in case of restart. Up to this point the
        # root job is not recoverable. FIXME: "root job" or "first job", which one is it?
        jobStore.set_root_job(self.jobStoreID)

        # Assign the config from the JobStore as if we were loaded.
        # TODO: Find a better way to make this the JobStore's responsibility
        self.description.assignConfig(jobStore.config)

        return self.description

    @classmethod
    def loadJob(
        cls, job_store: "AbstractJobStore", job_description: JobDescription
    ) -> "Job":
        """
        Retrieves a :class:`toil.job.Job` instance from a JobStore

        :param job_store: The job store.
        :param job_description: the JobDescription of the job to retrieve.
        :returns: The job referenced by the JobDescription.
        """

        file_store_id, user_module_descriptor = job_description.get_body()
        logger.debug("Loading user module %s.", user_module_descriptor)
        user_module = cls._loadUserModule(user_module_descriptor)

        # Loads context manager using file stream
        if file_store_id == "firstJob":
            # This one is actually a shared file name and not a file ID.
            manager = job_store.read_shared_file_stream(file_store_id)
        else:
            manager = job_store.read_file_stream(file_store_id)

        # Open and unpickle
        with manager as file_handle:

            job = cls._unpickle(user_module, file_handle, requireInstanceOf=Job)
            # Fill in the current description
            job._description = job_description

            # Set up the registry again, so children and follow-ons can be added on the worker
            job._registry = {job.jobStoreID: job}

        return job

    def _run(self, jobGraph=None, fileStore=None, **kwargs):
        """
        Function which worker calls to ultimately invoke
        a job's Job.run method, and then handle created
        children/followOn jobs.

        May be (but currently is not) overridden by specialized Toil-internal jobs.

        Should not be overridden by non-Toil code!

        Despite this, it has been overridden by non-Toil code, so we keep it
        around and use a hardened kwargs-based interface to try and tolerate
        bad behavior by workflows (e.g. Cactus).

        When everyone has upgraded to a sufficiently new Cactus, we can remove
        this!

        :param NoneType jobGraph: Ignored. Here for compatibility with old
               Cactus versions that pass two positional arguments.
        :param toil.fileStores.abstractFileStore.AbstractFileStore fileStore: the
               FileStore to use to access files when running the job. Required.
        """
        return self.run(fileStore)

    @contextmanager
    def _executor(self, stats, fileStore):
        """
        This is the core wrapping method for running the job within a worker.  It sets up the stats
        and logging before yielding. After completion of the body, the function will finish up the
        stats and logging, and starts the async update process for the job.

        Will modify the job's description with changes that need to be committed back to the JobStore.
        """
        if stats is not None:
            startTime = time.time()
            startClock = ResourceMonitor.get_total_cpu_time()
        baseDir = os.getcwd()
        
        succeeded = False
        try:
            yield

            if "download_only" in self._debug_flags:
                # We should stop right away
                logger.debug("Job did not stop itself after downloading files; stopping.")
                raise DebugStoppingPointReached()

            # If the job is not a checkpoint job, add the promise files to delete
            # to the list of jobStoreFileIDs to delete
            # TODO: why is Promise holding a global list here???
            if not self.checkpoint:
                for jobStoreFileID in Promise.filesToDelete:
                    # Make sure to wrap the job store ID in a FileID object so the file store will accept it
                    # TODO: talk directly to the job store here instead.
                    fileStore.deleteGlobalFile(FileID(jobStoreFileID, 0))
            else:
                # Else copy them to the job description to delete later
                self.description.checkpointFilesToDelete = list(Promise.filesToDelete)
            Promise.filesToDelete.clear()
            # Now indicate the asynchronous update of the job can happen
            fileStore.startCommit(jobState=True)

            succeeded = True
        finally:
            # Change dir back to cwd dir, if changed by job (this is a safety issue)
            if os.getcwd() != baseDir:
                os.chdir(baseDir)
            # Finish up the stats
            if stats is not None:
                totalCpuTime, total_memory_kib = (
                    ResourceMonitor.get_total_cpu_time_and_memory_usage()
                )
                stats.jobs.append(
                    # TODO: We represent everything as strings in the stats
                    # even though the JSON transport can take bools and floats.
                    Expando(
                        start=str(startTime),
                        time=str(time.time() - startTime),
                        clock=str(totalCpuTime - startClock),
                        class_name=self._jobName(),
                        memory=str(total_memory_kib),
                        requested_cores=str(self.cores), # TODO: Isn't this really consumed cores?
                        disk=str(fileStore.get_disk_usage()),
                        succeeded=str(succeeded),
                    )
                )

    def _runner(
        self,
        jobStore: "AbstractJobStore",
        fileStore: "AbstractFileStore",
        defer: Callable[[Any], None],
        **kwargs,
    ) -> None:
        """
        Run the job, and serialise the next jobs.

        It marks the job as completed (by clearing its body) and creates the
        successor relationships to new successors, but it doesn't actually
        commit those updates to the current job into the JobStore.

        We take all arguments as keyword arguments, and accept and ignore
        additional keyword arguments, for compatibility with workflows (*cough*
        Cactus *cough*) which are reaching in and overriding _runner (which
        they aren't supposed to do). If everything is passed as name=value it
        won't break as soon as we add or remove a parameter.

        :param class jobStore: Instance of the job store
        :param fileStore: Instance of a cached or uncached filestore
        :param defer: Function yielded by open() context
               manager of :class:`toil.DeferredFunctionManager`, which is called to
               register deferred functions.
        :param kwargs: Catch-all to accept superfluous arguments passed by old
               versions of Cactus. Cactus shouldn't override this method, but it does.
        """
        # Make deferred function registration available during run().
        self._defer = defer
        # Make fileStore available as an attribute during run() ...
        self._fileStore = fileStore
        # ... but also pass it to _run() as an argument for backwards
        # compatibility with workflows that tinker around with our internals,
        # and send a fake jobGraph in case they still think jobGraph exists.
        returnValues = self._run(jobGraph=None, fileStore=fileStore)

        # Clean up state changes made for run()
        self._defer = None
        self._fileStore = None

        # Serialize the new Jobs defined by the run method to the jobStore
        self._saveJobGraph(jobStore, saveSelf=False, returnValues=returnValues)

        # Clear out the body, because the job is done.
        self.description.detach_body()

        # That and the new child/follow-on relationships will need to be
        # recorded later by an update() of the JobDescription.

    def _jobName(self):
        """
        :rtype : string, used as identifier of the job class in the stats report.
        """
        return self._description.displayName

    def set_debug_flag(self, flag: str) -> None:
        """
        Enable the given debug option on the job.
        """
        self._debug_flags.add(flag)

    def has_debug_flag(self, flag: str) -> bool:
        """
        Return true if the given debug flag is set.
        """

        return flag in self._debug_flags

    def files_downloaded_hook(
        self, host_and_job_paths: Optional[list[tuple[str, str]]] = None
    ) -> None:
        """
        Function that subclasses can call when they have downloaded their input files.

        Will abort the job if the "download_only" debug flag is set.

        Can be hinted a list of file path pairs outside and inside the job
        container, in which case the container environment can be
        reconstructed.
        """

        if self.has_debug_flag("download_only"):
            # Stop the worker!
            logger.info("Job has downloaded its files. Stopping.")
            # Send off the path mapping for the debugging wrapper.
            raise FilesDownloadedStoppingPointReached(
                "Files downloaded", host_and_job_paths=host_and_job_paths
            )


class JobException(Exception):
    """General job exception."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class JobGraphDeadlockException(JobException):
    """
    An exception raised in the event that a workflow contains an unresolvable \
    dependency, such as a cycle. See :func:`toil.job.Job.checkJobGraphForDeadlocks`.
    """

    def __init__(self, string):
        super().__init__(string)


class FunctionWrappingJob(Job):
    """
    Job used to wrap a function. In its `run` method the wrapped function is called.
    """

    def __init__(self, userFunction, *args, **kwargs):
        """
        :param callable userFunction: The function to wrap. It will be called with ``*args`` and
               ``**kwargs`` as arguments.

        The keywords ``memory``, ``cores``, ``disk``, ``accelerators`,
        ``preemptible`` and ``checkpoint`` are reserved keyword arguments that
        if specified will be used to determine the resources required for the
        job, as :func:`toil.job.Job.__init__`. If they are keyword arguments to
        the function they will be extracted from the function definition, but
        may be overridden by the user (as you would expect).
        """
        # Use the user-specified requirements, if specified, else grab the default argument
        # from the function, if specified, else default to None
        argSpec = inspect.getfullargspec(userFunction)

        if argSpec.defaults is None:
            argDict = {}
        else:
            argDict = dict(
                list(zip(argSpec.args[-len(argSpec.defaults) :], argSpec.defaults))
            )

        def resolve(key, default=None, dehumanize=False):
            try:
                # First, try constructor arguments, ...
                value = kwargs.pop(key)
            except KeyError:
                try:
                    # ..., then try default value for function keyword arguments, ...
                    value = argDict[key]
                except KeyError:
                    # ... and finally fall back to a default value.
                    value = default
            # Optionally, convert strings with metric or binary prefixes.
            if dehumanize and isinstance(value, str):
                value = human2bytes(value)
            return value

        super().__init__(
            memory=resolve("memory", dehumanize=True),
            cores=resolve("cores", dehumanize=True),
            disk=resolve("disk", dehumanize=True),
            accelerators=resolve("accelerators"),
            preemptible=resolve("preemptible"),
            checkpoint=resolve("checkpoint", default=False),
            unitName=resolve("name", default=None),
        )

        self.userFunctionModule = ModuleDescriptor.forModule(
            userFunction.__module__
        ).globalize()
        self.userFunctionName = str(userFunction.__name__)
        self.description.jobName = self.userFunctionName
        self._args = args
        self._kwargs = kwargs

    def _getUserFunction(self):
        logger.debug(
            "Loading user function %s from module %s.",
            self.userFunctionName,
            self.userFunctionModule,
        )
        userFunctionModule = self._loadUserModule(self.userFunctionModule)
        return getattr(userFunctionModule, self.userFunctionName)

    def run(self, fileStore):
        userFunction = self._getUserFunction()
        return userFunction(*self._args, **self._kwargs)

    def getUserScript(self):
        return self.userFunctionModule

    def _jobName(self):
        return ".".join(
            (
                self.__class__.__name__,
                self.userFunctionModule.name,
                self.userFunctionName,
            )
        )


class JobFunctionWrappingJob(FunctionWrappingJob):
    """
    A job function is a function whose first argument is a :class:`.Job`
    instance that is the wrapping job for the function. This can be used to
    add successor jobs for the function and perform all the functions the
    :class:`.Job` class provides.

    To enable the job function to get access to the
    :class:`toil.fileStores.abstractFileStore.AbstractFileStore` instance (see
    :func:`toil.job.Job.run`), it is made a variable of the wrapping job called
    fileStore.

    To specify a job's resource requirements the following default keyword arguments
    can be specified:

        - memory
        - disk
        - cores
        - accelerators
        - preemptible

    For example to wrap a function into a job we would call::

        Job.wrapJobFn(myJob, memory='100k', disk='1M', cores=0.1)

    """

    @property
    def fileStore(self):
        return self._fileStore

    def run(self, fileStore):
        userFunction = self._getUserFunction()
        rValue = userFunction(*((self,) + tuple(self._args)), **self._kwargs)
        return rValue


class PromisedRequirementFunctionWrappingJob(FunctionWrappingJob):
    """
    Handles dynamic resource allocation using :class:`toil.job.Promise` instances.
    Spawns child function using parent function parameters and fulfilled promised
    resource requirements.
    """

    def __init__(self, userFunction, *args, **kwargs):
        self._promisedKwargs = kwargs.copy()
        # Replace resource requirements in intermediate job with small values.
        kwargs.update(
            dict(
                disk="1M",
                memory="32M",
                cores=0.1,
                accelerators=[],
                preemptible=True,
                preemptable=True,
            )
        )
        super().__init__(userFunction, *args, **kwargs)

    @classmethod
    def create(cls, userFunction, *args, **kwargs):
        """
        Creates an encapsulated Toil job function with unfulfilled promised resource
        requirements. After the promises are fulfilled, a child job function is created
        using updated resource values. The subgraph is encapsulated to ensure that this
        child job function is run before other children in the workflow. Otherwise, a
        different child may try to use an unresolved promise return value from the parent.
        """
        return EncapsulatedJob(cls(userFunction, *args, **kwargs))

    def run(self, fileStore):
        # Assumes promises are fulfilled when parent job is run
        self.evaluatePromisedRequirements()
        userFunction = self._getUserFunction()
        return self.addChildFn(userFunction, *self._args, **self._promisedKwargs).rv()

    def evaluatePromisedRequirements(self):
        # Fulfill resource requirement promises
        for requirement in REQUIREMENT_NAMES:
            try:
                if isinstance(self._promisedKwargs[requirement], PromisedRequirement):
                    self._promisedKwargs[requirement] = self._promisedKwargs[
                        requirement
                    ].getValue()
            except KeyError:
                pass


class PromisedRequirementJobFunctionWrappingJob(PromisedRequirementFunctionWrappingJob):
    """
    Handles dynamic resource allocation for job functions.
    See :class:`toil.job.JobFunctionWrappingJob`
    """

    def run(self, fileStore):
        self.evaluatePromisedRequirements()
        userFunction = self._getUserFunction()
        return self.addChildJobFn(
            userFunction, *self._args, **self._promisedKwargs
        ).rv()


class EncapsulatedJob(Job):
    """
    A convenience Job class used to make a job subgraph appear to be a single job.

    Let A be the root job of a job subgraph and B be another job we'd like to run after A
    and all its successors have completed, for this use encapsulate::

        #  Job A and subgraph, Job B
        A, B = A(), B()
        Aprime = A.encapsulate()
        Aprime.addChild(B)
        #  B will run after A and all its successors have completed, A and its subgraph of
        # successors in effect appear to be just one job.

    If the job being encapsulated has predecessors (e.g. is not the root job), then the encapsulated
    job will inherit these predecessors. If predecessors are added to the job being encapsulated
    after the encapsulated job is created then the encapsulating job will NOT inherit these
    predecessors automatically. Care should be exercised to ensure the encapsulated job has the
    proper set of predecessors.

    The return value of an encapsulated job (as accessed by the :func:`toil.job.Job.rv` function)
    is the return value of the root job, e.g. A().encapsulate().rv() and A().rv() will resolve to
    the same value after A or A.encapsulate() has been run.
    """

    def __init__(self, job, unitName=None):
        """
        :param toil.job.Job job: the job to encapsulate.
        :param str unitName: human-readable name to identify this job instance.
        """

        if job is not None:
            # Initial construction, when encapsulating a job

            # Giving the root of the subgraph the same resources as the first job in the subgraph.
            super().__init__(**job.description.requirements, unitName=unitName)
            # Ensure that the encapsulated job has the same direct predecessors as the job
            # being encapsulated.
            for predJob in job._directPredecessors:
                predJob.addChild(self)
            self.encapsulatedJob = job
            Job.addChild(self, job)
            # Use small resource requirements for dummy Job instance.
            # But not too small, or the job won't have enough resources to safely start up Toil.
            self.encapsulatedFollowOn = Job(
                disk="100M",
                memory="512M",
                cores=0.1,
                unitName=None if unitName is None else unitName + "-followOn",
            )
            Job.addFollowOn(self, self.encapsulatedFollowOn)
        else:
            # Unpickling on the worker, to be run as a no-op.
            # No need to try and hook things up, but nobody can add children or
            # follow-ons to us now either.
            super().__init__()
            self.encapsulatedJob = None
            self.encapsulatedFollowOn = None

    def addChild(self, childJob):
        if self.encapsulatedFollowOn is None:
            raise RuntimeError(
                "Children cannot be added to EncapsulatedJob while it is running"
            )
        return Job.addChild(self.encapsulatedFollowOn, childJob)

    def addService(self, service, parentService=None):
        if self.encapsulatedFollowOn is None:
            raise RuntimeError(
                "Services cannot be added to EncapsulatedJob while it is running"
            )
        return Job.addService(
            self.encapsulatedFollowOn, service, parentService=parentService
        )

    def addFollowOn(self, followOnJob):
        if self.encapsulatedFollowOn is None:
            raise RuntimeError(
                "Follow-ons cannot be added to EncapsulatedJob while it is running"
            )
        return Job.addFollowOn(self.encapsulatedFollowOn, followOnJob)

    def rv(self, *path) -> "Promise":
        if self.encapsulatedJob is None:
            raise RuntimeError("The encapsulated job was not set.")
        return self.encapsulatedJob.rv(*path)

    def prepareForPromiseRegistration(self, jobStore):
        # This one will be called after execution when re-serializing the
        # (unchanged) graph of jobs rooted here.
        super().prepareForPromiseRegistration(jobStore)
        if self.encapsulatedJob is not None:
            # Running where the job was created.
            self.encapsulatedJob.prepareForPromiseRegistration(jobStore)

    def _disablePromiseRegistration(self):
        if self.encapsulatedJob is None:
            raise RuntimeError("The encapsulated job was not set.")
        super()._disablePromiseRegistration()
        self.encapsulatedJob._disablePromiseRegistration()

    def __reduce__(self):
        """
        Called during pickling to define the pickled representation of the job.

        We don't want to pickle our internal references to the job we
        encapsulate, so we elide them here. When actually run, we're just a
        no-op job that can maybe chain.
        """

        return self.__class__, (None,)

    def getUserScript(self):
        if self.encapsulatedJob is None:
            raise RuntimeError("The encapsulated job was not set.")
        return self.encapsulatedJob.getUserScript()


class ServiceHostJob(Job):
    """
    Job that runs a service. Used internally by Toil. Users should subclass Service instead of using this.
    """

    def __init__(self, service):
        """
        This constructor should not be called by a user.

        :param service: The service to wrap in a job.
        :type service: toil.job.Job.Service
        """

        # Make sure the service hasn't been given a host already.
        if service.hostID is not None:
            raise RuntimeError(
                "Cannot set the host. The service has already been given a host."
            )

        # Make ourselves with name info from the Service and a
        # ServiceJobDescription that has the service control flags.
        super().__init__(
            **service.requirements,
            unitName=service.unitName,
            descriptionClass=ServiceJobDescription,
        )

        # Make sure the service knows it has a host now
        service.hostID = self.jobStoreID

        # service.__module__ is the module defining the class service is an instance of.
        # Will need to be loaded before unpickling the Service
        self.serviceModule = ModuleDescriptor.forModule(service.__module__).globalize()

        # The service to run, or None if it is still pickled.
        # We can't just pickle as part of ourselves because we may need to load
        # an additional module.
        self.service = service
        # The pickled service, or None if it isn't currently pickled.
        # We can't just pickle right away because we may owe promises from it.
        self.pickledService = None

        # Pick up our name from the service.
        self.description.jobName = service.jobName

    @property
    def fileStore(self):
        """
        Return the file store, which the Service may need.
        """
        return self._fileStore

    def _renameReferences(self, renames):
        # When the job store finally hads out IDs we have to fix up the
        # back-reference from our Service to us.
        super()._renameReferences(renames)
        if self.service is not None:
            self.service.hostID = renames[self.service.hostID]

    # Since the running service has us, make sure they don't try to tack more
    # stuff onto us.

    def addChild(self, child):
        raise RuntimeError(
            "Service host jobs cannot have children, follow-ons, or services"
        )

    def addFollowOn(self, followOn):
        raise RuntimeError(
            "Service host jobs cannot have children, follow-ons, or services"
        )

    def addService(self, service, parentService=None):
        raise RuntimeError(
            "Service host jobs cannot have children, follow-ons, or services"
        )

    def saveBody(self, jobStore):
        """
        Serialize the service itself before saving the host job's body.
        """
        # Save unpickled service
        service = self.service
        # Serialize service
        self.pickledService = pickle.dumps(
            self.service, protocol=pickle.HIGHEST_PROTOCOL
        )
        # Clear real service until we have the module to load it back
        self.service = None
        # Save body as normal
        super().saveBody(jobStore)
        # Restore unpickled service
        self.service = service
        self.pickledService = None

    def run(self, fileStore):
        # Unpickle the service
        logger.debug("Loading service module %s.", self.serviceModule)
        userModule = self._loadUserModule(self.serviceModule)
        service = self._unpickle(
            userModule, BytesIO(self.pickledService), requireInstanceOf=Job.Service
        )
        self.pickledService = None
        # Make sure it has the config, since it wasn't load()-ed via the JobStore
        service.assignConfig(fileStore.jobStore.config)
        # Start the service
        startCredentials = service.start(self)
        try:
            # The start credentials  must be communicated to processes connecting to
            # the service, to do this while the run method is running we
            # cheat and set the return value promise within the run method
            self._fulfillPromises(startCredentials, fileStore.jobStore)
            self._rvs = (
                {}
            )  # Set this to avoid the return values being updated after the
            # run method has completed!

            # Now flag that the service is running jobs can connect to it
            logger.debug(
                "Removing the start jobStoreID to indicate that establishment of the service"
            )
            if self.description.startJobStoreID is None:
                raise RuntimeError("No start jobStoreID to remove.")
            if fileStore.jobStore.file_exists(self.description.startJobStoreID):
                fileStore.jobStore.delete_file(self.description.startJobStoreID)
            if fileStore.jobStore.file_exists(self.description.startJobStoreID):
                raise RuntimeError("The start jobStoreID is not a file.")

            # Now block until we are told to stop, which is indicated by the removal
            # of a file
            if self.description.terminateJobStoreID is None:
                raise RuntimeError("No terminate jobStoreID to use.")
            while True:
                # Check for the terminate signal
                if not fileStore.jobStore.file_exists(
                    self.description.terminateJobStoreID
                ):
                    logger.debug(
                        "Detected that the terminate jobStoreID has been removed so exiting"
                    )
                    if not fileStore.jobStore.file_exists(
                        self.description.errorJobStoreID
                    ):
                        raise RuntimeError(
                            "Detected the error jobStoreID has been removed so exiting with an error"
                        )
                    break

                # Check the service's status and exit if failed or complete
                try:
                    if not service.check():
                        logger.debug(
                            "The service has finished okay, but we have not been told to terminate. "
                            "Waiting for leader to tell us to come back."
                        )
                        # TODO: Adjust leader so that it keys on something
                        # other than the services finishing (assumed to be
                        # after the children) to know when to run follow-on
                        # successors. Otherwise, if we come back now, we try to
                        # run the child jobs again and get
                        # https://github.com/DataBiosphere/toil/issues/3484
                except RuntimeError:
                    logger.debug("Detected abnormal termination of the service")
                    raise

                time.sleep(
                    fileStore.jobStore.config.servicePollingInterval
                )  # Avoid excessive polling

            logger.debug("Service is done")
        finally:
            # The stop function is always called
            service.stop(self)

    def getUserScript(self):
        return self.serviceModule


class FileMetadata(NamedTuple):
    """
    Metadata for a file.
    source is the URL to grab the file from
    parent_dir is parent directory of the source
    size is the size of the file. Is none if the filesize cannot be retrieved.
    """

    source: str
    parent_dir: str
    size: Optional[int]


def potential_absolute_uris(
    uri: str,
    path: list[str],
    importer: Optional[str] = None,
    execution_dir: Optional[str] = None,
) -> Iterator[str]:
    """
    Get potential absolute URIs to check for an imported file.

    Given a URI or bare path, yield in turn all the URIs, with schemes, where we
    should actually try to find it, given that we want to search under/against
    the given paths or URIs, the current directory, and the given importing WDL
    document if any.
    """

    if uri == "":
        # Empty URIs can't come from anywhere.
        return

    # We need to brute-force find this URI relative to:
    #
    # 1. Itself if a full URI.
    #
    # 2. Importer's URL, if importer is a URL and this is a
    #    host-root-relative URL starting with / or scheme-relative
    #    starting with //, or just plain relative.
    #
    # 3. Current directory, if a relative path.
    #
    # 4. All the prefixes in "path".
    #
    # If it can't be found anywhere, we ought to (probably) throw
    # FileNotFoundError like the MiniWDL implementation does, with a
    # correct errno.
    #
    # To do this, we have AbstractFileStore.read_from_url, which can read a
    # URL into a binary-mode writable, or throw some kind of unspecified
    # exception if the source doesn't exist or can't be fetched.

    # This holds scheme-applied full URIs for all the places to search.
    full_path_list = []

    if importer is not None:
        # Add the place the imported file came form, to search first.
        full_path_list.append(Toil.normalize_uri(importer))

    # Then the current directory. We need to make sure to include a filename component here or it will treat the current directory with no trailing / as a document and relative paths will look 1 level up.
    # When importing on a worker, the cwd will be a tmpdir and will result in FileNotFoundError after os.path.abspath, so override with the execution dir
    full_path_list.append(Toil.normalize_uri(execution_dir or ".") + "/.")

    # Then the specified paths.
    # TODO:
    # https://github.com/chanzuckerberg/miniwdl/blob/e3e8ef74e80fbe59f137b0ad40b354957915c345/WDL/Tree.py#L1479-L1482
    # seems backward actually and might do these first!
    full_path_list += [Toil.normalize_uri(p) for p in path]

    # This holds all the URIs we tried and failed with.
    failures: set[str] = set()

    for candidate_base in full_path_list:
        # Try fetching based off each base URI
        candidate_uri = urljoin(candidate_base, uri)
        if candidate_uri in failures:
            # Already tried this one, maybe we have an absolute uri input.
            continue
        logger.debug(
            "Consider %s which is %s off of %s", candidate_uri, uri, candidate_base
        )

        # Try it
        yield candidate_uri
        # If we come back it didn't work
        failures.add(candidate_uri)


def get_file_sizes(
    filenames: List[str],
    file_source: AbstractJobStore,
    search_paths: Optional[List[str]] = None,
    include_remote_files: bool = True,
    execution_dir: Optional[str] = None,
) -> Dict[str, FileMetadata]:
    """
    Resolve relative-URI files in the given environment and turn them into absolute normalized URIs. Returns a dictionary of the *string values* from the WDL file values
    to a tuple of the normalized URI, parent directory ID, and size of the file. The size of the file may be None, which means unknown size.

    :param filenames: list of filenames to evaluate on
    :param file_source: Context to search for files with
    :param task_path: Dotted WDL name of the user-level code doing the
        importing (probably the workflow name).
    :param search_paths: If set, try resolving input location relative to the URLs or
        directories in this list.
    :param include_remote_files: If set, import files from remote locations. Else leave them as URI references.
    """

    @memoize
    def get_filename_size(filename: str) -> FileMetadata:
        tried = []
        for candidate_uri in potential_absolute_uris(
            filename,
            search_paths if search_paths is not None else [],
            execution_dir=execution_dir,
        ):
            tried.append(candidate_uri)
            try:
                if not include_remote_files and is_remote_url(candidate_uri):
                    # Use remote URIs in place. But we need to find the one that exists.
                    if not file_source.url_exists(candidate_uri):
                        # Wasn't found there
                        continue

                # Now we know this exists, so pass it through
                # Get filesizes
                filesize = file_source.get_size(candidate_uri)
            except UnimplementedURLException as e:
                # We can't find anything that can even support this URL scheme.
                # Report to the user, they are probably missing an extra.
                logger.critical("Error: " + str(e))
                raise
            except HTTPError as e:
                # Something went wrong looking for it there.
                logger.warning(
                    "Checked URL %s but got HTTP status %s", candidate_uri, e.code
                )
                if e.code == 405:
                    # 405 Method not allowed, maybe HEAD requests are not supported
                    filesize = None
                else:
                    # Try the next location.
                    continue
            except FileNotFoundError:
                # Wasn't found there
                continue
            except Exception:
                # Something went wrong besides the file not being found. Maybe
                # we have no auth.
                logger.error(
                    "Something went wrong when testing for existence of %s",
                    candidate_uri,
                )
                raise

            # Work out what the basename for the file was
            file_basename = os.path.basename(urlsplit(candidate_uri).path)

            if file_basename == "":
                # We can't have files with no basename because we need to
                # download them at that basename later in WDL.
                raise RuntimeError(
                    f"File {candidate_uri} has no basename"
                )

            # Was actually found
            if is_remote_url(candidate_uri):
                # Might be a file URI or other URI.
                # We need to make sure file URIs and local paths that point to
                # the same place are treated the same.
                parsed = urlsplit(candidate_uri)
                if parsed.scheme == "file:":
                    # This is a local file URI. Convert to a path for source directory tracking.
                    parent_dir = os.path.dirname(unquote(parsed.path))
                else:
                    # This is some other URL. Get the URL to the parent directory and use that.
                    parent_dir = urljoin(candidate_uri, ".")
            else:
                # Must be a local path
                parent_dir = os.path.dirname(candidate_uri)

            return cast(FileMetadata, (candidate_uri, parent_dir, filesize))
        # Not found
        raise RuntimeError(
            f"Could not find {filename} at any of: {list(potential_absolute_uris(filename, search_paths if search_paths is not None else []))}"
        )

    return {k: get_filename_size(k) for k in filenames}


class CombineImportsJob(Job):
    """
    Combine the outputs of multiple WorkerImportsJob into one promise
    """

    def __init__(self, d: Sequence[Promised[Dict[str, FileID]]], **kwargs):
        """
        :param d: Sequence of dictionaries to merge
        """
        self._d = d
        super().__init__(**kwargs)

    def run(self, file_store: AbstractFileStore) -> Promised[Dict[str, FileID]]:
        """
        Merge the dicts
        """
        d = unwrap_all(self._d)
        return {k: v for item in d for k, v in item.items()}


class WorkerImportJob(Job):
    """
    Job to do file imports on a worker instead of a leader. Assumes all local and cloud files are accessible.

    For the CWL/WDL runners, this class is only used when runImportsOnWorkers is enabled.
    """

    def __init__(
        self,
        filenames: List[str],
        local: bool = False,
        **kwargs: Any
    ):
        """
        Setup importing files on a worker.
        :param filenames: List of file URIs to import
        :param kwargs: args for the superclass
        """
        self.filenames = filenames
        super().__init__(local=local, **kwargs)

    @staticmethod
    def import_files(
        files: List[str], file_source: "AbstractJobStore"
    ) -> Dict[str, FileID]:
        """
        Import a list of files into the jobstore. Returns a mapping of the filename to the associated FileIDs

        When stream is true but the import is not streamable, the worker will run out of
        disk space and run a new import job with enough disk space instead.
        :param files: list of files to import
        :param file_source: AbstractJobStore
        :return: Dictionary mapping filenames to associated jobstore FileID
        """
        # todo: make the import ensure streaming is done instead of relying on running out of disk space
        path_to_fileid = {}

        @memoize
        def import_filename(filename: str) -> Optional[FileID]:
            return file_source.import_file(filename, symlink=True)

        for file in files:
            imported = import_filename(file)
            if imported is not None:
                path_to_fileid[file] = imported
        return path_to_fileid

    def run(self, file_store: AbstractFileStore) -> Promised[Dict[str, FileID]]:
        """
        Import the workflow inputs and then create and run the workflow.
        :return: Promise of workflow outputs
        """
        return self.import_files(self.filenames, file_store.jobStore)


class ImportsJob(Job):
    """
    Job to organize and delegate files to individual WorkerImportJobs.

    For the CWL/WDL runners, this is only used when runImportsOnWorkers is enabled
    """

    def __init__(
        self,
        file_to_data: Dict[str, FileMetadata],
        max_batch_size: ParseableIndivisibleResource,
        import_worker_disk: ParseableIndivisibleResource,
        **kwargs: Any,
    ):
        """
        Job to take the inputs for a workflow and import them on a worker instead of a leader. Assumes all local and cloud files are accessible.

        This class is only used when runImportsOnWorkers is enabled.

        :param file_to_data: mapping of file source name to file metadata
        :param max_batch_size: maximum cumulative file size of a batched import
        """
        super().__init__(local=True, **kwargs)
        self._file_to_data = file_to_data
        self._max_batch_size = max_batch_size
        self._import_worker_disk = import_worker_disk

    def run(
        self, file_store: AbstractFileStore
    ) -> Tuple[Promised[Dict[str, FileID]], Dict[str, FileMetadata]]:
        """
        Import the workflow inputs and then create and run the workflow.
        :return: Tuple of a mapping from the candidate uri to the file id and a mapping of the source filenames to its metadata. The candidate uri is a field in the file metadata
        """
        max_batch_size = self._max_batch_size
        file_to_data = self._file_to_data
        # Run WDL imports on a worker instead

        filenames = list(file_to_data.keys())

        import_jobs = []

        # This list will hold lists of batched filenames
        file_batches = []

        # List of filenames for each batch
        per_batch_files = []
        per_batch_size = 0
        while len(filenames) > 0:
            filename = filenames.pop(0)
            # See if adding this to the queue will make the batch job too big
            filesize = file_to_data[filename][2]
            if per_batch_size + filesize >= max_batch_size:
                # batch is too big now, store to schedule the batch
                if len(per_batch_files) == 0:
                    # schedule the individual file
                    per_batch_files.append(filename)
                file_batches.append(per_batch_files)
                # reset batching calculation
                per_batch_size = 0
            else:
                per_batch_size += filesize
            per_batch_files.append(filename)

        if per_batch_files:
            file_batches.append(per_batch_files)

        # Create batch import jobs for each group of files
        for batch in file_batches:
            candidate_uris = [file_to_data[filename][0] for filename in batch]
            import_jobs.append(WorkerImportJob(candidate_uris, disk=self._import_worker_disk))

        for job in import_jobs:
            self.addChild(job)

        combine_imports_job = CombineImportsJob([job.rv() for job in import_jobs])
        for job in import_jobs:
            job.addFollowOn(combine_imports_job)
        self.addChild(combine_imports_job)

        return combine_imports_job.rv(), file_to_data


class Promise:
    """
    References a return value from a method as a *promise* before the method itself is run.

    References a return value from a :meth:`toil.job.Job.run` or
    :meth:`toil.job.Job.Service.start` method as a *promise* before the method itself is run.

    Let T be a job. Instances of :class:`.Promise` (termed a *promise*) are returned by T.rv(),
    which is used to reference the return value of T's run function. When the promise is passed
    to the constructor (or as an argument to a wrapped function) of a different, successor job
    the promise will be replaced by the actual referenced return value. This mechanism allows a
    return values from one job's run method to be input argument to job before the former job's
    run function has been executed.
    """

    _jobstore: Optional["AbstractJobStore"] = None
    """
    Caches the job store instance used during unpickling to prevent it from being instantiated
    for each promise
    """

    filesToDelete = set()
    """
    A set of IDs of files containing promised values when we know we won't need them anymore
    """

    def __init__(self, job: "Job", path: Any):
        """
        Initialize this promise.

        :param Job job: the job whose return value this promise references
        :param path: see :meth:`.Job.rv`
        """
        self.job = job
        self.path = path

    def __reduce__(self):
        """
        Return the Promise class and construction arguments.

        Called during pickling when a promise (an instance of this class) is about
        to be be pickled. Returns the Promise class and construction arguments
        that will be evaluated during unpickling, namely the job store coordinates
        of a file that will hold the promised return value. By the time the
        promise is about to be unpickled, that file should be populated.
        """
        # The allocation of the file in the job store is intentionally lazy, we
        # only allocate an empty file in the job store if the promise is actually
        # being pickled. This is done so that we do not allocate files for promises
        # that are never used.
        jobStoreLocator, jobStoreFileID = self.job.registerPromise(self.path)
        # Returning a class object here causes the pickling machinery to attempt
        # to instantiate the class. We will catch that with __new__ and return
        # an the actual return value instead.
        return self.__class__, (jobStoreLocator, jobStoreFileID)

    @staticmethod
    def __new__(cls, *args) -> "Promise":
        """Instantiate this Promise."""
        if len(args) != 2:
            raise RuntimeError(
                "Cannot instantiate promise. Invalid number of arguments given (Expected 2)."
            )
        if isinstance(args[0], Job):
            # Regular instantiation when promise is created, before it is being pickled
            return super().__new__(cls)
        else:
            # Attempted instantiation during unpickling, return promised value instead
            return cls._resolve(*args)

    @classmethod
    def _resolve(cls, jobStoreLocator, jobStoreFileID):
        # Initialize the cached job store if it was never initialized in the current process or
        # if it belongs to a different workflow that was run earlier in the current process.
        if cls._jobstore is None or cls._jobstore.config.jobStore != jobStoreLocator:
            cls._jobstore = Toil.resumeJobStore(jobStoreLocator)
        cls.filesToDelete.add(jobStoreFileID)
        with cls._jobstore.read_file_stream(jobStoreFileID) as fileHandle:
            # If this doesn't work then the file containing the promise may not exist or be
            # corrupted
            value = safeUnpickleFromStream(fileHandle)
            return value


# Machinery for type-safe-ish Toil Python workflows.
#
# TODO: Until we make Promise generic on the promised type, and work out how to
# tell MyPy that rv() returns a Promise for whatever type that object's run()
# method returns, this won't actually be type-safe, because any Promise will be
# a Promised[] for any type.

T = TypeVar("T")
# We have type shorthand for a promised value.
# Uses a generic type alias, so you can have a Promised[T]. See <https://github.com/python/mypy/pull/2378>.

Promised = Union[Promise, T]


def unwrap(p: Promised[T]) -> T:
    """
    Function for ensuring you actually have a promised value, and not just a promise.
    Mostly useful for satisfying type-checking.

    The "unwrap" terminology is borrowed from Rust.
    """
    if isinstance(p, Promise):
        raise TypeError(f"Attempted to unwrap a value that is still a Promise: {p}")
    return p


def unwrap_all(p: Sequence[Promised[T]]) -> Sequence[T]:
    """
    Function for ensuring you actually have a collection of promised values,
    and not any remaining promises. Mostly useful for satisfying type-checking.

    The "unwrap" terminology is borrowed from Rust.
    """
    for i, item in enumerate(p):
        if isinstance(item, Promise):
            raise TypeError(
                f"Attempted to unwrap a value at index {i} that is still a Promise: {item}"
            )
    return p


class PromisedRequirement:
    """
    Class for dynamically allocating job function resource requirements.

    (involving :class:`toil.job.Promise` instances.)

    Use when resource requirements depend on the return value of a parent function.
    PromisedRequirements can be modified by passing a function that takes the
    :class:`.Promise` as input.

    For example, let f, g, and h be functions. Then a Toil workflow can be
    defined as follows::
    A = Job.wrapFn(f)
    B = A.addChildFn(g, cores=PromisedRequirement(A.rv())
    C = B.addChildFn(h, cores=PromisedRequirement(lambda x: 2*x, B.rv()))
    """

    def __init__(self, valueOrCallable, *args):
        """
        Initialize this Promised Requirement.

        :param valueOrCallable: A single Promise instance or a function that
                                takes args as input parameters.
        :param args: variable length argument list
        :type args: int or .Promise
        """
        if hasattr(valueOrCallable, "__call__"):
            if len(args) == 0:
                raise RuntimeError("Need parameters for PromisedRequirement function.")
            func = valueOrCallable
        else:
            if len(args) != 0:
                raise RuntimeError(
                    "Define a PromisedRequirement function to handle multiple arguments."
                )
            func = lambda x: x
            args = [valueOrCallable]

        self._func = dill.dumps(func)
        self._args = list(args)

    def getValue(self):
        """Return PromisedRequirement value."""
        func = dill.loads(self._func)
        return func(*self._args)

    @staticmethod
    def convertPromises(kwargs: dict[str, Any]) -> bool:
        """
        Return True if reserved resource keyword is a Promise or PromisedRequirement instance.

        Converts Promise instance to PromisedRequirement.

        :param kwargs: function keyword arguments
        """
        for r in REQUIREMENT_NAMES:
            if isinstance(kwargs.get(r), Promise):
                kwargs[r] = PromisedRequirement(kwargs[r])
                return True
            elif isinstance(kwargs.get(r), PromisedRequirement):
                return True
        return False


class UnfulfilledPromiseSentinel:
    """
    This should be overwritten by a proper promised value.

    Throws an exception when unpickled.
    """

    def __init__(self, fulfillingJobName: str, file_id: str, unpickled: Any) -> None:
        self.fulfillingJobName = fulfillingJobName
        self.file_id = file_id

    @staticmethod
    def __setstate__(stateDict: dict[str, Any]) -> None:
        """
        Only called when unpickling.

        This won't be unpickled unless the promise wasn't resolved, so we throw
        an exception.
        """
        jobName = stateDict["fulfillingJobName"]
        file_id = stateDict["file_id"]
        raise RuntimeError(
            f"This job was passed promise {file_id} that wasn't yet resolved when it "
            f"ran. The job {jobName} that fulfills this promise hasn't yet "
            f"finished. This means that there aren't enough constraints to "
            f"ensure the current job always runs after {jobName}. Consider adding a "
            f"follow-on indirection between this job and its parent, or adding "
            f"this job as a child/follow-on of {jobName}."
        )
