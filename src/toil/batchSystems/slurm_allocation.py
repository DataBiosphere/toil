# Copyright (C) 2015-2026 Regents of the University of California
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

import logging
import math
import os
import re
import shlex
import subprocess
from argparse import ArgumentParser, _ArgumentGroup
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

from toil.batchSystems.abstractBatchSystem import (
    InsufficientSystemResources,
    ResourcePool,
)
from toil.batchSystems.options import OptionSetter
from toil.batchSystems.singleMachine import SingleMachineBatchSystem
from toil.common import Config
from toil.job import AcceleratorRequirement, Requirer
from toil.lib.misc import CalledProcessErrorStderr, call_command

logger = logging.getLogger(__name__)


def parse_cpus_per_node(spec: str) -> list[int]:
    """
    Get each node's CPU count from Slurm's SLURM_JOB_CPUS_PER_NODE format.

    >>> parse_cpus_per_node("16(x2),8")
    [16, 16, 8]
    >>> parse_cpus_per_node("48")
    [48]
    """
    counts: list[int] = []
    for part in spec.split(","):
        count, _, repeat = part.partition("(x")
        counts.extend([int(count)] * (int(repeat.rstrip(")")) if repeat else 1))
    return counts


def parse_node_resources(details: str) -> dict[str, tuple[int, int]]:
    """
    Get the memory in bytes and the GPU count that ``scontrol show job -d``
    reports for each set of nodes of a job.

    >>> parse_node_resources(
    ...     "   Nodes=node[2-3] CPU_IDs=0-1 Mem=63488 GRES=\\n"
    ...     "   Nodes=gpu1 CPU_IDs=0-3 Mem=1024 GRES=gpu:a100:2(IDX:0-1),gpu:1(IDX:3)\\n"
    ... )
    {'node[2-3]': (66571993088, 0), 'gpu1': (1073741824, 3)}
    """
    resources: dict[str, tuple[int, int]] = {}
    for match in re.finditer(
        r"^\s*Nodes=(\S+) CPU_IDs=\S* Mem=(\d+) GRES=(\S*)", details, re.MULTILINE
    ):
        nodes, memory, gres = match.groups()
        gpus = sum(int(n) for n in re.findall(r"(?<!\w)gpu(?::[\w.-]+)?:(\d+)", gres))
        resources[nodes] = (int(memory) * 2**20, gpus)
    return resources


def expand_hostlist(hostlist: str) -> list[str]:
    """Get the node names in a Slurm hostlist, such as ``node[2-3]``."""
    return call_command(["scontrol", "show", "hostnames", hostlist]).split()


def gpu_count(accelerators: list[AcceleratorRequirement]) -> int:
    """Get how many GPUs the given accelerator requirements ask for."""
    return sum(a["count"] for a in accelerators if a["kind"] == "gpu")


class AllocationNode:
    """A node of a Slurm allocation, with the resources on it not yet given to steps."""

    def __init__(self, name: str, cores: int, memory: int, gpus: int) -> None:
        self.name = name
        self.cores = cores
        self.memory = memory
        self.gpus = gpus
        self.free_cores = cores
        self.free_memory = memory
        self.free_gpus = gpus

    def __repr__(self) -> str:
        return (
            f"AllocationNode({self.name!r}, cores={self.cores}, "
            f"memory={self.memory}, gpus={self.gpus})"
        )

    def holds(self, cores: int, memory: int, gpus: int) -> bool:
        """Say if the node has the given resources at all, free or not."""
        return cores <= self.cores and memory <= self.memory and gpus <= self.gpus

    def without(self, cores: int, memory: int) -> AllocationNode:
        """Get this node less the given cores and memory, kept for something else."""
        return AllocationNode(
            self.name,
            max(self.cores - cores, 0),
            max(self.memory - memory, 0),
            self.gpus,
        )

    def reserve(self, cores: int, memory: int, gpus: int) -> Step | None:
        """Take the given resources for a step, if they are free."""
        if (
            cores > self.free_cores
            or memory > self.free_memory
            or gpus > self.free_gpus
        ):
            return None
        self.free_cores -= cores
        self.free_memory -= memory
        self.free_gpus -= gpus
        return Step(self, cores, memory, gpus)


@dataclass(frozen=True)
class Step:
    """The resources of one node reserved for one job step."""

    node: AllocationNode
    cores: int
    memory: int
    gpus: int

    def srun_options(self) -> list[str]:
        """Get the srun options that place the step on its node and resources."""
        options = [
            f"--nodelist={self.node.name}",
            f"--cpus-per-task={self.cores}",
            f"--mem={math.ceil(self.memory / 2**20)}M",
        ]
        if self.gpus > 0:
            options.append(f"--gres=gpu:{self.gpus}")
        return options

    def release(self) -> None:
        """Give the step's resources back to its node."""
        self.node.free_cores += self.cores
        self.node.free_memory += self.memory
        self.node.free_gpus += self.gpus


class SlurmStepBatchSystem(SingleMachineBatchSystem):
    """
    Runs each job as a step of the Slurm allocation that the leader runs in,
    with ``srun``, on a node of the allocation that has room for it.

    Each step asks Slurm for exactly its job's cores, memory and GPUs, so the
    steps on a node do not share them. On the leader's own node, one default
    job's cores and memory are kept for the leader. The allocation provides
    only the GPUs it has; jobs asking for other accelerator kinds, or for more
    than any one node has, are refused.
    """

    def __init__(
        self,
        config: Config,
        maxCores: float,
        maxMemory: float,
        maxDisk: int,
        max_jobs: int | None = None,
    ) -> None:
        if "SLURM_JOB_ID" not in os.environ:
            raise RuntimeError(
                "The Slurm allocation batch system must run inside a Slurm "
                "allocation, but SLURM_JOB_ID is not set"
            )
        self.allocation = os.environ["SLURM_JOB_ID"]
        self.nodes = self._read_nodes(config)
        logger.info(
            "Running jobs as steps of Slurm job %s on %s", self.allocation, self.nodes
        )
        self.numCores = max(node.cores for node in self.nodes)
        self.physicalMemory = max(node.memory for node in self.nodes)
        super().__init__(config, maxCores, maxMemory, maxDisk, max_jobs=max_jobs)

    def _read_nodes(self, config: Config) -> list[AllocationNode]:
        """Get the allocation's nodes, less the leader's share of its own node."""
        names = expand_hostlist(os.environ["SLURM_JOB_NODELIST"])
        cores = parse_cpus_per_node(os.environ["SLURM_JOB_CPUS_PER_NODE"])
        details = call_command(["scontrol", "show", "job", "-d", self.allocation])
        resources = {
            name: node_resources
            for hostlist, node_resources in parse_node_resources(details).items()
            for name in expand_hostlist(hostlist)
        }
        nodes = [
            AllocationNode(name, node_cores, *resources[name])
            for name, node_cores in zip(names, cores, strict=True)
        ]
        leader = os.environ.get("SLURMD_NODENAME")
        return [
            (
                node.without(math.ceil(config.defaultCores), config.defaultMemory)
                if node.name == leader
                else node
            )
            for node in nodes
        ]

    def _step_name(self, jobID: int) -> str:
        return f"toil_job_{jobID}"

    def _check_accelerator_request(self, requirer: Requirer) -> None:
        for accelerator in requirer.accelerators:
            if accelerator["kind"] != "gpu":
                raise InsufficientSystemResources(
                    requirer,
                    "accelerators",
                    details=[
                        f"The Slurm allocation has no {accelerator['kind']} accelerators."
                    ],
                )

    def check_resource_request(self, requirer: Requirer) -> None:
        super().check_resource_request(requirer)
        if not any(
            node.holds(
                math.ceil(requirer.cores),
                requirer.memory,
                gpu_count(requirer.accelerators),
            )
            for node in self.nodes
        ):
            raise InsufficientSystemResources(
                requirer,
                "cores",
                details=[
                    (
                        "No node of the Slurm allocation has the job's cores, "
                        f"memory and GPUs together: {self.nodes}"
                    )
                ],
            )

    def _acquire_resources(
        self,
        jobID: int,
        coreFractions: int,
        jobMemory: int,
        jobDisk: int,
        job_accelerators: list[AcceleratorRequirement],
    ) -> Step | None:
        slots = cast(ResourcePool, self.resource_sources[0])
        if not slots.acquireNow(1):
            self._setSchedulingStatusMessage(f"Not enough job slots to run job {jobID}")
            return None
        cores = math.ceil(round(coreFractions * self.minCores, 6))
        for node in self.nodes:
            step = node.reserve(cores, jobMemory, gpu_count(job_accelerators))
            if step is not None:
                return step
        slots.release(1)
        self._setSchedulingStatusMessage(
            f"No node of the Slurm allocation has room for job {jobID} now"
        )
        return None

    def _release_acquired_resources(self, resources: Step) -> None:
        resources.release()
        cast(ResourcePool, self.resource_sources[0]).release(1)

    def _accelerator_environment(self, acquired: Step) -> dict[str, str]:
        return {}

    def _child_command(self, jobCommand: str, jobID: int, acquired: Step) -> str:
        return shlex.join(
            [
                "srun",
                f"--jobid={self.allocation}",
                "--nodes=1",
                "--ntasks=1",
                "--exact",
                f"--job-name={self._step_name(jobID)}",
                *acquired.srun_options(),
                *self.config.slurm_step_args,  # type: ignore[attr-defined]
                "/bin/sh",
                "-c",
                jobCommand,
            ]
        )

    def _stop_now(self, popens: Sequence[subprocess.Popen[bytes]]) -> list[int]:
        names = {
            self._step_name(int(self.childToJob[popen.pid]))
            for popen in popens
            if popen.pid in self.childToJob
        }
        if names:
            self._cancel_steps(names)
        pgids: list[int] = super()._stop_now(popens)
        return pgids

    def _cancel_steps(self, names: set[str]) -> None:
        """Cancel the allocation's running steps that have the given names."""
        listing = call_command(
            [
                "squeue",
                "--steps",
                f"--jobs={self.allocation}",
                "--noheader",
                "--format=%i %j",
            ]
        )
        steps = [
            step
            for step, _, name in (
                line.strip().partition(" ") for line in listing.splitlines()
            )
            if name in names
        ]
        if steps:
            try:
                call_command(["scancel", *steps])
            except CalledProcessErrorStderr as e:
                logger.warning("Could not cancel Slurm steps %s: %s", steps, e)

    @classmethod
    def add_options(cls, parser: ArgumentParser | _ArgumentGroup) -> None:
        parser.add_argument(
            "--slurmStepArgs",
            dest="slurm_step_args",
            type=shlex.split,
            default=[],
            metavar="ARGS",
            help="Extra srun arguments for each job step in the Slurm allocation, "
            "such as --overlap on Slurm versions that need it to run steps beside "
            "the batch script. Used in the slurm_allocation batch system.",
        )

    @classmethod
    def setOptions(cls, setOption: OptionSetter) -> None:
        super().setOptions(setOption)
        setOption("slurm_step_args")
