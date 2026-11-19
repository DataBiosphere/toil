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
"""
The nodes of the PBS or Slurm job that Toil runs in, and the hostfile that
tells a job's MPI launcher where its processes go.
"""

import json
import os
import re
import tempfile

from toil.lib.misc import call_command

PROCESS_HOSTFILE = "TOIL_PROCESS_HOSTFILE"
"""
The environment variable holding the path of a job's process hostfile, which
has one line per process of the job, naming the host it goes on.
"""


def write_process_hostfile(hosts: list[str], directory: str) -> str:
    """
    Write a process hostfile placing one process on each of the given hosts.

    :returns: the path of the hostfile.
    """
    fd, path = tempfile.mkstemp(dir=directory, prefix="processes-", suffix=".hosts")
    with os.fdopen(fd, "w") as hostfile:
        hostfile.writelines(f"{host}\n" for host in hosts)
    return path


def short_hostname(host: str) -> str:
    """
    Get a host name without its domain.

    >>> short_hostname("node1.cluster.example.org")
    'node1'
    """
    return host.split(".")[0]


def expand_slurm_counts(counts: str) -> list[int]:
    """
    Get the count for each node from a Slurm per-node count variable, such as
    SLURM_JOB_CPUS_PER_NODE or SLURM_TASKS_PER_NODE.

    >>> expand_slurm_counts("48(x2),36")
    [48, 48, 36]
    >>> expand_slurm_counts("4")
    [4]
    """
    expanded: list[int] = []
    for group in counts.split(","):
        match = re.fullmatch(r"(\d+)(?:\(x(\d+)\))?", group)
        if match is None:
            raise ValueError(f"Unparsable Slurm per-node count: {counts!r}")
        expanded += [int(match.group(1))] * int(match.group(2) or 1)
    return expanded


def slurm_nodes(counts_variable: str) -> dict[str, int]:
    """
    Get each node of the Slurm job Toil runs in, in Slurm's order, with its
    count from the given Slurm per-node count variable.
    """
    names = call_command(
        ["scontrol", "show", "hostnames", os.environ["SLURM_JOB_NODELIST"]]
    ).split()
    counts = expand_slurm_counts(os.environ[counts_variable])
    if len(names) != len(counts):
        raise RuntimeError(
            f"Slurm job has nodes {names} but {counts_variable} gives counts {counts}"
        )
    return dict(zip(names, counts))


def pbs_cores() -> dict[str, int]:
    """
    Get each node of the PBS job Toil runs in, in PBS_NODEFILE's order, with
    the cores the job holds on it.

    PBS_NODEFILE has a line per MPI process PBS planned for a node, not per
    core, so the cores come from the job's exec_vnode.
    """
    with open(os.environ["PBS_NODEFILE"]) as nodefile:
        names = list(dict.fromkeys(line.strip() for line in nodefile if line.strip()))
    jobs = json.loads(
        call_command(["qstat", "-f", "-F", "json", os.environ["PBS_JOBID"]])
    )["Jobs"]
    (job,) = jobs.values()
    cores: dict[str, int] = {}
    # exec_vnode is like (node1:ncpus=64)+(node2[0]:ncpus=32+node2[1]:ncpus=32)
    for vnode in job["exec_vnode"].split("+"):
        name, *resources = vnode.strip("()").split(":")
        host = short_hostname(name.split("[")[0])
        for resource in resources:
            key, value = resource.split("=", 1)
            if key == "ncpus":
                cores[host] = cores.get(host, 0) + int(value)
    missing = [name for name in names if short_hostname(name) not in cores]
    if missing:
        raise RuntimeError(
            f"PBS job's exec_vnode {job['exec_vnode']!r} gives no cores on {missing}"
        )
    return {name: cores[short_hostname(name)] for name in names}


def allocation_cores() -> dict[str, int] | None:
    """
    Get each node of the PBS or Slurm job Toil runs in, with the cores the job
    holds on it, or None if Toil does not run in one.
    """
    if "PBS_NODEFILE" in os.environ:
        return pbs_cores()
    if "SLURM_JOB_NODELIST" in os.environ:
        return slurm_nodes("SLURM_JOB_CPUS_PER_NODE")
    return None


def allocation_variables() -> list[str]:
    """
    Get regular expressions matching the environment variables through which
    a launcher finds the PBS or Slurm job Toil runs in, if any.
    """
    if "PBS_NODEFILE" in os.environ:
        return ["PBS_.*"]
    if "SLURM_JOB_NODELIST" in os.environ:
        return ["SLURM_.*"]
    return []
