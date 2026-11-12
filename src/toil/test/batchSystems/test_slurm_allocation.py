import shlex
from uuid import uuid4

import pytest

import toil.batchSystems.slurm_allocation
from toil.batchSystems.abstractBatchSystem import InsufficientSystemResources
from toil.batchSystems.slurm_allocation import (
    SlurmAllocationBatchSystem,
    SlurmStepBatchSystem,
)
from toil.common import Config
from toil.job import Job, JobDescription, Requirer

GiB = 2**30

SCONTROL_DETAILS = """JobId=100 JobName=toil
   NumNodes=2 NumCPUs=8 NumTasks=2 CPUs/Task=1 ReqB:S:C:T=0:0:*:*
     Nodes=node1 CPU_IDs=0-3 Mem=16384 GRES=
     Nodes=node2 CPU_IDs=0-3 Mem=8192 GRES=gpu:a100:2(IDX:0-1)
"""

HOSTNAMES = {"node[1-2]": "node1\nnode2\n", "node1": "node1\n", "node2": "node2\n"}


class FakeSlurm:
    def __init__(self) -> None:
        self.steps = ""
        self.cancelled: list[str] = []

    def __call__(self, args: list[str], **_: object) -> str:
        if args[:3] == ["scontrol", "show", "hostnames"]:
            return HOSTNAMES[args[3]]
        if args[:4] == ["scontrol", "show", "job", "-d"]:
            return SCONTROL_DETAILS
        if args[0] == "squeue":
            return self.steps
        if args[0] == "scancel":
            self.cancelled.extend(args[1:])
            return ""
        raise RuntimeError(f"Cannot fake command call: {args}")


@pytest.fixture
def slurm(monkeypatch: pytest.MonkeyPatch) -> FakeSlurm:
    fake = FakeSlurm()
    monkeypatch.setattr(toil.batchSystems.slurm_allocation, "call_command", fake)
    monkeypatch.setenv("SLURM_JOB_ID", "100")
    monkeypatch.setenv("SLURM_JOB_NODELIST", "node[1-2]")
    monkeypatch.setenv("SLURM_JOB_CPUS_PER_NODE", "4(x2)")
    monkeypatch.setenv("SLURMD_NODENAME", "node1")
    return fake


@pytest.fixture
def batch_system(slurm: FakeSlurm, tmp_path: object):
    config = Config()
    config.workflowID = str(uuid4())
    config.workDir = str(tmp_path)
    config.cleanWorkDir = "always"
    system = SlurmStepBatchSystem(config, maxCores=64, maxMemory=64 * GiB, maxDisk=GiB)
    yield system
    system.shutdown()


def requirer(
    cores: int, memory: int, accelerators: list[dict] | None = None
) -> Requirer:
    return Requirer(
        {
            "cores": cores,
            "memory": memory,
            "disk": 1,
            "accelerators": accelerators or [],
        }
    )


def test_leader_keeps_a_default_job_on_its_node(
    batch_system: SlurmStepBatchSystem,
) -> None:
    config = batch_system.config
    assert [
        (node.name, node.cores, node.memory, node.gpus) for node in batch_system.nodes
    ] == [
        ("node1", 4 - config.defaultCores, 16 * GiB - config.defaultMemory, 0),
        ("node2", 4, 8 * GiB, 2),
    ]


def test_refuses_accelerator_kinds_the_allocation_lacks(
    batch_system: SlurmStepBatchSystem,
) -> None:
    with pytest.raises(InsufficientSystemResources):
        batch_system.check_resource_request(
            requirer(1, GiB, [{"kind": "quantum", "count": 1}])
        )


def test_refuses_more_gpus_than_a_node_has(batch_system: SlurmStepBatchSystem) -> None:
    batch_system.check_resource_request(requirer(1, GiB, [{"kind": "gpu", "count": 2}]))
    with pytest.raises(InsufficientSystemResources):
        batch_system.check_resource_request(
            requirer(1, GiB, [{"kind": "gpu", "count": 3}])
        )


def test_refuses_a_job_no_single_node_holds(batch_system: SlurmStepBatchSystem) -> None:
    batch_system.check_resource_request(requirer(4, 8 * GiB))
    batch_system.check_resource_request(requirer(3, 14 * GiB))
    with pytest.raises(InsufficientSystemResources):
        batch_system.check_resource_request(requirer(4, 10 * GiB))


def test_steps_go_to_nodes_with_room(batch_system: SlurmStepBatchSystem) -> None:
    fractions = int(1 / batch_system.minCores)
    first = batch_system._acquire_resources(1, 3 * fractions, GiB, 1, [])
    second = batch_system._acquire_resources(
        2, 3 * fractions, GiB, 1, [{"kind": "gpu", "count": 1}]
    )
    assert first is not None and first.node.name == "node1"
    assert second is not None and second.node.name == "node2"
    assert batch_system._acquire_resources(3, 2 * fractions, GiB, 1, []) is None

    batch_system._release_acquired_resources(first)
    third = batch_system._acquire_resources(3, 2 * fractions, GiB, 1, [])
    assert third is not None and third.node.name == "node1"


def test_step_command_asks_srun_for_the_step(
    batch_system: SlurmStepBatchSystem,
) -> None:
    step = batch_system._acquire_resources(
        7, int(2 / batch_system.minCores), 3 * GiB, 1, [{"kind": "gpu", "count": 2}]
    )
    assert step is not None
    assert shlex.split(batch_system._child_command("_toil_worker a b", 7, step)) == [
        "srun",
        "--jobid=100",
        "--nodes=1",
        "--ntasks=1",
        "--exact",
        "--job-name=toil_job_7",
        "--nodelist=node2",
        "--cpus-per-task=2",
        "--mem=3072M",
        "--gres=gpu:2",
        "/bin/sh",
        "-c",
        "_toil_worker a b",
    ]


def test_cancels_only_the_named_steps(
    batch_system: SlurmStepBatchSystem, slurm: FakeSlurm
) -> None:
    slurm.steps = "100.0 toil_job_1\n100.1 toil_job_2\n100.batch batch\n"
    batch_system._cancel_steps({"toil_job_2"})
    assert slurm.cancelled == ["100.1"]


def test_mixed_placement_runs_steps_and_submits_the_rest(
    batch_system: SlurmStepBatchSystem, monkeypatch: pytest.MonkeyPatch
) -> None:
    issued: list[str] = []
    monkeypatch.setattr(
        batch_system,
        "issueBatchJob",
        lambda command, job_desc, job_environment=None: issued.append(command) or 1,
    )
    mixed = SlurmAllocationBatchSystem.__new__(SlurmAllocationBatchSystem)
    mixed.localBatch = batch_system

    def job(accelerators: list[dict]) -> JobDescription:
        return JobDescription(
            requirements={
                "cores": 1,
                "memory": GiB,
                "disk": 1,
                "accelerators": accelerators,
            },
            jobName="job",
        )

    assert mixed.handleLocalJob("in allocation", job([])) == 1
    assert mixed.handleLocalJob("on a GPU", job([{"kind": "gpu", "count": 1}])) == 1
    assert (
        mixed.handleLocalJob("on a QPU", job([{"kind": "quantum", "count": 1}])) is None
    )
    assert mixed.handleLocalJob("on 3 GPUs", job([{"kind": "gpu", "count": 3}])) is None
    assert issued == ["in allocation", "on a GPU"]


def test_options_are_added_once() -> None:
    options = Job.Runner.getDefaultOptions("file:store")
    assert options.slurm_step_args == []
