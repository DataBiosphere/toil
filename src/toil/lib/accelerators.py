# Copyright (C) 2015-2022 Regents of the University of California
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

"""Accelerator (i.e. GPU) utilities for Toil"""

import os
import string
import subprocess
from typing import Union, cast
from xml.dom import minidom

from toil.job import AcceleratorRequirement
from toil.lib.memoize import memoize


@memoize
def have_working_nvidia_smi() -> bool:
    """
    Return True if the nvidia-smi binary, from nvidia's CUDA userspace
    utilities, is installed and can be run successfully.

    TODO: This isn't quite the same as the check that cwltool uses to decide if
    it can fulfill a CUDARequirement.
    """
    try:
        subprocess.check_call(["nvidia-smi"])
    except (
        FileNotFoundError,
        PermissionError,
        subprocess.CalledProcessError,
        OSError,
        UnicodeDecodeError,
    ):
        return False
    return True


@memoize
def get_host_accelerator_numbers() -> list[int]:
    """
    Work out what accelerator is what.

    For each accelerator visible to us, returns the host-side (for example,
    outside-of-Slurm-job) number for that accelerator. It is often the same as
    the apparent number.

    Can be used with Docker's --gpus='"device=#,#,#"' option to forward the
    right GPUs as seen from a Docker daemon.
    """

    for number_list_var in [
        "SLURM_STEP_GPUS",
        "SLURM_JOB_GPUS",
        "CUDA_VISIBLE_DEVICES",
        "NVIDIA_VISIBLE_DEVICES",
    ]:
        # Any of these can have a list of GPU numbers, but the CUDA/NVIDIA ones
        # also support a system of GPU GUIDs that we don't support.
        # TODO: If Slurm confinement is set we ignore any attempt to further
        # limit us with the app-level variables. Does that make sense? Writing
        # code to translate through would be hard and probably not actually
        # useful.
        if number_list_var in os.environ:
            device_string = os.environ[number_list_var]
            # Parse all the numbers we have
            device_numbers = [
                int(part) for part in device_string.split(",") if part.isnumeric()
            ]
            if len(device_numbers) > 0:
                # We found some numbers, so use those
                return device_numbers

    # If we don't see a set of limits we understand, say we have all nvidia GPUs
    return list(range(count_nvidia_gpus()))


@memoize
def have_working_nvidia_docker_runtime() -> bool:
    """
    Return True if Docker exists and can handle an "nvidia" runtime and the "--gpus" option.
    """
    try:
        # The runtime injects nvidia-smi; it doesn't seem to have to be in the image we use here
        subprocess.check_call(
            [
                "docker",
                "run",
                "--rm",
                "--runtime",
                "nvidia",
                "--gpus",
                "all",
                "ubuntu:20.04",
                "nvidia-smi",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except (
        FileNotFoundError,
        PermissionError,
        subprocess.CalledProcessError,
        OSError,
        UnicodeDecodeError,
    ):
        return False
    return True


@memoize
def count_nvidia_gpus() -> int:
    """
    Return the number of nvidia GPUs seen by nvidia-smi, or 0 if it is not working.
    """

    # I don't have nvidia-smi, but cwltool knows how to do this, so we do what
    # they do:
    # <https://github.com/common-workflow-language/cwltool/blob/6f29c59fb1b5426ef6f2891605e8fa2d08f1a8da/cwltool/cuda.py>
    # Some example output is here: <https://gist.github.com/loretoparisi/2620b777562c2dfd50d6b618b5f20867>
    try:
        return int(
            cast(
                minidom.Text,
                minidom.parseString(subprocess.check_output(["nvidia-smi", "-q", "-x"]))
                .getElementsByTagName("attached_gpus")[0]
                .firstChild,
            ).data
        )
    except:
        return 0

    # TODO: Parse each gpu > product_name > text content and convert to some
    # kind of "model" that agrees with e.g. Kubernetes naming.


@memoize
def count_amd_gpus() -> int:
    """
    Return the number of amd GPUs seen by rocm-smi, or 0 if it is not working.
    :return:
    """
    try:
        # see if the amd-smi CLI tool is installed
        # we believe this is the expected output for amd-smi, but we don't actually have and amd gpu to test against
        # so we assume the output from the amd-smi documentation:
        # https://rocm.docs.amd.com/projects/amdsmi/en/latest/how-to/using-AMD-SMI-CLI-tool.html
        out = subprocess.check_output(["amd-smi", "static"])
        gpu_count = len(
            [line for line in out.decode("utf-8").split("\n") if line.startswith("gpu")]
        )
        return gpu_count
    except (
        FileNotFoundError,
        PermissionError,
        subprocess.SubprocessError,
        OSError,
        UnicodeDecodeError,
    ):
        # if the amd-smi command fails, try rocm-smi
        # if a different exception is raised, something other than the subprocess call is wrong
        pass
    try:
        # similarly, since we don't have an AMD gpu to test against, assume the output from the rocm-smi documentation:
        # https://rocm.blogs.amd.com/software-tools-optimization/affinity/part-2/README.html#gpu-numa-configuration-rocm-smi-showtoponuma
        out = subprocess.check_output(["rocm-smi"])
        gpu_count = len(
            [
                line
                for line in out.decode("utf-8").split("\n")
                if len(line) > 0 and line[0] in string.digits
            ]
        )
        return gpu_count
    except (
        FileNotFoundError,
        PermissionError,
        subprocess.SubprocessError,
        OSError,
        UnicodeDecodeError,
    ):
        pass
    return 0


@memoize
def get_individual_local_accelerators() -> list[AcceleratorRequirement]:
    """
    Determine all the local accelerators available. Report each with count 1,
    in the order of the number that can be used to assign them.

    TODO: How will numbers work with multiple types of accelerator? We need an
    accelerator assignment API.
    """

    gpus: list[AcceleratorRequirement] = [
        {"kind": "gpu", "brand": "nvidia", "api": "cuda", "count": 1}
        for _ in range(count_nvidia_gpus())
    ]
    gpus.extend(
        [
            {"kind": "gpu", "brand": "amd", "api": "rocm", "count": 1}
            for _ in range(count_amd_gpus())
        ]
    )
    return gpus


def get_restrictive_environment_for_local_accelerators(
    accelerator_numbers: Union[set[int], list[int]]
) -> dict[str, str]:
    """
    Get environment variables which can be applied to a process to restrict it
    to using only the given accelerator numbers.

    The numbers are in the space of accelerators returned by
    get_individual_local_accelerators().
    """

    # Since we only know about nvidia GPUs right now, we can just say our
    # accelerator numbering space is the same as nvidia's GPU numbering space.
    gpu_list = ",".join(str(i) for i in accelerator_numbers)

    # Put this in several places: CUDA_VISIBLE_DEVICES for controlling
    # processes right here, and SINGULARITYENV_CUDA_VISIBLE_DEVICES for
    # propagating to Singularity containers.
    return {
        "CUDA_VISIBLE_DEVICES": gpu_list,
        "SINGULARITYENV_CUDA_VISIBLE_DEVICES": gpu_list,
    }
