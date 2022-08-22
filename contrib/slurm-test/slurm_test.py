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
import fcntl
import itertools
import logging
import os
import subprocess
import sys
import tempfile
import textwrap
import time
from abc import ABCMeta, abstractmethod
from fractions import Fraction
from unittest import skipIf

from toil.test import (ToilTest,
                       needs_aws_batch,
                       needs_aws_s3,
                       needs_fetchable_appliance,
                       needs_gridengine,
                       needs_htcondor,
                       needs_kubernetes_installed,
                       needs_kubernetes,
                       needs_lsf,
                       needs_mesos,
                       needs_parasol,
                       needs_slurm,
                       needs_tes,
                       needs_torque,
                       slow)


logger = logging.getLogger(__name__)


class SlurmTest(ToilTest):

    def setUp(self):
        subprocess.run(["docker-compose", "up", "-d"])
        process = subprocess.run(["docker", "ps"], capture_output=True)
        captured_out = process.stdout
        with open("output.txt", "w") as f:
            f.write(captured_out.decode())

    def test(self):
        subprocess.run(["docker", "cp", "toilScript.py", "slurm-test_slurmmaster_1:/tmp"], check=True)
        subprocess.run(["docker", "cp", "test_script.sh", "slurm-test_slurmmaster_1:/tmp"], check=True)
        subprocess.run(["docker", "exec", "slurm-test_slurmmaster_1", "sudo", "apt-get", "update", "-y"])
        inst_toil = subprocess.run(["docker", "exec", "slurm-test_slurmmaster_1", "sudo", "apt", "install", "toil", "-y"], capture_output=True)
        #process = subprocess.run(["docker", "exec", "slurm-test_slurmmaster_1", "/tmp/test_script.sh"], capture_output=True)
        process = subprocess.run(["docker", "exec", "slurm-test_slurmmaster_1", "toil", "--help"], capture_output=True)
        #if process.returncode:
            #raise RuntimeError(process.stderr.decode() + process.stdout.decode())
        with open("output.txt", "w") as f:
            f.write(inst_toil.stdout.decode())
            #f.write("\n There just isnt output")
            f.write(process.stdout.decode())


        return True

    #Docker cp script into leader, then run on master
    #def tearDown(self):
        #subprocess.run(["docker-compose", "stop"])
