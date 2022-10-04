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
import os
import subprocess
import time
from uuid import uuid4

import boto.ec2

from toil.lib.aws import zone_to_region
from toil.lib.retry import retry
from toil.provisioners.aws import get_best_aws_zone
from toil.test import ToilTest, needs_aws_ec2, needs_fetchable_appliance

log = logging.getLogger(__name__)

@needs_aws_ec2
@needs_fetchable_appliance
class AbstractClusterTest(ToilTest):
    def __init__(self, methodName):
        super().__init__(methodName=methodName)
        self.keyName = os.getenv('TOIL_AWS_KEYNAME').strip() or 'id_rsa'
        self.clusterName = 'aws-provisioner-test-' + str(uuid4())
        self.leaderNodeType = 't2.medium'
        self.clusterType = 'mesos'
        self.zone = get_best_aws_zone()
        assert self.zone is not None, "Could not determine AWS availability zone to test in; is TOIL_AWS_ZONE set?"
        # We need a boto2 connection to EC2 to check on the cluster
        self.boto2_ec2 = boto.ec2.connect_to_region(zone_to_region(self.zone))
        # Where should we put our virtualenv?
        self.venvDir = '/tmp/venv'

    def python(self):
        """
        Return the full path to the venv Python on the leader.
        """
        return os.path.join(self.venvDir, 'bin/python')

    def pip(self):
        """
        Return the full path to the venv pip on the leader.
        """
        return os.path.join(self.venvDir, 'bin/pip')

    def destroyCluster(self) -> None:
        """
        Destroy the cluster we built, if it exists.

        Succeeds if the cluster does not currently exist.
        """
        subprocess.check_call(['toil', 'destroy-cluster', '-p=aws', '-z', self.zone, self.clusterName])

    def setUp(self):
        """
        Set up for the test.
        Must be overridden to call this method and set self.jobStore.
        """
        super().setUp()
        # Make sure that destroy works before we create any clusters.
        # If this fails, no tests will run.
        self.destroyCluster()

    def tearDown(self):
        # Note that teardown will run even if the test crashes.
        super().tearDown()
        self.destroyCluster()
        subprocess.check_call(['toil', 'clean', self.jobStore])

    def sshUtil(self, command):
        """
        Run the given command on the cluster.
        Raise subprocess.CalledProcessError if it fails.
        """

        cmd = ['toil', 'ssh-cluster', '--insecure', '-p=aws', '-z', self.zone, self.clusterName] + command
        log.info("Running %s.", str(cmd))
        p = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        # Put in non-blocking mode. See https://stackoverflow.com/a/59291466
        os.set_blocking(p.stdout.fileno(), False)
        os.set_blocking(p.stderr.fileno(), False)

        out_buffer = b''
        err_buffer = b''

        loops_since_line = 0

        running = True
        while running:
            # While the process is running, see if it stopped
            running = (p.poll() is None)

            # Also collect its output
            out_data = p.stdout.read()
            if out_data:
                out_buffer += out_data

            while out_buffer.find(b'\n') != -1:
                # And log every full line
                cut = out_buffer.find(b'\n')
                log.info('STDOUT: %s', out_buffer[0:cut].decode('utf-8', errors='ignore'))
                loops_since_line = 0
                out_buffer = out_buffer[cut+1:]

            # Same for the error
            err_data = p.stderr.read()
            if err_data:
                err_buffer += err_data

            while err_buffer.find(b'\n') != -1:
                cut = err_buffer.find(b'\n')
                log.info('STDERR: %s', err_buffer[0:cut].decode('utf-8', errors='ignore'))
                loops_since_line = 0
                err_buffer = err_buffer[cut+1:]

            loops_since_line += 1
            if loops_since_line > 60:
                log.debug('...waiting...')
                loops_since_line = 0

            time.sleep(1)

        # At the end, log the last lines
        if out_buffer:
            log.info('STDOUT: %s', out_buffer.decode('utf-8', errors='ignore'))
        if err_buffer:
            log.info('STDERR: %s', err_buffer.decode('utf-8', errors='ignore'))

        if p.returncode != 0:
            # It failed
            log.error("Failed to run %s.", str(cmd))
            raise subprocess.CalledProcessError(p.returncode, ' '.join(cmd))

    @retry(errors=[subprocess.CalledProcessError], intervals=[1, 1])
    def createClusterUtil(self, args=None):
        args = [] if args is None else args

        command = ['toil', 'launch-cluster', '-p=aws', '-z', self.zone, f'--keyPairName={self.keyName}',
                   f'--leaderNodeType={self.leaderNodeType}', f'--clusterType={self.clusterType}', '--logDebug', self.clusterName] + args

        log.debug('Launching cluster: %s', command)

        # Try creating the cluster
        subprocess.check_call(command)
        # If we fail, tearDown will destroy the cluster.

    def launchCluster(self):
        self.createClusterUtil()
