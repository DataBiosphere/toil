# Copyright (C) 2015-2018 Regents of the University of California
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
from future.utils import with_metaclass
from abc import ABCMeta, abstractmethod
from builtins import object
from functools import total_ordering
import logging
import os.path

from toil import subprocess
from toil import applianceSelf, customDockerInitCmd
from toil.lib.retry import never

a_short_time = 5
log = logging.getLogger(__name__)


@total_ordering
class Shape(object):
    """
    Represents a job or a node's "shape", in terms of the dimensions of memory, cores, disk and
    wall-time allocation.

    The wallTime attribute stores the number of seconds of a node allocation, e.g. 3600 for AWS.
    FIXME: and for jobs?

    The memory and disk attributes store the number of bytes required by a job (or provided by a
    node) in RAM or on disk (SSD or HDD), respectively.
    """
    def __init__(self, wallTime, memory, cores, disk, preemptable):
        self.wallTime = wallTime
        self.memory = memory
        self.cores = cores
        self.disk = disk
        self.preemptable = preemptable

    def __eq__(self, other):
        return (self.wallTime == other.wallTime and
                self.memory == other.memory and
                self.cores == other.cores and
                self.disk == other.disk and
                self.preemptable == other.preemptable)

    def greater_than(self, other):
        if self.preemptable < other.preemptable:
            return True
        elif self.preemptable > other.preemptable:
            return False
        elif self.memory > other.memory:
            return True
        elif self.memory < other.memory:
            return False
        elif self.cores > other.cores:
            return True
        elif self.cores < other.cores:
            return False
        elif self.disk > other.disk:
            return True
        elif self.disk < other.disk:
            return False
        elif self.wallTime > other.wallTime:
            return True
        elif self.wallTime < other.wallTime:
            return False
        else:
            return False

    def __gt__(self, other):
        return self.greater_than(other)

    def __repr__(self):
        return "Shape(wallTime=%s, memory=%s, cores=%s, disk=%s, preemptable=%s)" % \
               (self.wallTime,
                self.memory,
                self.cores,
                self.disk,
                self.preemptable)

    def __str__(self):
        return self.__repr__()
                
    def __hash__(self):
        # Since we replaced __eq__ we need to replace __hash__ as well.
        return hash(
            (self.wallTime,
             self.memory,
             self.cores,
             self.disk,
             self.preemptable))


class AbstractProvisioner(with_metaclass(ABCMeta, object)):
    """
    An abstract base class to represent the interface for provisioning worker nodes to use in a
    Toil cluster.
    """
    LEADER_HOME_DIR = '/root/'  # home directory in the Toil appliance on an instance

    def __init__(self, clusterName=None, zone=None, nodeStorage=50):
        """
        Initialize provisioner.

        :param clusterName: The cluster identifier.
        :param zone: The zone the cluster runs in.
        :param nodeStorage: The amount of storage on the worker instances, in gigabytes.
        """
        self.clusterName = clusterName
        self._zone = zone
        self._nodeStorage = nodeStorage
        self._leaderPrivateIP = None

    def readClusterSettings(self):
        """
        Initialize class from an existing cluster. This method assumes that
        the instance we are running on is the leader.
        """
        raise NotImplementedError

    def setAutoscaledNodeTypes(self, nodeTypes):
        """
        Set node types, shapes and spot bids. Preemptable nodes will have the form "type:spotBid".
        :param nodeTypes: A list of node types
        """
        self._spotBidsMap = {}
        self.nodeShapes = []
        self.nodeTypes = []
        for nodeTypeStr in nodeTypes:
            nodeBidTuple = nodeTypeStr.split(":")
            if len(nodeBidTuple) == 2:
                # This is a preemptable node type, with a spot bid
                nodeType, bid = nodeBidTuple
                self.nodeTypes.append(nodeType)
                self.nodeShapes.append(self.getNodeShape(nodeType, preemptable=True))
                self._spotBidsMap[nodeType] = bid
            else:
                self.nodeTypes.append(nodeTypeStr)
                self.nodeShapes.append(self.getNodeShape(nodeTypeStr, preemptable=False))

    @staticmethod
    def retryPredicate(e):
        """
        Return true if the exception e should be retried by the cluster scaler.
        For example, should return true if the exception was due to exceeding an API rate limit.
        The error will be retried with exponential backoff.

        :param e: exception raised during execution of setNodeCount
        :return: boolean indicating whether the exception e should be retried
        """
        return never(e)

    @abstractmethod
    def launchCluster(self, leaderNodeType, leaderStorage, owner, **kwargs):
        """
        Initialize a cluster and create a leader node.

        :param leaderNodeType: The leader instance.
        :param leaderStorage: The amount of disk to allocate to the leader in gigabytes.
        :param owner: Tag identifying the owner of the instances.

        """
        raise NotImplementedError

    @abstractmethod
    def addNodes(self, nodeType, numNodes, preemptable, spotBid=None):
        """
        Used to add worker nodes to the cluster

        :param numNodes: The number of nodes to add
        :param preemptable: whether or not the nodes will be preemptable
        :param spotBid: The bid for preemptable nodes if applicable (this can be set in config, also).
        :return: number of nodes successfully added
        """
        raise NotImplementedError

    @abstractmethod
    def terminateNodes(self, nodes):
        """
        Terminate the nodes represented by given Node objects

        :param nodes: list of Node objects
        """
        raise NotImplementedError

    @abstractmethod
    def getLeader(self):
        """
        :return: The leader node.
        """
        raise NotImplementedError

    @abstractmethod
    def getProvisionedWorkers(self, nodeType, preemptable):
        """
        Gets all nodes of the given preemptability from the provisioner.
        Includes both static and autoscaled nodes.

        :param preemptable: Boolean value indicating whether to return preemptable nodes or
           non-preemptable nodes
        :return: list of Node objects
        """
        raise NotImplementedError

    @abstractmethod
    def getNodeShape(self, nodeType=None, preemptable=False):
        """
        The shape of a preemptable or non-preemptable node managed by this provisioner. The node
        shape defines key properties of a machine, such as its number of cores or the time
        between billing intervals.

        :param str nodeType: Node type name to return the shape of.

        :rtype: Shape
        """
        raise NotImplementedError

    @abstractmethod
    def destroyCluster(self):
        """
        Terminates all nodes in the specified cluster and cleans up all resources associated with the
        cluser.
        :param clusterName: identifier of the cluster to terminate.
        """
        raise NotImplementedError

    def _setSSH(self):
        """
        Generate a key pair, save it in /root/.ssh/id_rsa.pub, and return the public key.
        The file /root/.sshSuccess is used to prevent this operation from running twice.
        :return Public key.
        """
        if not os.path.exists('/root/.sshSuccess'):
            subprocess.check_call(['ssh-keygen', '-f', '/root/.ssh/id_rsa', '-t', 'rsa', '-N', ''])
            with open('/root/.sshSuccess', 'w') as f:
                f.write('written here because of restrictive permissions on .ssh dir')
        os.chmod('/root/.ssh', 0o700)
        subprocess.check_call(['bash', '-c', 'eval $(ssh-agent) && ssh-add -k'])
        with open('/root/.ssh/id_rsa.pub') as f:
            masterPublicKey = f.read()
        masterPublicKey = masterPublicKey.split(' ')[1]  # take 'body' of key
        # confirm it really is an RSA public key
        assert masterPublicKey.startswith('AAAAB3NzaC1yc2E'), masterPublicKey
        return masterPublicKey


    cloudConfigTemplate = """#cloud-config

write_files:
    - path: "/home/core/volumes.sh"
      permissions: "0777"
      owner: "root"
      content: |
        #!/bin/bash
        set -x
        ephemeral_count=0
        drives=""
        directories="toil mesos docker cwl"
        for drive in /dev/xvd{{a..z}} /dev/nvme{{0..26}}n1; do
            echo checking for $drive
            if [ -b $drive ]; then
                echo found it
                if mount | grep $drive; then
                    echo "already mounted, likely a root device"
                else
                    ephemeral_count=$((ephemeral_count + 1 ))
                    drives="$drives $drive"
                    echo increased ephemeral count by one
                fi
            fi
        done
        if (("$ephemeral_count" == "0" )); then
            echo no ephemeral drive
            for directory in $directories; do
                sudo mkdir -p /var/lib/$directory
            done
            exit 0
        fi
        sudo mkdir /mnt/ephemeral
        if (("$ephemeral_count" == "1" )); then
            echo one ephemeral drive to mount
            sudo mkfs.ext4 -F $drives
            sudo mount $drives /mnt/ephemeral
        fi
        if (("$ephemeral_count" > "1" )); then
            echo multiple drives
            for drive in $drives; do
                dd if=/dev/zero of=$drive bs=4096 count=1024
            done
            sudo mdadm --create -f --verbose /dev/md0 --level=0 --raid-devices=$ephemeral_count $drives # determine force flag
            sudo mkfs.ext4 -F /dev/md0
            sudo mount /dev/md0 /mnt/ephemeral
        fi
        for directory in $directories; do
            sudo mkdir -p /mnt/ephemeral/var/lib/$directory
            sudo mkdir -p /var/lib/$directory
            sudo mount --bind /mnt/ephemeral/var/lib/$directory /var/lib/$directory
        done

coreos:
    update:
      reboot-strategy: off
    units:
    - name: "volume-mounting.service"
      command: "start"
      content: |
        [Unit]
        Description=mounts ephemeral volumes & bind mounts toil directories
        Before=docker.service

        [Service]
        Type=oneshot
        Restart=no
        ExecStart=/usr/bin/bash /home/core/volumes.sh

    - name: "toil-{role}.service"
      command: "start"
      content: |
        [Unit]
        Description=toil-{role} container
        After=docker.service

        [Service]
        Restart=on-failure
        RestartSec=2
        ExecStartPre=-/usr/bin/docker rm toil_{role}
        ExecStart=/usr/bin/docker run \
            --entrypoint={entrypoint} \
            --net=host \
            -v /var/run/docker.sock:/var/run/docker.sock \
            -v /var/lib/mesos:/var/lib/mesos \
            -v /var/lib/docker:/var/lib/docker \
            -v /var/lib/toil:/var/lib/toil \
            -v /var/lib/cwl:/var/lib/cwl \
            -v /tmp:/tmp \
            --name=toil_{role} \
            {dockerImage} \
            {mesosArgs}
    - name: "node-exporter.service"
      command: "start"
      content: |
        [Unit]
        Description=node-exporter container
        After=docker.service

        [Service]
        Restart=on-failure
        RestartSec=2
        ExecStartPre=-/usr/bin/docker rm node_exporter
        ExecStart=/usr/bin/docker run \
            -p 9100:9100 \
            -v /proc:/host/proc \
            -v /sys:/host/sys \
            -v /:/rootfs \
            --name node-exporter \
            --restart always \
            prom/node-exporter:v0.15.2 \
            --path.procfs /host/proc \
            --path.sysfs /host/sys \
            --collector.filesystem.ignored-mount-points ^/(sys|proc|dev|host|etc)($|/)
"""

    sshTemplate = """ssh_authorized_keys:
    - "ssh-rsa {sshKey}"
"""

    # If keys are rsynced, then the mesos-slave needs to be started after the keys have been
    # transferred. The waitForKey.sh script loops on the new VM until it finds the keyPath file, then it starts the
    # mesos-slave. If there are multiple keys to be transferred, then the last one to be transferred must be
    # set to keyPath.

    MESOS_LOG_DIR = '--log_dir=/var/lib/mesos '
    LEADER_DOCKER_ARGS = '--registry=in_memory --cluster={name}'
    # --no-systemd_enable_support is necessary in Ubuntu 16.04 (otherwise,
    # Mesos attempts to contact systemd but can't find its run file)
    WORKER_DOCKER_ARGS = '--work_dir=/var/lib/mesos --master={ip}:5050 --attributes=preemptable:{preemptable} --no-hostname_lookup --no-systemd_enable_support'


    def _getCloudConfigUserData(self, role, masterPublicKey=None, keyPath=None, preemptable=False):
        if role == 'leader':
            entryPoint = 'mesos-master'
            mesosArgs = self.MESOS_LOG_DIR + self.LEADER_DOCKER_ARGS.format(name=self.clusterName)
        elif role == 'worker':
            entryPoint = 'mesos-slave'
            mesosArgs = self.MESOS_LOG_DIR + self.WORKER_DOCKER_ARGS.format(ip=self._leaderPrivateIP,
                                                        preemptable=preemptable)
        else:
            raise RuntimeError("Unknown role %s" % role)

        template = self.cloudConfigTemplate
        if masterPublicKey:
            template += self.sshTemplate
        if keyPath:
            mesosArgs = keyPath + ' ' + mesosArgs
            entryPoint = "waitForKey.sh"
        customDockerInitCommand = customDockerInitCmd()
        if customDockerInitCommand:
            mesosArgs = " ".join(["'" + customDockerInitCommand + "'", entryPoint, mesosArgs])
            entryPoint = "customDockerInit.sh"
        templateArgs = dict(role=role,
                            dockerImage=applianceSelf(),
                            entrypoint=entryPoint,
                            sshKey=masterPublicKey,   # ignored if None
                            mesosArgs=mesosArgs)
        userData = template.format(**templateArgs)
        return userData
