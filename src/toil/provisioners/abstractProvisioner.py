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
import configparser
import logging
import os.path
import subprocess
import tempfile
import textwrap
import yaml
from abc import ABC, abstractmethod
from functools import total_ordering
from typing import List, Dict, Tuple, Optional, Set

from toil import applianceSelf, customDockerInitCmd, customInitCmd
from toil.provisioners import ClusterTypeNotSupportedException
from toil.provisioners.node import Node

a_short_time = 5
logger = logging.getLogger(__name__)


class ManagedNodesNotSupportedException(RuntimeError):
    """
    Raised when attempting to add managed nodes (which autoscale up and down by
    themselves, without the provisioner doing the work) to a provisioner that
    does not support them.

    Polling with this and try/except is the Right Way to check if managed nodes
    are available from a provisioner.
    """
    pass


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


class AbstractProvisioner(ABC):
    """
    An abstract base class to represent the interface for provisioning worker nodes to use in a
    Toil cluster.
    """
    LEADER_HOME_DIR = '/root/'  # home directory in the Toil appliance on an instance

    def __init__(self, clusterName=None, clusterType='mesos', zone=None, nodeStorage=50, nodeStorageOverrides=None):
        """
        Initialize provisioner.

        Implementations should raise ClusterTypeNotSupportedException if
        presented with an unimplemented clusterType.

        :param clusterName: The cluster identifier.
        :param clusterType: The kind of cluster to make; 'mesos' or 'kubernetes'.
        :param zone: The zone the cluster runs in.
        :param nodeStorage: The amount of storage on the worker instances, in gigabytes.
        """
        self.clusterName = clusterName
        self.clusterType = clusterType

        if self.clusterType not in self.supportedClusterTypes():
            # This isn't actually a cluster type we can do
            raise ClusterTypeNotSupportedException(type(self), clusterType)

        self._zone = zone
        self._nodeStorage = nodeStorage
        self._nodeStorageOverrides = {}
        for override in nodeStorageOverrides or []:
            nodeShape, storageOverride = override.split(':')
            self._nodeStorageOverrides[nodeShape] = int(storageOverride)
        self._leaderPrivateIP = None
        # This will hold an SSH public key for Mesos clusters, or the
        # Kubernetes joining information as a dict for Kubernetes clusters.
        self._leaderWorkerAuthentication = None

        if clusterName:
            # Making a new cluster
            self.createClusterSettings()
        else:
            # Starting up on an existing cluster
            self.readClusterSettings()

    @abstractmethod
    def supportedClusterTypes(self) -> Set[str]:
        """
        Get all the cluster types that this provisioner implementation
        supports.
        """
        raise NotImplementedError

    @abstractmethod
    def createClusterSettings(self):
        """
        Initialize class for a new cluster, to be deployed, when running
        outside the cloud.
        """
        raise NotImplementedError

    @abstractmethod
    def readClusterSettings(self):
        """
        Initialize class from an existing cluster. This method assumes that
        the instance we are running on is the leader.

        Implementations must call _setLeaderWorkerAuthentication().
        """
        raise NotImplementedError

    def _write_file_to_cloud(self, key: str, contents: bytes) -> str:
        """
        Write a file to a physical storage system that is accessible to the
        leader and all nodes during the life of the cluster. Additional
        resources should be cleaned up in `self.destroyCluster()`.

        :return: A public URL that can be used to retrieve the file.
        """
        raise NotImplementedError

    def _read_file_from_cloud(self, key: str) -> bytes:
        """
        Return the contents of the file written by `self._write_file_to_cloud()`.
        """
        raise NotImplementedError

    def _get_user_data_limit(self) -> int:
        """
        Get the maximum number of bytes that can be passed as the user data
        during node creation.
        """
        raise NotImplementedError

    def _setLeaderWorkerAuthentication(self, leader: Node = None):
        """
        Configure authentication between the leader and the workers.

        Assumes that we are running on the leader, unless a Node is given, in
        which case credentials will be pulled from or created there.

        Configures the backing cluster scheduler so that the leader and workers
        will be able to communicate securely. Authentication may be one-way or
        mutual.

        Until this is called, new nodes may not be able to communicate with the
        leader. Afterward, the provisioner will include the necessary
        authentication information when provisioning nodes.

        :param leader: Node to pull credentials from, if not the current machine.
        """

        if self.clusterType == 'mesos':
            # We're using a Mesos cluster, so set up SSH from leader to workers.
            self._leaderWorkerAuthentication = self._setSSH(leader=leader)
        elif self.clusterType == 'kubernetes':
            # We're using a Kubernetes cluster.
            self._leaderWorkerAuthentication = self._getKubernetesJoiningInfo(leader=leader)

    def _clearLeaderWorkerAuthentication(self):
        """
        Forget any authentication information populated by
        _setLeaderWorkerAuthentication(). It will need to be called again to
        provision more workers.
        """

        self._leaderWorkerAuthentication = None

    def _setSSH(self, leader: Node = None) -> str:
        """
        Generate a key pair, save it in /root/.ssh/id_rsa.pub on the leader,
        and return the public key. The file /root/.sshSuccess is used to
        prevent this operation from running twice.

        Also starts the ssh agent on the local node, if operating on the local
        node.

        :param leader: Node to operate on, if not the current machine.

        :return: Public key, without the "ssh-rsa" part.
        """

        # To work locally or remotely we need to do all our setup work as one
        # big bash -c
        command = ['bash', '-c', ('set -e; if [ ! -e /root/.sshSuccess ] ; '
                                  'then ssh-keygen -f /root/.ssh/id_rsa -t rsa -N ""; '
                                  'touch /root/.sshSuccess; fi; chmod 700 /root/.ssh;')]

        if leader is None:
            # Run locally
            subprocess.check_call(command)

            # Grab from local file
            with open('/root/.ssh/id_rsa.pub') as f:
                leaderPublicKey = f.read()
        else:
            # Run remotely
            leader.sshInstance(*command, appliance=True)

            # Grab from remote file
            with tempfile.TemporaryDirectory() as tmpdir:
                localFile = os.path.join(tmpdir, 'id_rsa.pub')
                leader.extractFile('/root/.ssh/id_rsa.pub', localFile, 'toil_leader')

                with open(localFile) as f:
                    leaderPublicKey = f.read()

        # Drop the key type and keep just the key data
        leaderPublicKey = leaderPublicKey.split(' ')[1]

        # confirm it really is an RSA public key
        assert leaderPublicKey.startswith('AAAAB3NzaC1yc2E'), leaderPublicKey
        return leaderPublicKey

    def _getKubernetesJoiningInfo(self, leader: Node = None) -> Dict[str, str]:
        """
        Get the Kubernetes joining info created when Kubernetes was set up on
        this node, which is the leader, or on a different specified Node.

        Returns a dict of JOIN_TOKEN, JOIN_CERT_HASH, and JOIN_ENDPOINT, which
        can be inserted into our Kubernetes worker setup script and config.

        :param leader: Node to operate on, if not the current machine.
        """

        # Make a parser for the config
        config = configparser.ConfigParser(interpolation=None)
        # Leave case alone
        config.optionxform = str

        if leader is None:
            # This info is always supposed to be set up before the Toil appliance
            # starts, and mounted in at the same path as on the host. So we just go
            # read it.
            with open('/etc/kubernetes/worker.ini') as f:
                config.read_file(f)
        else:
            # Grab from remote file
            with tempfile.TemporaryDirectory() as tmpdir:
                localFile = os.path.join(tmpdir, 'worker.ini')
                leader.extractFile('/etc/kubernetes/worker.ini', localFile, 'toil_leader')

                with open(localFile) as f:
                    config.read_file(f)

        # Grab everything out of the default section where our setup script put
        # it.
        return dict(config['DEFAULT'])

    def setAutoscaledNodeTypes(self, nodeTypes: List[Tuple[Set[str], Optional[float]]]):
        """
        Set node types, shapes and spot bids for Toil-managed autoscaling.
        :param nodeTypes: A list of node types, as parsed with parse_node_types.
        """
        # This maps from an equivalence class of instance names to a spot bid.
        self._spotBidsMap = {}

        # This maps from a node Shape object to the instance type that has that
        # shape. TODO: what if multiple instance types in a cloud provider have
        # the same shape (e.g. AMD and Intel instances)???
        self._shape_to_instance_type = {}

        for node_type in nodeTypes:
            preemptable = node_type[1] is not None
            if preemptable:
                # Record the spot bid for the whole equivalence class
                self._spotBidsMap[frozenset(node_type[0])] = node_type[1]
            for instance_type_name in node_type[0]:
                # Record the instance shape and associated type.
                shape = self.getNodeShape(instance_type_name, preemptable)
                self._shape_to_instance_type[shape] = instance_type_name

    def hasAutoscaledNodeTypes(self) -> bool:
        """
        Check if node types have been configured on the provisioner (via
        setAutoscaledNodeTypes).

        :returns: True if node types are configured for autoscaling, and false
                  otherwise.
        """
        return len(self.getAutoscaledInstanceShapes()) > 0

    def getAutoscaledInstanceShapes(self) -> Dict[Shape, str]:
        """
        Get all the node shapes and their named instance types that the Toil
        autoscaler should manage.
        """

        if hasattr(self, '_shape_to_instance_type'):
            # We have had Toil-managed autoscaling set up
            return dict(self._shape_to_instance_type)
        else:
            # Nobody has called setAutoscaledNodeTypes yet, so nothing is to be autoscaled.
            return {}

    @staticmethod
    def retryPredicate(e):
        """
        Return true if the exception e should be retried by the cluster scaler.
        For example, should return true if the exception was due to exceeding an API rate limit.
        The error will be retried with exponential backoff.

        :param e: exception raised during execution of setNodeCount
        :return: boolean indicating whether the exception e should be retried
        """
        return False

    @abstractmethod
    def launchCluster(self, *args, **kwargs):
        """
        Initialize a cluster and create a leader node.

        Implementations must call _setLeaderWorkerAuthentication() with the
        leader so that workers can be launched.

        :param leaderNodeType: The leader instance.
        :param leaderStorage: The amount of disk to allocate to the leader in gigabytes.
        :param owner: Tag identifying the owner of the instances.

        """
        raise NotImplementedError

    @abstractmethod
    def addNodes(self, nodeTypes: Set[str], numNodes, preemptable, spotBid=None):
        """
        Used to add worker nodes to the cluster

        :param numNodes: The number of nodes to add
        :param preemptable: whether or not the nodes will be preemptable
        :param spotBid: The bid for preemptable nodes if applicable (this can be set in config, also).
        :return: number of nodes successfully added
        """
        raise NotImplementedError

    def addManagedNodes(self, nodeTypes: Set[str], minNodes, maxNodes, preemptable, spotBid=None) -> None:
        """
        Add a group of managed nodes of the given type, up to the given maximum.
        The nodes will automatically be launched and termianted depending on cluster load.

        Raises ManagedNodesNotSupportedException if the provisioner
        implementation or cluster configuration can't have managed nodes.

        :param minNodes: The minimum number of nodes to scale to
        :param maxNodes: The maximum number of nodes to scale to
        :param preemptable: whether or not the nodes will be preemptable
        :param spotBid: The bid for preemptable nodes if applicable (this can be set in config, also).
        """

        # Not available by default
        raise ManagedNodesNotSupportedException("Managed nodes not supported by this provisioner")

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
    def getProvisionedWorkers(self, instance_type: Optional[str] = None, preemptable: Optional[bool] = None):
        """
        Gets all nodes, optionally of the given instance type or
        preemptability, from the provisioner. Includes both static and
        autoscaled nodes.

        :param preemptable: Boolean value to restrict to preemptable
               nodes or non-preemptable nodes
        :return: list of Node objects
        """
        raise NotImplementedError

    @abstractmethod
    def getNodeShape(self, instance_type: str, preemptable=False):
        """
        The shape of a preemptable or non-preemptable node managed by this provisioner. The node
        shape defines key properties of a machine, such as its number of cores or the time
        between billing intervals.

        :param str instance_type: Instance type name to return the shape of.

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

    class InstanceConfiguration:
        """
        Allows defining the initial setup for an instance and then turning it
        into a CloudConfig configuration for instance user data.
        """

        def __init__(self):
            # Holds dicts with keys 'path', 'owner', 'permissions', and 'content' for files to create.
            # Permissions is a string octal number with leading 0.
            self.files = []
            # Holds dicts with keys 'name', 'command', and 'content' defining Systemd units to create
            self.units = []
            # Holds strings like "ssh-rsa actualKeyData" for keys to authorize (independently of cloud provider's system)
            self.sshPublicKeys = []

        def addFile(self, path: str, owner: str = 'root', permissions: str = '0755', content: str = ''):
            """
            Make a file on the instance with the given owner, permissions, and content.
            """

            self.files.append({'path': path, 'owner': owner, 'permissions': permissions, 'content': content})

        def addUnit(self, name: str, command: str = 'start', enable: bool = True, content: str = ''):
            """
            Make a systemd unit on the instance with the given name (including
            .service), and content, and apply the given command to it (default:
            'start'). Units will be enabled by default.

            Unit logs can be investigated with:
                systemctl status whatever.service
            or:
                journalctl -xe
            """

            self.units.append({'name': name, 'command': command, 'enable': enable, 'content': content})

        def addSSHRSAKey(self, keyData: str):
            """
            Authorize the given bare, encoded RSA key (without "ssh-rsa").
            """

            self.sshPublicKeys.append("ssh-rsa " + keyData)


        def toCloudConfig(self) -> str:
            """
            Return a CloudConfig configuration describing the desired config.
            """

            # Define the base config
            config = {
                'write_files': self.files,
                'coreos': {
                    'update': {
                        'reboot-strategy': 'off'
                    },
                    'units': self.units
                }
            }

            if len(self.sshPublicKeys) > 0:
                # Add SSH keys if needed
                config['ssh_authorized_keys'] = self.sshPublicKeys

            # Mark as CloudConfig and serialize as YAML
            return '#cloud-config\n\n' + yaml.dump(config)

    def getBaseInstanceConfiguration(self) -> InstanceConfiguration:
        """
        Get the base configuration for both leader and worker instances for all cluster types.
        """

        config = self.InstanceConfiguration()

        # First we have volume mounting. That always happens.
        self.addVolumesService(config)
        # We also always add the service to talk to Prometheus
        self.addNodeExporterService(config)

        return config

    def addVolumesService(self, config: InstanceConfiguration):
        """
        Add a service to prepare and mount local scratch volumes.
        """
        config.addFile("/home/core/volumes.sh", content=textwrap.dedent("""\
            #!/bin/bash
            set -x
            ephemeral_count=0
            drives=()
            directories=(toil mesos docker kubelet cwl)
            for drive in /dev/xvd{a..z} /dev/nvme{0..26}n1; do
                echo "checking for ${drive}"
                if [ -b $drive ]; then
                    echo "found it"
                    while [ "$(readlink -f "${drive}")" != "${drive}" ] ; do
                        drive="$(readlink -f "${drive}")"
                        echo "was a symlink to ${drive}"
                    done
                    seen=0
                    for other_drive in "${drives[@]}" ; do
                        if [ "${other_drive}" == "${drive}" ] ; then
                            seen=1
                            break
                        fi
                    done
                    if (( "${seen}" == "1" )) ; then
                        echo "already discovered via another name"
                        continue
                    fi
                    if mount | grep "^${drive}"; then
                        echo "already mounted, likely a root device"
                    else
                        ephemeral_count=$((ephemeral_count + 1 ))
                        drives+=("${drive}")
                        echo "increased ephemeral count by one"
                    fi
                fi
            done
            if (("$ephemeral_count" == "0" )); then
                echo "no ephemeral drive"
                for directory in "${directories[@]}"; do
                    sudo mkdir -p /var/lib/$directory
                done
                exit 0
            fi
            sudo mkdir /mnt/ephemeral
            if (("$ephemeral_count" == "1" )); then
                echo "one ephemeral drive to mount"
                sudo mkfs.ext4 -F "${drives[@]}"
                sudo mount "${drives[@]}" /mnt/ephemeral
            fi
            if (("$ephemeral_count" > "1" )); then
                echo "multiple drives"
                for drive in "${drives[@]}"; do
                    sudo dd if=/dev/zero of=$drive bs=4096 count=1024
                done
                # determine force flag
                sudo mdadm --create -f --verbose /dev/md0 --level=0 --raid-devices=$ephemeral_count "${drives[@]}"
                sudo mkfs.ext4 -F /dev/md0
                sudo mount /dev/md0 /mnt/ephemeral
            fi
            for directory in "${directories[@]}"; do
                sudo mkdir -p /mnt/ephemeral/var/lib/$directory
                sudo mkdir -p /var/lib/$directory
                sudo mount --bind /mnt/ephemeral/var/lib/$directory /var/lib/$directory
            done
            """))
        config.addUnit("volume-mounting.service", content=textwrap.dedent("""\
            [Unit]
            Description=mounts ephemeral volumes & bind mounts toil directories
            Before=docker.service

            [Service]
            Type=oneshot
            Restart=no
            ExecStart=/usr/bin/bash /home/core/volumes.sh
            """))

    def addNodeExporterService(self, config: InstanceConfiguration):
        """
        Add the node exporter service for Prometheus to an instance configuration.
        """

        config.addUnit("node-exporter.service", content=textwrap.dedent('''\
            [Unit]
            Description=node-exporter container
            After=docker.service

            [Service]
            Restart=on-failure
            RestartSec=2
            ExecStartPre=-/usr/bin/docker rm node_exporter
            ExecStart=/usr/bin/docker run \\
                -p 9100:9100 \\
                -v /proc:/host/proc \\
                -v /sys:/host/sys \\
                -v /:/rootfs \\
                --name node-exporter \\
                --restart always \\
                quay.io/prometheus/node-exporter:v0.15.2 \\
                --path.procfs /host/proc \\
                --path.sysfs /host/sys \\
                --collector.filesystem.ignored-mount-points ^/(sys|proc|dev|host|etc)($|/)
            '''))

    def addToilService(self, config: InstanceConfiguration, role: str, keyPath: str = None, preemptable: bool = False):
        """
        Add the Toil leader or worker service to an instance configuration.

        Will run Mesos master or agent as appropriate in Mesos clusters.
        For Kubernetes clusters, will just sleep to provide a place to shell
        into on the leader, and shouldn't run on the worker.

        :param role: Should be 'leader' or 'worker'. Will not work for 'worker' until leader credentials have been collected.
        :param keyPath: path on the node to a server-side encryption key that will be added to the node after it starts. The service will wait until the key is present before starting.
        :param preemptable: Whether a woeker should identify itself as preemptable or not to the scheduler.
        """

        # If keys are rsynced, then the mesos-agent needs to be started after the keys have been
        # transferred. The waitForKey.sh script loops on the new VM until it finds the keyPath file, then it starts the
        # mesos-agent. If there are multiple keys to be transferred, then the last one to be transferred must be
        # set to keyPath.
        MESOS_LOG_DIR = '--log_dir=/var/lib/mesos '
        LEADER_DOCKER_ARGS = '--registry=in_memory --cluster={name}'
        # --no-systemd_enable_support is necessary in Ubuntu 16.04 (otherwise,
        # Mesos attempts to contact systemd but can't find its run file)
        WORKER_DOCKER_ARGS = '--work_dir=/var/lib/mesos --master={ip}:5050 --attributes=preemptable:{preemptable} --no-hostname_lookup --no-systemd_enable_support'

        if self.clusterType == 'mesos':
            if role == 'leader':
                entryPoint = 'mesos-master'
                entryPointArgs = MESOS_LOG_DIR + LEADER_DOCKER_ARGS.format(name=self.clusterName)
            elif role == 'worker':
                entryPoint = 'mesos-agent'
                entryPointArgs = MESOS_LOG_DIR + WORKER_DOCKER_ARGS.format(ip=self._leaderPrivateIP,
                                                                           preemptable=preemptable)
            else:
                raise RuntimeError("Unknown role %s" % role)
        elif self.clusterType == 'kubernetes':
            if role == 'leader':
                # We need *an* entry point or the leader container will finish
                # and go away, and thus not be available to take user logins.
                entryPoint = 'sleep'
                entryPointArgs = 'infinity'
            else:
                raise RuntimeError('Toil service not needed for %s nodes in a %s cluster',
                                   role, self.clusterType)
        else:
            raise RuntimeError('Toil service not needed in a %s cluster', self.clusterType)

        if keyPath:
            entryPointArgs = keyPath + ' ' + entryPointArgs
            entryPoint = "waitForKey.sh"
        customDockerInitCommand = customDockerInitCmd()
        if customDockerInitCommand:
            entryPointArgs = " ".join(["'" + customDockerInitCommand + "'", entryPoint, entryPointArgs])
            entryPoint = "customDockerInit.sh"

        config.addUnit(f"toil-{role}.service", content=textwrap.dedent(f'''\
            [Unit]
            Description=toil-{role} container
            After=docker.service
            After=create-kubernetes-cluster.service

            [Service]
            Restart=on-failure
            RestartSec=2
            ExecStartPre=-/usr/bin/docker rm toil_{role}
            ExecStartPre=-/usr/bin/bash -c '{customInitCmd()}'
            ExecStart=/usr/bin/docker run \\
                --entrypoint={entryPoint} \\
                --net=host \\
                -v /var/run/docker.sock:/var/run/docker.sock \\
                -v /var/lib/mesos:/var/lib/mesos \\
                -v /var/lib/docker:/var/lib/docker \\
                -v /var/lib/toil:/var/lib/toil \\
                -v /var/lib/cwl:/var/lib/cwl \\
                -v /tmp:/tmp \\
                -v /opt:/opt \\
                -v /etc/kubernetes:/etc/kubernetes \\
                -v /etc/kubernetes/admin.conf:/root/.kube/config \\
                --name=toil_{role} \\
                {applianceSelf()} \\
                {entryPointArgs}
            '''))

    def getKubernetesValues(self):
        """
        Returns a dict of Kubernetes component versions and paths for formatting into Kubernetes-related templates.
        """
        return dict(
            CNI_VERSION="v0.8.2",
            CRICTL_VERSION="v1.17.0",
            CNI_DIR="/opt/cni/bin",
            DOWNLOAD_DIR="/opt/bin",
            # This is the version of Kubernetes to use
            # Get current from: curl -sSL https://dl.k8s.io/release/stable.txt
            # Make sure it is compatible with the kubelet.service unit we ship, or update that too.
            KUBERNETES_VERSION="v1.19.3",
            # Now we need the basic cluster services
            # Version of Flannel networking to get the YAML from
            FLANNEL_VERSION="v0.13.0",
            # Version of node CSR signign bot to run
            RUBBER_STAMP_VERSION="v0.3.1",
            # Version of the autoscaler to run
            AUTOSCALER_VERSION="1.19.0",
            # Version of metrics service to install for `kubectl top nodes`
            METRICS_API_VERSION="v0.3.7",
            CLUSTER_NAME=self.clusterName,
            # YAML line that tells the Kubelet to use a cloud provider, if we need one.
            CLOUD_PROVIDER_SPEC=('cloud-provider: ' + self.getKubernetesCloudProvider()) if self.getKubernetesCloudProvider() else ''
        )

    def addKubernetesServices(self, config: InstanceConfiguration):
        """
        Add installing Kubernetes and Kubeadm and setting up the Kubelet to run when configured to an instance configuration.
        The same process applies to leaders and workers.
        """

        values = self.getKubernetesValues()

        # We're going to ship the Kubelet service from Kubernetes' release pipeline via cloud-config
        config.addUnit("kubelet.service", content=textwrap.dedent('''\
            # This came from https://raw.githubusercontent.com/kubernetes/release/v0.4.0/cmd/kubepkg/templates/latest/deb/kubelet/lib/systemd/system/kubelet.service
            # It has been modified to replace /usr/bin with {DOWNLOAD_DIR}
            # License: https://raw.githubusercontent.com/kubernetes/release/v0.4.0/LICENSE

            [Unit]
            Description=kubelet: The Kubernetes Node Agent
            Documentation=https://kubernetes.io/docs/home/
            Wants=network-online.target
            After=network-online.target

            [Service]
            ExecStart={DOWNLOAD_DIR}/kubelet
            Restart=always
            StartLimitInterval=0
            RestartSec=10

            [Install]
            WantedBy=multi-user.target
            ''').format(**values))

        # It needs this config file
        config.addFile("/etc/systemd/system/kubelet.service.d/10-kubeadm.conf", permissions="0644", content=textwrap.dedent('''\
            # This came from https://raw.githubusercontent.com/kubernetes/release/v0.4.0/cmd/kubepkg/templates/latest/deb/kubeadm/10-kubeadm.conf
            # It has been modified to replace /usr/bin with {DOWNLOAD_DIR}
            # License: https://raw.githubusercontent.com/kubernetes/release/v0.4.0/LICENSE

            # Note: This dropin only works with kubeadm and kubelet v1.11+
            [Service]
            Environment="KUBELET_KUBECONFIG_ARGS=--bootstrap-kubeconfig=/etc/kubernetes/bootstrap-kubelet.conf --kubeconfig=/etc/kubernetes/kubelet.conf"
            Environment="KUBELET_CONFIG_ARGS=--config=/var/lib/kubelet/config.yaml"
            # This is a file that "kubeadm init" and "kubeadm join" generates at runtime, populating the KUBELET_KUBEADM_ARGS variable dynamically
            EnvironmentFile=-/var/lib/kubelet/kubeadm-flags.env
            # This is a file that the user can use for overrides of the kubelet args as a last resort. Preferably, the user should use
            # the .NodeRegistration.KubeletExtraArgs object in the configuration files instead. KUBELET_EXTRA_ARGS should be sourced from this file.
            EnvironmentFile=-/etc/default/kubelet
            ExecStart=
            ExecStart={DOWNLOAD_DIR}/kubelet $KUBELET_KUBECONFIG_ARGS $KUBELET_CONFIG_ARGS $KUBELET_KUBEADM_ARGS $KUBELET_EXTRA_ARGS
            ''').format(**values))

        # Before we let the kubelet try to start, we have to actually download it (and kubeadm)
        config.addFile("/home/core/install-kubernetes.sh", content=textwrap.dedent('''\
            #!/usr/bin/env bash
            set -e

            # Make sure we have Docker enabled; Kubeadm later might complain it isn't.
            systemctl enable docker.service

            mkdir -p {CNI_DIR}
            curl -L "https://github.com/containernetworking/plugins/releases/download/{CNI_VERSION}/cni-plugins-linux-amd64-{CNI_VERSION}.tgz" | tar -C {CNI_DIR} -xz
            mkdir -p {DOWNLOAD_DIR}
            curl -L "https://github.com/kubernetes-sigs/cri-tools/releases/download/{CRICTL_VERSION}/crictl-{CRICTL_VERSION}-linux-amd64.tar.gz" | tar -C {DOWNLOAD_DIR} -xz

            cd {DOWNLOAD_DIR}
            curl -L --remote-name-all https://storage.googleapis.com/kubernetes-release/release/{KUBERNETES_VERSION}/bin/linux/amd64/{{kubeadm,kubelet,kubectl}}
            chmod +x {{kubeadm,kubelet,kubectl}}
            ''').format(**values))
        config.addUnit("install-kubernetes.service", content=textwrap.dedent('''\
            [Unit]
            Description=base Kubernetes installation
            Wants=network-online.target
            After=network-online.target
            Before=kubelet.service

            [Service]
            Type=oneshot
            Restart=no
            ExecStart=/usr/bin/bash /home/core/install-kubernetes.sh
            '''))

        # Now we should have the kubeadm command, and the bootlooping kubelet
        # waiting for kubeadm to configure it.

    def getKubernetesAutoscalerSetupCommands(self, values: Dict[str, str]) -> str:
        """
        Return Bash commands that set up the Kubernetes cluster autoscaler for
        provisioning from the environment supported by this provisioner.

        Should only be implemented if Kubernetes clusters are supported.

        :param values: Contains definitions of cluster variables, like
                       AUTOSCALER_VERSION and CLUSTER_NAME.

        :returns: Bash snippet
        """
        raise NotImplementedError()

    def getKubernetesCloudProvider(self) -> Optional[str]:
        """
        Return the Kubernetes cloud provider (for example, 'aws'), to pass to
        the kubelets in a Kubernetes cluster provisioned using this provisioner.

        Defaults to None if not overridden, in which case no cloud provider
        integration will be used.

        :returns: Cloud provider name, or None
        """
        return None

    def addKubernetesLeader(self, config: InstanceConfiguration):
        """
        Add services to configure as a Kubernetes leader, if Kubernetes is already set to be installed.
        """

        values = self.getKubernetesValues()

        # Customize scheduler to pack jobs into as few nodes as possible
        # See: https://kubernetes.io/docs/reference/scheduling/config/#profiles
        config.addFile("/home/core/scheduler-config.yml", permissions="0644", content=textwrap.dedent('''\
            apiVersion: kubescheduler.config.k8s.io/v1beta1
            kind: KubeSchedulerConfiguration
            clientConnection:
              kubeconfig: /etc/kubernetes/scheduler.conf
            profiles:
              - schedulerName: default-scheduler
                plugins:
                  score:
                    disabled:
                    - name: NodeResourcesLeastAllocated
                    enabled:
                    - name: NodeResourcesMostAllocated
                      weight: 1
            '''.format(**values)))

        # Main kubeadm cluster configuration.
        # Make sure to mount the scheduler config where the scheduler can see
        # it, which is undocumented but inferred from
        # https://pkg.go.dev/k8s.io/kubernetes@v1.21.0/cmd/kubeadm/app/apis/kubeadm#ControlPlaneComponent
        config.addFile("/home/core/kubernetes-leader.yml", permissions="0644", content=textwrap.dedent('''\
            apiVersion: kubeadm.k8s.io/v1beta2
            kind: InitConfiguration
            nodeRegistration:
              kubeletExtraArgs:
                volume-plugin-dir: "/opt/libexec/kubernetes/kubelet-plugins/volume/exec/"
                {CLOUD_PROVIDER_SPEC}
            ---
            apiVersion: kubeadm.k8s.io/v1beta2
            kind: ClusterConfiguration
            controllerManager:
              extraArgs:
                flex-volume-plugin-dir: "/opt/libexec/kubernetes/kubelet-plugins/volume/exec/"
            scheduler:
              extraArgs:
                config: "/etc/kubernetes/scheduler-config.yml"
              extraVolumes:
                - name: schedulerconfig
                  hostPath: "/home/core/scheduler-config.yml"
                  mountPath: "/etc/kubernetes/scheduler-config.yml"
                  readOnly: true
                  pathType: "File"
            networking:
              serviceSubnet: "10.96.0.0/12"
              podSubnet: "10.244.0.0/16"
              dnsDomain: "cluster.local"
            ---
            apiVersion: kubelet.config.k8s.io/v1beta1
            kind: KubeletConfiguration
            serverTLSBootstrap: true
            rotateCertificates: true
            cgroupDriver: systemd
            '''.format(**values)))

        # Make a script to apply that and the other cluster components
        # Note that we're escaping {{thing}} as {{{{thing}}}} because we need to match mustaches in a yaml we hack up.
        config.addFile("/home/core/create-kubernetes-cluster.sh", content=textwrap.dedent('''\
            #!/usr/bin/env bash
            set -e

            export PATH="$PATH:{DOWNLOAD_DIR}"

            # We need the kubelet being restarted constantly by systemd while kubeadm is setting up.
            # Systemd doesn't really let us say that in the unit file.
            systemctl start kubelet

            kubeadm init --config /home/core/kubernetes-leader.yml

            mkdir -p $HOME/.kube
            cp /etc/kubernetes/admin.conf $HOME/.kube/config
            chown $(id -u):$(id -g) $HOME/.kube/config

            # Install network
            kubectl apply -f https://raw.githubusercontent.com/flannel-io/flannel/{FLANNEL_VERSION}/Documentation/kube-flannel.yml

            # Deploy rubber stamp CSR signing bot
            kubectl apply -f https://raw.githubusercontent.com/kontena/kubelet-rubber-stamp/release/{RUBBER_STAMP_VERSION}/deploy/service_account.yaml
            kubectl apply -f https://raw.githubusercontent.com/kontena/kubelet-rubber-stamp/release/{RUBBER_STAMP_VERSION}/deploy/role.yaml
            kubectl apply -f https://raw.githubusercontent.com/kontena/kubelet-rubber-stamp/release/{RUBBER_STAMP_VERSION}/deploy/role_binding.yaml
            kubectl apply -f https://raw.githubusercontent.com/kontena/kubelet-rubber-stamp/release/{RUBBER_STAMP_VERSION}/deploy/operator.yaml

            ''').format(**values) + self.getKubernetesAutoscalerSetupCommands(values) + textwrap.dedent('''\
            # Set up metrics server, which needs serverTLSBootstrap and rubber stamp, and insists on running on a worker
            curl -sSL https://github.com/kubernetes-sigs/metrics-server/releases/download/{METRICS_API_VERSION}/components.yaml | \\
                sed 's/          - --secure-port=4443/          - --secure-port=4443\\n          - --kubelet-preferred-address-types=Hostname/' | \\
                kubectl apply -f -

            # Grab some joining info and make a file we can parse later with configparser
            echo "[DEFAULT]" >/etc/kubernetes/worker.ini
            echo "JOIN_TOKEN=$(kubeadm token create --ttl 0)" >>/etc/kubernetes/worker.ini
            echo "JOIN_CERT_HASH=sha256:$(openssl x509 -pubkey -in /etc/kubernetes/pki/ca.crt | openssl rsa -pubin -outform der 2>/dev/null | openssl dgst -sha256 -hex | sed 's/^.* //')" >>/etc/kubernetes/worker.ini
            echo "JOIN_ENDPOINT=$(hostname):6443" >>/etc/kubernetes/worker.ini
            ''').format(**values))
        config.addUnit("create-kubernetes-cluster.service", content=textwrap.dedent('''\
            [Unit]
            Description=Kubernetes cluster bootstrap
            After=install-kubernetes.service
            After=docker.service
            Before=toil-leader.service
            # Can't be before kubelet.service because Kubelet has to come up as we run this.

            [Service]
            Type=oneshot
            Restart=no
            ExecStart=/usr/bin/bash /home/core/create-kubernetes-cluster.sh
            '''))

        # We also need a node cleaner service
        config.addFile("/home/core/cleanup-nodes.sh", content=textwrap.dedent('''\
            #!/usr/bin/env bash
            # cleanup-nodes.sh: constantly clean up NotReady nodes that are tainted as having been deleted
            set -e

            export PATH="$PATH:{DOWNLOAD_DIR}"

            while true ; do
                echo "$(date | tr -d '\\n'): Checking for scaled-in nodes..."
                for NODE_NAME in $(kubectl --kubeconfig /etc/kubernetes/admin.conf get nodes -o json | jq -r '.items[] | select(.spec.taints) | select(.spec.taints[] | select(.key == "ToBeDeletedByClusterAutoscaler")) | select(.spec.taints[] | select(.key == "node.kubernetes.io/unreachable")) | select(.status.conditions[] | select(.type == "Ready" and .status == "Unknown")) | .metadata.name' | tr '\\n' ' ') ; do
                    # For every node that's tainted as ToBeDeletedByClusterAutoscaler, and
                    # as node.kubernetes.io/unreachable, and hasn't dialed in recently (and
                    # is thus in readiness state Unknown)
                    echo "Node $NODE_NAME is supposed to be scaled away and also gone. Removing from cluster..."
                    # Drop it if possible
                    kubectl --kubeconfig /etc/kubernetes/admin.conf delete node "$NODE_NAME" || true
                done
                sleep 300
            done
            ''').format(**values))
        config.addUnit("cleanup-nodes.service", content=textwrap.dedent('''\
            [Unit]
            Description=Remove scaled-in nodes
            After=install-kubernetes.service
            [Service]
            ExecStart=/home/core/cleanup-nodes.sh
            Restart=always
            StartLimitInterval=0
            RestartSec=10
            [Install]
            WantedBy=multi-user.target
            '''))

    def addKubernetesWorker(self, config: InstanceConfiguration, authVars: Dict[str, str], preemptable: bool = False):
        """
        Add services to configure as a Kubernetes worker, if Kubernetes is
        already set to be installed.

        Authenticate back to the leader using the JOIN_TOKEN, JOIN_CERT_HASH,
        and JOIN_ENDPOINT set in the given authentication data dict.

        :param config: The configuration to add services to
        :param authVars: Dict with authentication info
        :param preemptable: Whether the worker should be labeled as preemptable or not
        """

        # Collect one combined set of auth and general settings.
        values = dict(**self.getKubernetesValues(), **authVars)

        # Mark the node as preemptable if it is.
        # TODO: We use the same label that EKS uses here, because nothing is standardized.
        # This won't be quite appropriate as we aren't on EKS and we might not
        # even be on AWS, but the batch system should understand it.
        values['WORKER_LABEL_SPEC'] = 'node-labels: "eks.amazonaws.com/capacityType=SPOT"' if preemptable else ''

        # Kubeadm worker configuration
        config.addFile("/home/core/kubernetes-worker.yml", permissions="0644", content=textwrap.dedent('''\
            apiVersion: kubeadm.k8s.io/v1beta2
            kind: JoinConfiguration
            nodeRegistration:
              kubeletExtraArgs:
                volume-plugin-dir: "/opt/libexec/kubernetes/kubelet-plugins/volume/exec/"
                {CLOUD_PROVIDER_SPEC}
                {WORKER_LABEL_SPEC}
            discovery:
              bootstrapToken:
                apiServerEndpoint: {JOIN_ENDPOINT}
                token: {JOIN_TOKEN}
                caCertHashes:
                - "{JOIN_CERT_HASH}"
            ---
            apiVersion: kubelet.config.k8s.io/v1beta1
            kind: KubeletConfiguration
            cgroupDriver: systemd
            '''.format(**values)))

        # Make a script to join the cluster using that configuration
        config.addFile("/home/core/join-kubernetes-cluster.sh", content=textwrap.dedent('''\
            #!/usr/bin/env bash
            set -e

            export PATH="$PATH:{DOWNLOAD_DIR}"

            # We need the kubelet being restarted constantly by systemd while kubeadm is setting up.
            # Systemd doesn't really let us say that in the unit file.
            systemctl start kubelet

            kubeadm join {JOIN_ENDPOINT} --config /home/core/kubernetes-worker.yml
            ''').format(**values))

        config.addUnit("join-kubernetes-cluster.service", content=textwrap.dedent('''\
            [Unit]
            Description=Kubernetes cluster membership
            After=install-kubernetes.service
            After=docker.service
            # Can't be before kubelet.service because Kubelet has to come up as we run this.

            [Service]
            Type=oneshot
            Restart=no
            ExecStart=/usr/bin/bash /home/core/join-kubernetes-cluster.sh
            '''))

    def _getCloudConfigUserData(self, role, keyPath=None, preemptable=False):
        """
        Return the text (not bytes) user data to pass to a provisioned node.

        If leader-worker authentication is currently stored, uses it to connect
        the worker to the leader.

        :param str keyPath: The path of a secret key for the worker to wait for the leader to create on it.
        :param bool preemptable: Set to true for a worker node to identify itself as preemptible in the cluster.
        """

        # Start with a base config
        config = self.getBaseInstanceConfiguration()

        if self.clusterType == 'kubernetes':
            # Install Kubernetes
            self.addKubernetesServices(config)

            if role == 'leader':
                # Set up the cluster
                self.addKubernetesLeader(config)

            # We can't actually set up a Kubernetes worker without credentials
            # to connect back to the leader.

        if self.clusterType == 'mesos' or role == 'leader':
            # Leaders, and all nodes in a Mesos cluster, need a Toil service
            self.addToilService(config, role, keyPath, preemptable)

        if role == 'worker' and self._leaderWorkerAuthentication is not None:
            # We need to connect the worker to the leader.
            if self.clusterType == 'mesos':
                # This involves an SSH public key form the leader
                config.addSSHRSAKey(self._leaderWorkerAuthentication)
            elif self.clusterType == 'kubernetes':
                # We can install the Kubernetes worker and make it phone home
                # to the leader.
                # TODO: this puts sufficient info to fake a malicious worker
                # into the worker config, which probably is accessible by
                # anyone in the cloud account.
                self.addKubernetesWorker(config, self._leaderWorkerAuthentication, preemptable=preemptable)

        # Make it into a string for CloudConfig
        return config.toCloudConfig()


