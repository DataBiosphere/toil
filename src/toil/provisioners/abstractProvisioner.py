# Copyright (C) 2015-2016 Regents of the University of California
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
from abc import ABCMeta, abstractmethod
from builtins import object
from collections import namedtuple
from itertools import count
import logging
import pipes
import socket
import subprocess

from future.utils import with_metaclass

from bd2k.util.retry import never
a_short_time = 5

log = logging.getLogger(__name__)


Shape = namedtuple("_Shape", "wallTime memory cores disk preemptable")
"""
Represents a job or a node's "shape", in terms of the dimensions of memory, cores, disk and
wall-time allocation.

The wallTime attribute stores the number of seconds of a node allocation, e.g. 3600 for AWS,
or 60 for Azure. FIXME: and for jobs?

The memory and disk attributes store the number of bytes required by a job (or provided by a
node) in RAM or on disk (SSD or HDD), respectively.
"""


class AbstractProvisioner(with_metaclass(ABCMeta, object)):
    """
    An abstract base class to represent the interface for provisioning worker nodes to use in a
    Toil cluster.
    """

    def __init__(self, config=None):
        """
        Initialize provisioner. If config and batchSystem are not specified, the
        provisioner is being used to manage nodes without a workflow

        :param config: Config from common.py
        :param batchSystem: The batchSystem used during run
        """
        self.config = config
        self.stop = False
        self.staticNodesDict = {}  # dict with keys of nodes private IPs, val is nodeInfo
        self.static = {}

    def getStaticNodes(self, preemptable):
        return self.static[preemptable]

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

    def setStaticNodes(self, nodes, preemptable):
        """
        Used to track statically provisioned nodes. These nodes are
        treated differently than autoscaled nodes in that they should not
        be automatically terminated.

        :param nodes: list of Node objects
        """
        prefix = 'non-' if not preemptable else ''
        log.debug("Adding %s to %spreemptable static nodes", nodes, prefix)
        if nodes is not None:
            self.static[preemptable] = {node.privateIP : node for node in nodes}

    @abstractmethod
    def addNodes(self, nodeType, numNodes, preemptable):
        """
        Used to add worker nodes to the cluster

        :param numNodes: The number of nodes to add
        :param preemptable: whether or not the nodes will be preemptable
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
    def remainingBillingInterval(self, node):
        """
        Calculate how much of a node's allocated billing interval is
        left in this cycle.

        :param node: Node object
        :return: float from 0 -> 1.0 representing percentage of pre-paid time left in cycle
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

    @classmethod
    @abstractmethod
    def rsyncLeader(cls, clusterName, args, **kwargs):
        """
        Rsyncs to the leader of the cluster with the specified name. The arguments are passed directly to
        Rsync.

        :param clusterName: name of the cluster to target
        :param args: list of string arguments to rsync. Identical to the normal arguments to rsync, but the
           host name of the remote host can be omitted. ex) ['/localfile', ':/remotedest']
        :param \**kwargs:
           See below

        :Keyword Arguments:
            * *strict*: if False, strict host key checking is disabled. (Enabled by default.)
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def launchCluster(cls, instanceType, keyName, clusterName, spotBid=None):
        """
        Launches a cluster with the specified instance type for the leader with the specified name.

        :param instanceType: desired type of the leader instance
        :param keyName: name of the ssh key pair to launch the instance with
        :param clusterName: desired identifier of the cluster
        :param spotBid: how much to bid for the leader instance. If none, use on demand pricing.
        :return:
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def sshLeader(cls, clusterName, args, **kwargs):
        """
        SSH into the leader instance of the specified cluster with the specified arguments to SSH.
        :param clusterName: name of the cluster to target
        :param args: list of string arguments to ssh.
        :param strict: If False, strict host key checking is disabled. (Enabled by default.)
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def destroyCluster(cls, clusterName):
        """
        Terminates all nodes in the specified cluster and cleans up all resources associated with the
        cluser.
        :param clusterName: identifier of the cluster to terminate.
        """
        raise NotImplementedError

    @classmethod
    def _waitForSSHPort(cls, ip_address):
        """
        Wait until the instance represented by this box is accessible via SSH.

        :return: the number of unsuccessful attempts to connect to the port before a the first
        success
        """
        log.info('Waiting for ssh port to open...')
        for i in count():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.settimeout(a_short_time)
                s.connect((ip_address, 22))
                log.info('...ssh port open')
                return i
            except socket.error:
                pass
            finally:
                s.close()

    @classmethod
    def _sshAppliance(cls, leaderIP, *args, **kwargs):
        """
        :param str leaderIP: IP of the master
        :param args: arguments to execute in the appliance
        :param kwargs: tty=bool tells docker whether or not to create a TTY shell for
            interactive SSHing. The default value is False. Input=string is passed as
            input to the Popen call.
        """
        kwargs['appliance'] = True
        return cls._coreSSH(leaderIP, *args, **kwargs)

    @classmethod
    def _sshInstance(cls, nodeIP, *args, **kwargs):
        # returns the output from the command
        kwargs['collectStdout'] = True
        return cls._coreSSH(nodeIP, *args, **kwargs)

    @classmethod
    def _coreSSH(cls, nodeIP, *args, **kwargs):
        """
        If strict=False, strict host key checking will be temporarily disabled.
        This is provided as a convenience for internal/automated functions and
        ought to be set to True whenever feasible, or whenever the user is directly
        interacting with a resource (e.g. rsync-cluster or ssh-cluster). Assumed
        to be False by default.

        kwargs: input, tty, appliance, collectStdout, sshOptions, strict
        """
        commandTokens = ['ssh', '-t']
        strict = kwargs.pop('strict', False)
        if not strict:
            kwargs['sshOptions'] = ['-oUserKnownHostsFile=/dev/null', '-oStrictHostKeyChecking=no'] + kwargs.get('sshOptions', [])
        sshOptions = kwargs.pop('sshOptions', None)
        # Forward port 3000 for grafana dashboard
        commandTokens.extend(['-L', '3000:localhost:3000', '-L', '9090:localhost:9090'])
        if sshOptions:
            # add specified options to ssh command
            assert isinstance(sshOptions, list)
            commandTokens.extend(sshOptions)
        # specify host
        commandTokens.append('core@%s' % nodeIP)

        appliance = kwargs.pop('appliance', None)
        if appliance:
            # run the args in the appliance
            tty = kwargs.pop('tty', None)
            ttyFlag = '-t' if tty else ''
            commandTokens += ['docker', 'exec', '-i', ttyFlag, 'toil_leader']

        inputString = kwargs.pop('input', None)
        if inputString is not None:
            kwargs['stdin'] = subprocess.PIPE

        collectStdout = kwargs.pop('collectStdout', None)
        if collectStdout:
            kwargs['stdout'] = subprocess.PIPE

        log.debug('Node %s: %s', nodeIP, ' '.join(args))
        args = list(map(pipes.quote, args))
        commandTokens += args
        log.debug('Full command %s', ' '.join(commandTokens))
        popen = subprocess.Popen(commandTokens, **kwargs)
        stdout, stderr = popen.communicate(input=inputString)
        # at this point the process has already exited, no need for a timeout
        resultValue = popen.wait()
        if resultValue != 0:
            raise RuntimeError('Executing the command "%s" on the appliance returned a non-zero '
                               'exit code %s with stdout %s and stderr %s' % (' '.join(args), resultValue, stdout, stderr))
        assert stderr is None
        return stdout

    @classmethod
    def _coreRsync(cls, ip, args, applianceName='toil_leader', **kwargs):
        remoteRsync = "docker exec -i %s rsync" % applianceName  # Access rsync inside appliance
        parsedArgs = []
        sshCommand = "ssh"
        strict = kwargs.pop('strict', False)
        if not strict:
            sshCommand = "ssh -oUserKnownHostsFile=/dev/null -oStrictHostKeyChecking=no"
        hostInserted = False
        # Insert remote host address
        for i in args:
            if i.startswith(":") and not hostInserted:
                i = ("core@%s" % ip) + i
                hostInserted = True
            elif i.startswith(":") and hostInserted:
                raise ValueError("Cannot rsync between two remote hosts")
            parsedArgs.append(i)
        if not hostInserted:
            raise ValueError("No remote host found in argument list")
        command = ['rsync', '-e', sshCommand, '--rsync-path', remoteRsync]
        log.debug("Running %r.", command + parsedArgs)

        return subprocess.check_call(command + parsedArgs)
