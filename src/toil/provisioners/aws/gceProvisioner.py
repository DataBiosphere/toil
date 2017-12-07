# Copyright (C) 2015 UCSC Computational Genomics Lab
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
from builtins import str
from builtins import map
from builtins import range

import sys
import pipes
import subprocess
import time
import threading

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from toil import applianceSelf
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.aws import *


logger = logging.getLogger(__name__)

gceUserData = """#cloud-config

write_files:
    - path: "/home/core/volumes.sh"
      permissions: "0777"
      owner: "root"
      content: |
        #!/bin/bash
        set -x
        ephemeral_count=0
        drives=""
        directories="toil mesos docker"
        for drive in /dev/xvd{{b..z}}; do
            echo checking for $drive
            if [ -b $drive ]; then
                echo found it
                ephemeral_count=$((ephemeral_count + 1 ))
                drives="$drives $drive"
                echo increased ephemeral count by one
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
        Author=cketchum@ucsc.edu
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
        Author=cketchum@ucsc.edu
        After=docker.service

        [Service]
        Restart=on-failure
        RestartSec=2
        ExecPre=-/usr/bin/docker rm toil_{role}
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
            {image} \
            {args}
    - name: "node-exporter.service"
      command: "start"
      content: |
        [Unit]
        Description=node-exporter container
        After=docker.service

        [Service]
        Restart=on-failure
        RestartSec=2
        ExecPre=-/usr/bin/docker rm node_exporter
        ExecStart=/usr/bin/docker run \
            -p 9100:9100 \
            -v /proc:/host/proc \
            -v /sys:/host/sys \
            -v /:/rootfs \
            --name node-exporter \
            --restart always \
            prom/node-exporter:0.12.0 \
            -collector.procfs /host/proc \
            -collector.sysfs /host/sys \
            -collector.filesystem.ignored-mount-points ^/(sys|proc|dev|host|etc)($|/)
"""
#ssh_authorized_keys:
#    - "ssh-rsa {sshKey}"
#"""

class GCEProvisioner(AbstractProvisioner):

    def __init__(self, config=None):
        """
        :param config: Optional config object from common.py
        :param batchSystem:
        """
        super(GCEProvisioner, self).__init__(config)
        if config:
            self._getLeader(clusterName, zone=zone)

            self.instanceMetaData = get_instance_metadata()
            self.clusterName = self._getClusterNameFromTags(self.instanceMetaData)
            self.leaderIP = self.instanceMetaData['local-ipv4']  # this is PRIVATE IP
            self.keyName = list(self.instanceMetaData['public-keys'].keys())[0]
            self.tags = self._getLeader(self.clusterName).tags
            self.masterPublicKey = self._setSSH()
            self.nodeStorage = config.nodeStorage
            spotBids = []
            self.nonPreemptableNodeTypes = []
            self.preemptableNodeTypes = []
            for nodeTypeStr in config.nodeTypes:
                nodeBidTuple = nodeTypeStr.split(":")
                if len(nodeBidTuple) == 2:
                    #This is a preemptable node type, with a spot bid
                    self.preemptableNodeTypes.append(nodeBidTuple[0])
                    spotBids.append(nodeBidTuple[1])
                else:
                    self.nonPreemptableNodeTypes.append(nodeTypeStr)
            self.preemptableNodeShapes = [self.getNodeShape(nodeType=nodeType, preemptable=True) for nodeType in self.preemptableNodeTypes]
            self.nonPreemptableNodeShapes = [self.getNodeShape(nodeType=nodeType, preemptable=False) for nodeType in self.nonPreemptableNodeTypes]

            self.nodeShapes = self.nonPreemptableNodeShapes + self.preemptableNodeShapes
            self.nodeTypes = self.nonPreemptableNodeTypes + self.preemptableNodeTypes
            self.spotBids = dict(zip(self.preemptableNodeTypes, spotBids))

        else:
            self.clusterName = None
            self.instanceMetaData = None
            self.leaderIP = None
            self.keyName = None
            self.tags = None
            self.masterPublicKey = None
            self.nodeStorage = None
        self.subnetID = None



    def launchCluster(self, leaderNodeType, leaderSpotBid, nodeTypes, preemptableNodeTypes, keyName,
            clusterName, numWorkers=0, numPreemptableWorkers=0, spotBids=None, userTags=None, zone=None, vpcSubnet=None, leaderStorage=50, nodeStorage=50):
        if self.config is None:
            self.nodeStorage = nodeStorage
        if userTags is None:
            userTags = {}

        driver = self._getDriver()

        # TODO
        # - profileARN: what is this for? access policies?
        # - security group: just for a cluster identifier?
        # - bdm (just for RAID?)
        # - make imsage type, size and zone user variables
        # - what if cluster already exists? Is this checked in ARN or security group creation?
        #       - test this with AWS
        #profileARN = self._getProfileARN(ctx)
        #leaderInstanceType = ec2_instance_types[leaderNodeType]
        self.masterPublicKey = 'AAAAB3NzaC1yc2Enoauthorizedkeyneeded'
        leaderData = dict(role='leader',
                          image=applianceSelf(),
                          entrypoint='mesos-master',
                          sshKey=self.masterPublicKey,
                          args=leaderArgs.format(name=clusterName))
        userData = gceUserData.format(**leaderData)
        metadata = {'items': [{'key': 'user-data', 'value': userData}]}

        #size = 'n1-standard-1'
        imageType = 'coreos-stable'
        sa_scopes = [{'scopes': ['compute', 'storage-full']}]
        zone = 'us-west1-a'

        # TODO: add leader tags (is this for identification and billing?)
        defaultTags = {'Name': clusterName, 'Owner': keyName}
        defaultTags.update(userTags)


        driver = self._getDriver()
        if False:
            leader = self._getLeader(clusterName, zone=zone)
        elif not leaderSpotBid:
            logger.info('Launching non-preemptable leader')
            # TODO:
            # - rootVolSize=leaderStorage
            # - ex_keyname=keyName (what for?)
            # - tags? - Are they labels in gce?
            # - kwargs
            #   - kwargs["subnet_id"] = vpcSubnet
            #   - 'key_name': keyName,
            leader = driver.create_node(clusterName, leaderNodeType, imageType,
                                    location=zone,
                                    ex_service_accounts=sa_scopes,
                                    ex_metadata=metadata,
                                    ex_subnetwork=vpcSubnet,
                                    ex_tags=defaultTags)
        else:
            logger.info('Launching preemptable leader')
            # force generator to evaluate
            list(create_spot_instances(ec2=ctx.ec2,
                                       price=leaderSpotBid,
                                       image_id=self._discoverAMI(ctx),
                                       spec=kwargs,
                                       num_instances=1))


        logger.info('... toil_leader is running')

        self._waitForNode(leader.public_ips[0], keyName, role='toil_leader')


        # TODO: get tags
        #self.tags = leader.tags

        # if we running launch cluster we need to save this data as it won't be generated
        # from the metadata. This data is needed to launch worker nodes.
        self.leaderIP = leader.private_ips[0]
        if spotBids:
            self.spotBids = dict(zip(preemptableNodeTypes, spotBids))
        self.clusterName = clusterName
        self.keyName = keyName
        #TODO: get subnetID - what is this used for?
        #self.subnetID = leader.subnet_id
        # assuming that if the leader was launched without a spotbid then all workers
        # will be non-preemptable
        workersCreated = 0
        for nodeType, workers in zip(nodeTypes, numWorkers):
            workersCreated += self.addNodes(nodeType=nodeType, numNodes=workers, preemptable=False)
        for nodeType, workers in zip(preemptableNodeTypes, numPreemptableWorkers):
            workersCreated += self.addNodes(nodeType=nodeType, numNodes=workers, preemptable=True)
        logger.info('Added %d workers', workersCreated)

        return leader

    def getNodeShape(self, nodeType, preemptable=False):
        instanceType = ec2_instance_types[nodeType]

        disk = instanceType.disks * instanceType.disk_capacity * 2 ** 30
        if disk == 0:
            # This is an EBS-backed instance. We will use the root
            # volume, so add the amount of EBS storage requested for
            # the root volume
            disk = self.nodeStorage * 2 ** 30

        #Underestimate memory by 100M to prevent autoscaler from disagreeing with
        #mesos about whether a job can run on a particular node type
        memory = (instanceType.memory - 0.1) * 2** 30
        return Shape(wallTime=60 * 60,
                     memory=memory,
                     cores=instanceType.cores,
                     disk=disk,
                     preemptable=preemptable)

    @staticmethod
    def retryPredicate(e):
        return GCEProvisioner._throttlePredicate(e)

    @classmethod
    def destroyCluster(cls, clusterName, zone=None):
        def expectedShutdownErrors(e):
            return e.status == 400 and 'dependent object' in e.body

        instances = cls._getNodesInCluster(clusterName, nodeType=None, both=True)

        #TODO: anything for this
        #spotIDs = cls._getSpotRequestIDs(ctx, clusterName)
        #if spotIDs:
        #    ctx.ec2.cancel_spot_instance_requests(request_ids=spotIDs)

        #TODO: this works with env TOIL_AWS_NODE_DEBUG
        #instances = awsFilterImpairedNodes(instances, ctx.ec2)
        instancesToTerminate = instances
        vpcId = None
        if instancesToTerminate:
            #vpcId = instancesToTerminate[0].vpc_id
           # cls._deleteIAMProfiles(instances=instancesToTerminate, ctx=ctx)
            cls._terminateInstances(instances=instancesToTerminate)
        return
        # TODO: any cleanup
        if len(instances) == len(instancesToTerminate):
            logger.info('Deleting security group...')
            removed = False
            for attempt in retry(timeout=300, predicate=expectedShutdownErrors):
                with attempt:
                    for sg in ctx.ec2.get_all_security_groups():
                        if sg.name == clusterName and vpcId and sg.vpc_id == vpcId:
                            try:
                                ctx.ec2.delete_security_group(group_id=sg.id)
                                removed = True
                            except BotoServerError as e:
                                if e.error_code == 'InvalidGroup.NotFound':
                                    pass
                                else:
                                    raise
            if removed:
                logger.info('... Succesfully deleted security group')
        else:
            assert len(instances) > len(instancesToTerminate)
            # the security group can't be deleted until all nodes are terminated
            logger.warning('The TOIL_AWS_NODE_DEBUG environment variable is set and some nodes '
                           'have failed health checks. As a result, the security group & IAM '
                           'roles will not be deleted.')

    @classmethod
    def sshLeader(cls, clusterName, args=None, zone=None, **kwargs):
        leader = cls._getLeader(clusterName, zone=zone)
        logger.info('SSH ready')
        kwargs['tty'] = sys.stdin.isatty()
        command = args if args else ['bash']
        cls._sshAppliance(leader.public_ips[0], *command, **kwargs)

    @classmethod
    def rsyncLeader(cls, clusterName, args, zone=None, **kwargs):
        leader = cls._getLeader(clusterName, zone=zone)
        cls._rsyncNode(leader.public_ips[0], args, **kwargs)

    def remainingBillingInterval(self, node):
        #TODO - does this exist in GCE?
        return awsRemainingBillingInterval(node)

    def terminateNodes(self, nodes):
        #TODO This takes a set of node ids as input, which might not be compatible with _terminateNodes
        self._terminateNodes(nodes, self.ctx)

    def addNodes(self, nodeType, numNodes, preemptable):
        #instanceType = ec2_instance_types[nodeType]
        #bdm = self._getBlockDeviceMapping(instanceType, rootVolSize=self.nodeStorage)
        #arn = self._getProfileARN(self.ctx)
        keyPath = '' if not self.config or not self.config.sseKey else self.config.sseKey
        entryPoint = 'mesos-slave' if not self.config or not self.config.sseKey else "waitForKey.sh"
        workerData = dict(role='worker',
                          image=applianceSelf(),
                          entrypoint=entryPoint,
                          sshKey=self.masterPublicKey,
                          args=workerArgs.format(ip=self.leaderIP, preemptable=preemptable, keyPath=keyPath))
        userData = awsUserData.format(**workerData)
        #sgs = [sg for sg in self.ctx.ec2.get_all_security_groups() if sg.name == self.clusterName]
        #kwargs = {'key_name': self.keyName,
        #          'security_group_ids': [sg.id for sg in sgs],
        #          'instance_type': instanceType.name,
        #          'user_data': userData,
        #          'block_device_map': bdm,
        #          'instance_profile_arn': arn,
        #          'placement': getCurrentAWSZone()}
        #kwargs["subnet_id"] = self.subnetID if self.subnetID else self._getClusterInstance(self.instanceMetaData).subnet_id

        userData = gceUserData.format(**workerData)
        metadata = {'items': [{'key': 'user-data', 'value': userData}]}

        imageType = 'coreos-stable'
        sa_scopes = [{'scopes': ['compute', 'storage-full']}]
        zone = 'us-west1-a'

        driver = self._getDriver()
        if not preemptable:
            logger.info('Launching %s non-preemptable nodes', numNodes)
            instancesLaunched = driver.ex_create_multiple_nodes(self.clusterName, nodeType, imageType, numNodes,
                                    location=zone,
                                    ex_service_accounts=sa_scopes,
                                    ex_metadata=metadata)
                                    #ex_tags=defaultTags)
        else:
            logger.info('Launching %s preemptable nodes', numNodes)
            kwargs['placement'] = getSpotZone(self.spotBids[nodeType], instanceType.name, self.ctx)
            # force generator to evaluate
            instancesLaunched = list(create_spot_instances(ec2=self.ctx.ec2,
                                                           price=self.spotBids[nodeType],
                                                           image_id=self._discoverAMI(self.ctx),
                                                           tags={'clusterName': self.clusterName},
                                                           spec=kwargs,
                                                           num_instances=numNodes,
                                                           tentative=True)
                                     )
            # flatten the list
            instancesLaunched = [item for sublist in instancesLaunched for item in sublist]

        #for attempt in retry(predicate=LibCloudProvisioner._throttlePredicate):
        #    with attempt:
        #        wait_instances_running(self.ctx.ec2, instancesLaunched)

        for instance in instancesLaunched:
            self._waitForNode(instance.public_ips[0], self.keyName, role='toil_worker')

        # request throttling retry happens internally to these two methods to insure proper granularity
        #LibCloudProvisioner._addTags(instancesLaunched, self.tags)
        self._propagateKey(instancesLaunched)

        logger.info('Launched %s new instance(s)', numNodes)
        return len(instancesLaunched)

    def getProvisionedWorkers(self, nodeType, preemptable):
        entireCluster = self._getNodesInCluster(ctx=self.ctx, clusterName=self.clusterName, both=True, nodeType=nodeType)
        logger.debug('All nodes in cluster: %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self.leaderIP]
        logger.debug('All workers found in cluster: %s', workerInstances)
        workerInstances = [i for i in workerInstances if preemptable != (i.spot_instance_request_id is None)]
        logger.debug('%spreemptable workers found in cluster: %s', 'non-' if not preemptable else '', workerInstances)
        workerInstances = awsFilterImpairedNodes(workerInstances, self.ctx.ec2)
        return [Node(publicIP=i.ip_address, privateIP=i.private_ip_address,
                     name=i.id, launchTime=i.launch_time, nodeType=i.instance_type,
                     preemptable=preemptable)
                for i in workerInstances]


    @classmethod
    def _getLeader(cls, clusterName, zone=None):
        instances = cls._getNodesInCluster(clusterName, nodeType=None, both=True)
        #for x in instances:
            #print x
            #dir(x)
            #print dir(x)
            #print x.extra

        instances.sort(key=lambda x: x.created_at)
        try:
            leader = instances[0]  # assume leader was launched first
        except IndexError:
            raise NoSuchClusterException(clusterName)
        return leader

    @classmethod
    def _getNodesInCluster(cls, clusterName, nodeType=None, preemptable=False, both=False):

        driver = cls._getDriver()

        #TODO:
        # - filter by clusterName
        # - finish this
        return  driver.list_nodes() #zone=zone)

        pendingInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                                        'instance-state-name': 'pending'})
        runningInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                                       'instance-state-name': 'running'})
        if nodeType:
            pendingInstances = [instance for instance in pendingInstances if instance.instance_type == nodeType]
            runningInstances = [instance for instance in runningInstances if instance.instance_type == nodeType]
        instances = set(pendingInstances)
        if not preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is None]
        elif preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is not None]
        elif both:
            return [x for x in instances.union(set(runningInstances))]

    @classmethod
    def _getDriver(cls):
        GCE_JSON = "/Users/ejacox/toil-dev-41fd0135b44d.json"
        GCE_CLIENT_EMAIL = "100104111990-compute@developer.gserviceaccount.com"
        driverCls = get_driver(Provider.GCE)
        return driverCls(GCE_CLIENT_EMAIL, GCE_JSON, project='toil-dev', datacenter='us-west1-a')

    @classmethod
    def _waitForNode(cls, instanceIP, keyName, role):
        # returns the node's IP
        #cls._waitForSSHPort(instanceIP)

        cls._waitForSSHKeys(instanceIP, keyName=keyName)

        # TODO: this should be in a separate function (_waitForNode() is just for waiting)
        # copy keys to core user
        # - normal mechanism failed unless public key was in the google-ssh format
        # - even so, the key wasn't copied correctly to the core account
        keyFile = '/home/%s/.ssh/authorized_keys' % keyName
        cls._sshInstance(instanceIP, '/usr/bin/sudo', '/usr/bin/cp', keyFile, '/home/core/.ssh', user=keyName)
        cls._sshInstance(instanceIP, '/usr/bin/sudo', '/usr/bin/chown', 'core', '/home/core/.ssh/authorized_keys', user=keyName)

        # wait here so docker commands can be used reliably afterwards
        cls._waitForDockerDaemon(instanceIP)
        cls._waitForAppliance(instanceIP, role=role)

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
        #Forward port 3000 for grafana dashboard
        commandTokens.extend(['-L', '3000:localhost:3000', '-L', '9090:localhost:9090'])
        if sshOptions:
            # add specified options to ssh command
            assert isinstance(sshOptions, list)
            commandTokens.extend(sshOptions)
        # specify host
        user = kwargs.pop('user', 'core')   # TODO: Is this needed?
        commandTokens.append('%s@%s' % (user,str(nodeIP)))
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
        logger.debug('Node %s: %s', nodeIP, ' '.join(args))
        args = list(map(pipes.quote, args))
        commandTokens += args
        logger.debug('Full command %s', ' '.join(commandTokens))
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
    def _terminateInstances(cls, instances):
        def worker(driver, instance):
            logger.info('Terminating instance: %s', instance.name)
            driver.destroy_node(instance)

        driver = cls._getDriver()
        threads = []
        for instance in instances:
            t = threading.Thread(target=worker, args=(driver,instance))
            threads.append(t)
            t.start()

        logger.info('... Waiting for instance(s) to shut down...')
        for t in threads:
            t.join()



    def _propagateKey(self, instances):
        if not self.config or not self.config.sseKey:
            return
        for node in instances:
            # since we're going to be rsyncing into the appliance we need the appliance to be running first
            ipAddress = self._waitForNode(node, 'toil_worker')
            self._rsyncNode(ipAddress, [self.config.sseKey, ':' + self.config.sseKey], applianceName='toil_worker')



## UNCHANGED CLASSES

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
    def _waitForSSHPort(cls, ip_address):
        """
        Wait until the instance represented by this box is accessible via SSH.

        :return: the number of unsuccessful attempts to connect to the port before a the first
        success
        """
        logger.info('Waiting for ssh port to open...')
        i=0
        while True:
            i += 1
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.settimeout(a_short_time)
                s.connect((ip_address, 22))
                logger.info('...ssh port open')
                return i
            except socket.error:
                pass
            finally:
                s.close()

    @classmethod
    def _waitForSSHKeys(cls, instanceIP, keyName='core'):
        # the propagation of public ssh keys vs. opening the SSH port is racey, so this method blocks until
        # the keys are propagated and the instance can be SSH into
        while True:
            try:
                logger.info('Attempting to establish SSH connection...')
                cls._sshInstance(instanceIP, 'ps', sshOptions=['-oBatchMode=yes'], user=keyName)
            except RuntimeError:
                logger.info('Connection rejected, waiting for public SSH key to be propagated. Trying again in 10s.')
                time.sleep(10)
            else:
                logger.info('...SSH connection established.')
                # ssh succeeded
                return

    @classmethod
    def _waitForDockerDaemon(cls, ip_address):
        logger.info('Waiting for docker on %s to start...', ip_address)
        while True:
            output = cls._sshInstance(ip_address, '/usr/bin/ps', 'aux')
            time.sleep(5)
            if 'dockerd' in output:
                # docker daemon has started
                break
            else:
                logger.info('... Still waiting...')
        logger.info('Docker daemon running')

    @classmethod
    def _waitForAppliance(cls, ip_address, role):
        logger.info('Waiting for %s Toil appliance to start...', role)
        while True:
            output = cls._sshInstance(ip_address, '/usr/bin/docker', 'ps')
            if role in output:
                logger.info('...Toil appliance started')
                break
            else:
                logger.info('...Still waiting, trying again in 10sec...')
                time.sleep(10)

    @classmethod
    def _rsyncNode(cls, ip, args, applianceName='toil_leader', **kwargs):
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
        logger.debug("Running %r.", command + parsedArgs)

        return subprocess.check_call(command + parsedArgs)

