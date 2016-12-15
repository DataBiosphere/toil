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
from __future__ import absolute_import
import datetime
import logging
import os

from bd2k.util import parse_iso_utc, less_strict_bool


logger = logging.getLogger(__name__)


def awsRemainingBillingInterval(instance):
    def partialBillingInterval(instance):
        """
        Returns a floating point value between 0 and 1.0 representing how far we are into the
        current billing cycle for the given instance. If the return value is .25, we are one
        quarter into the billing cycle, with three quarters remaining before we will be charged
        again for that instance.
        """
        launch_time = parse_iso_utc(instance.launch_time)
        now = datetime.datetime.utcnow()
        delta = now - launch_time
        return delta.total_seconds() / 3600.0 % 1.0

    return 1.0 - partialBillingInterval(instance)


def awsFilterImpairedNodes(nodes, ec2):
    # if TOIL_AWS_NODE_DEBUG is set don't terminate nodes with
    # failing status checks so they can be debugged
    nodeDebug = less_strict_bool(os.environ.get('TOIL_AWS_NODE_DEBUG'))
    if not nodeDebug:
        return nodes
    nodeIDs = [node.id for node in nodes]
    statuses = ec2.get_all_instance_status(instance_ids=nodeIDs)
    statusMap = {status.id: status.instance_status for status in statuses}
    healthyNodes = [node for node in nodes if statusMap.get(node.id, None) != 'impaired']
    impairedNodes = [node.id for node in nodes if statusMap.get(node.id, None) == 'impaired']
    logger.warn('TOIL_AWS_NODE_DEBUG is set and nodes %s have failed EC2 status checks so '
                'will not be terminated.', ' '.join(impairedNodes))
    return healthyNodes


class Cluster(object):
    def __init__(self, clusterName, provisioner):
        self.clusterName = clusterName
        if provisioner == 'aws':
            from toil.provisioners.aws.awsProvisioner import AWSProvisioner
            self.provisioner = AWSProvisioner
        elif provisioner == 'cgcloud':
            from toil.provisioners.cgcloud.provisioner import CGCloudProvisioner
            self.provisioner = CGCloudProvisioner
        else:
            assert False, "Invalid provisioner '%s'" % provisioner

    def sshCluster(self, args):
        self.provisioner.sshLeader(self.clusterName, args)

    def rsyncCluster(self, args):
        self.provisioner.rsyncLeader(self.clusterName, args)

    def destroyCluster(self):
        self.provisioner.destroyCluster(self.clusterName)
