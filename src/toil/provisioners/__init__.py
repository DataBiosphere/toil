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
from bd2k.util import parse_iso_utc



def AWSRemainingBillingInterval(instance):
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


class Cluster(object):
    def __init__(self, clusterName, provisioner):
        self.clusterName = clusterName
        self.provisioner = None
        if provisioner == 'aws':
            from toil.provisioners.aws.awsProvisioner import AWSProvisioner
            self.provisioner = AWSProvisioner
        elif provisioner == 'cgcloud':
            from toil.provisioners.cgcloud.provisioner import CGCloudProvisioner
            self.provisioner = CGCloudProvisioner
        else:
            raise RuntimeError('The only options are aws and cgcloud')

    def sshCluster(self):
        self.provisioner.sshLeader(self.clusterName)

    def destroyCluster(self):
        self.provisioner.destroyCluster(self.clusterName)
