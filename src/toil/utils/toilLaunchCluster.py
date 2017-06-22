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
"""
Launches a toil leader instance with the specified provisioner
"""
import logging
from toil.lib.bioio import parseBasicOptions, getBasicOptionParser
from toil.utils import addBasicProvisionerOptions

logger = logging.getLogger(__name__)


def createTagsDict(tagList):
    tagsDict = dict()
    for tag in tagList:
        key, value = tag.split('=')
        tagsDict[key] = value
    return tagsDict


def main():
    parser = getBasicOptionParser()
    parser = addBasicProvisionerOptions(parser)
    parser.add_argument("--nodeType", dest='nodeType', required=True,
                        help="Node type for {non-|}preemptable nodes. The syntax depends on the "
                             "provisioner used. For the aws provisioner this is the name of an "
                             "EC2 instance type followed by a colon and the price in dollar to "
                             "bid for a spot instance, for example 'c3.8xlarge:0.42'.")
    parser.add_argument("--keyPairName", dest='keyPairName', required=True,
                        help="The name of the AWS key pair to include on the instance")
    parser.add_argument("-t", "--tag", metavar='NAME=VALUE', dest='tags', default=[], action='append',
                        help="Tags are added to the AWS cluster for this node and all of its "
                             "children. Tags are of the form:\n"
                             " --t key1=value1 --tag key2=value2\n"
                             "Multiple tags are allowed and each tag needs its own flag. By "
                             "default the cluster is tagged with "
                             " {\n"
                             "      \"Name\": clusterName,\n"
                             "      \"Owner\": IAM username\n"
                             " }. ")
    parser.add_argument("--vpcSubnet",
                        help="VPC subnet ID to launch cluster in. Uses default subnet if not specified."
                        "This subnet needs to have auto assign IPs turned on.")
    parser.add_argument("-w", "--workers", dest='workers', default=0, type=int,
                        help="Specify a number of workers to launch alongside the leader when the "
                             "cluster is created. This can be useful if running toil without "
                             "auto-scaling but with need of more hardware support")
    config = parseBasicOptions(parser)
    tagsDict = None if config.tags is None else createTagsDict(config.tags)

    spotBid = None
    if config.provisioner == 'aws':
        logger.info('Using aws provisioner.')
        try:
            from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        except ImportError:
            raise RuntimeError('The aws extra must be installed to use this provisioner')
        provisioner = AWSProvisioner()
        parsedBid = config.nodeType.split(':', 1)
        if len(config.nodeType) != len(parsedBid[0]):
            # there is a bid
            spotBid = float(parsedBid[1])
            config.nodeType = parsedBid[0]
    else:
        assert False

    provisioner.launchCluster(instanceType=config.nodeType,
                              keyName=config.keyPairName,
                              clusterName=config.clusterName,
                              workers=config.workers,
                              spotBid=spotBid,
                              userTags=tagsDict,
                              zone=config.zone,
                              vpcSubnet=config.vpcSubnet)
