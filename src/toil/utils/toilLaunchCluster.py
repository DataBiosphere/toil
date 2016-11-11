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
from toil.lib.bioio import parseBasicOptions, setLoggingFromOptions, getBasicOptionParser
from toil.utils import addBasicProvisionerOptions

logger = logging.getLogger( __name__ )


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
    config = parseBasicOptions(parser)
    setLoggingFromOptions(config)
    spotBid = None
    if config.provisioner == 'aws':
        logger.info('Using aws provisioner.')
        try:
            from toil.provisioners.aws.awsProvisioner import AWSProvisioner
        except ImportError:
            raise RuntimeError('The aws extra must be installed to use this provisioner')
        provisioner = AWSProvisioner
        parsedBid = config.nodeType.split(':', 1)
        if len(config.nodeType) != len(parsedBid[0]):
            # there is a bid
            spotBid = float(parsedBid[1])
            config.nodeType = parsedBid[0]
    else:
        assert False

    provisioner.launchCluster(instanceType=config.nodeType, clusterName=config.clusterName,
                              keyName=config.keyPairName, spotBid=spotBid)
