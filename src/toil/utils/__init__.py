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

# Python 3 compatibility imports
from __future__ import absolute_import

import logging

from toil import version
from toil.provisioners.aws import getCurrentAWSZone

logger = logging.getLogger(__name__)


def addBasicProvisionerOptions(parser):
    parser.add_argument("--version", action='version', version=version)
    parser.add_argument('-p', "--provisioner", dest='provisioner', choices=['aws'], required=False, default="aws",
                        help="The provisioner for cluster auto-scaling. Only aws is currently "
                             "supported")
    try:
        currentZone = getCurrentAWSZone()
    except ImportError:
        currentZone = None
    zoneString = currentZone if currentZone else 'No zone could be determined'
    parser.add_argument('-z', '--zone', dest='zone', required=False, default=currentZone,
                        help="The AWS availability zone of the master. This parameter can also be "
                             "set via the TOIL_AWS_ZONE environment variable, or by the ec2_region_name "
                             "parameter in your .boto file, or derived from the instance metadata if "
                             "using this utility on an existing EC2 instance. "
                             "Currently: %s" % zoneString)
    parser.add_argument("clusterName", help="The name that the cluster will be identifiable by. "
                                            "Must be lowercase and may not contain the '_' "
                                            "character.")
    return parser
