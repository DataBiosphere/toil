from __future__ import absolute_import

from toil import version
import logging
import os
logger = logging.getLogger(__name__)


def addBasicProvisionerOptions(parser):
    parser.add_argument("--version", action='version', version=version)
    parser.add_argument('-p', "--provisioner", dest='provisioner', choices=['aws', 'gce'], required=False, default="aws",
                        help="The provisioner for cluster auto-scaling. AWS and Google are currently supported")
    parser.add_argument('-z', '--zone', dest='zone', required=False, default=None,
                        help="The availability zone of the master. This parameter can also be set via the 'TOIL_X_ZONE' "
                             "environment variable, where X is AWS or GCE, or by the ec2_region_name parameter "
                             "in your .boto file, or derived from the instance metadata if using this utility on an "
                             "existing EC2 instance.")
    parser.add_argument("clusterName", help="The name that the cluster will be identifiable by. "
                                            "Must be lowercase and may not contain the '_' "
                                            "character.")
    return parser


def getZoneFromEnv(provisioner):
    """
    Find the zone specified in an environment variable.

    The user can specify zones in environment variables in lieu of writing them at the commandline every time.
    Given a provisioner, this method will look for the stored value and return it.
    :param str provisioner: One of the supported provisioners ('aws', 'gce')
    :rtype: str
    :return: None or the value stored in a 'TOIL_X_ZONE' environment variable.
    """

    return os.environ.get('TOIL_' + provisioner.upper() + '_ZONE')
