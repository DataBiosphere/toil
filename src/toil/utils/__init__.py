from __future__ import absolute_import

from toil import version
import logging

logger = logging.getLogger(__name__)


def addBasicProvisionerOptions(parser):
    parser.add_argument("--version", action='version', version=version)
    parser.add_argument('-p', "--provisioner", dest='provisioner', choices=['aws'], required=False, default="aws",
                        help="The provisioner for cluster auto-scaling. Only aws is currently "
                             "supported")
    try:
        from toil.provisioners.aws import getCurrentAWSZone
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
