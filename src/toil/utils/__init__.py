from __future__ import absolute_import

import os
from toil import version
from toil.lib.bioio import getBasicOptionParser, parseBasicOptions, setLoggingFromOptions
import logging

logger = logging.getLogger( __name__ )


def getCurrentAWSZone():
    zone = None
    try:
        import boto
        from boto.utils import get_instance_metadata
    except ImportError:
        pass
    else:
        zone = os.environ.get('TOIL_AWS_ZONE', None)
        if not zone:
            zone = boto.config.get('Boto', 'ec2_region_name')
            zone += 'a'  # derive an availability zone in the region
        if not zone:
            try:
                zone = get_instance_metadata()['placement']['availability-zone']
            except KeyError:
                pass
    return zone if zone else 'No zone set.'


def getBasicProvisionerParser():
    parser = getBasicOptionParser()
    parser.add_argument("--version", action='version', version=version)
    parser.add_argument('-p', "--provisioner", dest='provisioner', choices=['aws'], required=True,
                        help="The provisioner for cluster auto-scaling. Only aws is currently "
                             "supported")
    currentZone = getCurrentAWSZone()
    parser.add_argument('-z', '--zone', dest='zone', required=False, default=currentZone,
                        help="The AWS availability zone of the master. This parameter can also be "
                             "set via the TOIL_AWS_ZONE environment variable, or by the ec2_region_name "
                             "parameter in your .boto file, or derived from the instance metadata if "
                             "using this utility on an existing EC2 instance. "
                             "Currently: %s" % currentZone)
    parser.add_argument("clusterName", help="The name that the cluster will be identifiable by")
    return parser
