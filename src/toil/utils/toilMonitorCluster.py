"""Starts a monitoring service on the cluster leader
"""
import argparse
import logging

from toil.lib.bioio import parseBasicOptions, setLoggingFromOptions, getBasicOptionParser
from toil.provisioners import Cluster
from toil.utils import addBasicProvisionerOptions


logger = logging.getLogger(__name__)


def main():
    parser = getBasicOptionParser()
    parser = addBasicProvisionerOptions(parser)
    parser.add_argument("--keyPairName")
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Arguments to pass to"
                        "`start-monitoring`. Takes any arguments that rsync accepts. Specify the"
                        " remote with a colon. For example, to upload `example.py`,"
                        " specify `toil rsync-cluster -p aws test-cluster example.py :`."
                        "\nOr, to download a file from the remote:, `toil rsync-cluster"
                        " -p aws test-cluster :example.py .`")
    config = parseBasicOptions(parser)
    setLoggingFromOptions(config)
    cluster = Cluster(provisioner=config.provisioner, clusterName=config.clusterName, zone=config.zone)
    cluster.monitorCluster(args=config.args)
