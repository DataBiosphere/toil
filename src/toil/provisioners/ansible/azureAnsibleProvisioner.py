import argparse
import logging
import pipettor
from toil.provisioners.ansible.abstractAnsibleProvisioner import AbstractAnsibleProvisioner

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class AzureAnsibleProvisioner(AbstractAnsibleProvisioner):

    def parseArgumentsAndRun(self, arglist):

        # instantiate parser and sub-parser
        parser = argparse.ArgumentParser(description='parse command-line for azure')
        subparsers = parser.add_subparsers(title='sub-commands', description='valid sub-commands')

        # create sub-parser for 'launchCluster' sub-command
        launchClusterParser = subparsers.add_parser('launchCluster', help='Launch a cluster in Azure')
        launchClusterParser.set_defaults(func=self.launchCluster)
        launchClusterParser.add_argument('-n', dest="vmname", help='vm name ', required=True)
        launchClusterParser.add_argument('-g', dest="resgrp", help='resource group name', required=True)
        launchClusterParser.add_argument('-v', dest="vnet", help='virtual network name', required=True)
        launchClusterParser.add_argument('-s', dest="subnet", help='subnet name', required=True)
        launchClusterParser.add_argument('-p', dest="playbook", help='path to playbook', required=True)

        # create sub-parser for 'destroyCluster' sub-command
        destroyClusterParser = subparsers.add_parser('destroyCluster', help='Destroy a cluster in Azure')
        destroyClusterParser.set_defaults(func=self.destroyCluster)
        destroyClusterParser.add_argument('-n', dest="vmname", help='vm name ', required=True)
        destroyClusterParser.add_argument('-g', dest="resgrp", help='resource group name', required=True)
        destroyClusterParser.add_argument('-p', dest="playbook", help='path to playbook', required=True)

        logger.debug('parsing args')
        # trim out the function itself from the front of arglist
        arglist.pop(0)

        # now parse the args
        arguments = parser.parse_args(arglist)

        # if the function being invoked exists, then invoke it with the parsed arguments
        if arguments.func:
            arguments.func(arguments)

    def launchCluster(self, args):

        # construct the command string
        logger.debug("constructing command string from args ")
        command = "ansible-playbook -c local --extra-vars " + "'" + "vmname=" + args.vmname + " resgrp=" + args.resgrp \
                  + " vnet=" + args.vnet + " subnet=" + args.subnet + "' " + args.playbook

        # launch the command
        logger.info("executing command: " + command)
        out = pipettor.runlex(command)

        logger.info(out)


    def destroyCluster(self, args):

        # construct the command string
        logger.debug("constructing command string from args")
        command = "ansible-playbook -c local --extra-vars " + "'" + "vmname=" + args.vmname + " resgrp=" + args.resgrp \
                  + "' " + args.playbook

        logger.info("executing command: " + command)
        out = pipettor.runlex(command)

        logger.info(out)
