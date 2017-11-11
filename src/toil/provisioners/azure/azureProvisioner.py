import logging
import pipettor
from toil.provisioners.abstractProvisioner import AbstractProvisioner
import os.path

logger = logging.getLogger(__name__)


def callAnsible(playbook, args):
    extravar = "".join(["%s=%s " % (k, v) for k, v in args.items()])
    playbook = os.path.join(os.path.dirname(os.path.realpath(__file__)), playbook)
    command = "ansible-playbook -c local --extra-vars '%s' %s" % (extravar, playbook)
    logger.info("Executing Ansible call `%s`" % command)
    return pipettor.runlex(command)


class AzureProvisioner(AbstractProvisioner):
    def launchCluster(self, instanceType, keyName, clusterName, spotBid=None, **kwargs):
        if not self.isValidClusterName(clusterName):
            raise RuntimeError("invalid cluster name")  # should this be parsed at argument parsing time?

        if spotBid:
            raise NotImplementedError

        args = {
            'vmsize': instanceType,
            'sshkey': keyName,
            'vmname': clusterName,
            'resgrp': clusterName,
            'subnet': clusterName,
            'vnet': clusterName,
        }
        out = callAnsible('create-azure-vm.yml', args)
        logger.debug(out)

    @classmethod
    def destroyCluster(self, clusterName, zone):
        args = {
            'vmname': clusterName,
            'resgrp': clusterName,
        }
        out = callAnsible('delete-azure-vm.yml', args)
        logger.debug(out)

    def addNodes(self):
        raise NotImplementedError

    def getNodeShape(self):
        raise NotImplementedError

    def getProvisionedWorkers(self):
        raise NotImplementedError

    def remainingBillingInterval(self):
        raise NotImplementedError

    def terminateNodes(self):
        raise NotImplementedError

    @staticmethod
    def isValidClusterName(name):
        """Valid Azure cluster names must be between 3 and 24 characters in
        length and use numbers and lower-case letters only."""
        if len(name) > 24 or len(name) < 3:
            return False
        if any(not (c.isdigit() or c.islower()) for c in name):
            return False
        return True
