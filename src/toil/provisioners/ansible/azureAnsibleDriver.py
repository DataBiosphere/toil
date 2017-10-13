import logging
import sys

from toil.provisioners.ansible import azureAnsibleProvisioner

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():

    logger.debug('instantiating provisioner')
    provisioner = azureAnsibleProvisioner.AzureAnsibleProvisioner()

    logger.debug('invoking provisioner')
    provisioner.parseArgumentsAndRun(sys.argv)

if __name__ == '__main__':
    main()
