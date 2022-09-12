import json
import logging
import os
import time
import urllib.request
from typing import Dict, Optional

from botocore.client import BaseClient

from toil.lib.retry import retry

logger = logging.getLogger(__name__)


def get_flatcar_ami(ec2_client: BaseClient, architecture: str = 'amd64') -> str:
    """
    Retrieve the flatcar AMI image to use as the base for all Toil autoscaling instances.

    AMI must be available to the user on AWS (attempting to launch will return a 403 otherwise).

    Priority is:
      1. User specified AMI via TOIL_AWS_AMI
      2. Official AMI from stable.release.flatcar-linux.net
      3. Search the AWS Marketplace

    If all of these sources fail, we raise an error to complain.

    :param ec2_client: Boto3 EC2 Client
    :param architecture: The architecture type for the new AWS machine. Can be either amd64 or arm64
    """

    # How many times should we bang on the Flatcar release feed before moving
    # on if it can't get us an AMI?
    MAX_TRIES = 3

    # Take a user override
    ami = os.environ.get('TOIL_AWS_AMI')
    try_number = 0
    while not ami and try_number < MAX_TRIES:
        try_number += 1
        logger.debug('No AMI found in TOIL_AWS_AMI; checking Flatcar release feed (try %s)', try_number)
        ami = official_flatcar_ami_release(ec2_client=ec2_client, architecture=architecture)
        if not ami and try_number < MAX_TRIES:
            time.sleep(10)
    if not ami:
        logger.warning('No available AMI found in Flatcar release feed; checking marketplace')
        ami = aws_marketplace_flatcar_ami_search(ec2_client=ec2_client, architecture=architecture)
    if not ami:
        logger.critical('No available AMI found in marketplace')
        raise RuntimeError('Unable to fetch the latest flatcar image.')
    logger.info('Selected Flatcar AMI: %s', ami)
    return ami


@retry()  # TODO: What errors do we get for timeout, JSON parse failure, etc?
def official_flatcar_ami_release(ec2_client: BaseClient, architecture: str = 'amd64') -> Optional[str]:
    """
    Check stable.release.flatcar-linux.net for the latest flatcar AMI.  Verify it's on AWS.

    :param ec2_client: Boto3 EC2 Client
    :param architecture: The architecture type for the new AWS machine. Can be either amd64 or arm64
    """
    # Flatcar images only live for 9 months.
    # Rather than hardcode a list of AMIs by region that will die, we use
    # their JSON feed of the current ones.

    JSON_FEED_URL = f'https://stable.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json'
    region = ec2_client._client_config.region_name  # type: ignore
    feed = json.loads(urllib.request.urlopen(JSON_FEED_URL).read())

    try:
        for ami_record in feed['amis']:
            # Scan the list of regions
            if ami_record['name'] == region:
                # When we find ours, return the AMI ID
                ami = ami_record['hvm']
                # verify it exists on AWS
                response = ec2_client.describe_images(Filters=[{'Name': 'image-id', 'Values': [ami]}])  # type: ignore
                if len(response['Images']) == 1 and response['Images'][0]['State'] == 'available':
                    return ami  # type: ignore
        # We didn't find it
        logger.warning(f'Flatcar release feed at {JSON_FEED_URL} does not have an image for region {region}')
    except KeyError:
        # We didn't see a field we need
        logger.warning(f'Flatcar release feed at {JSON_FEED_URL} does not have expected format')
    return None


@retry()  # TODO: What errors do we get for timeout, JSON parse failure, etc?
def aws_marketplace_flatcar_ami_search(ec2_client: BaseClient, architecture: str = 'amd64') -> Optional[str]:
    """Query AWS for all AMI names matching 'Flatcar-stable-*' and return the most recent one."""

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.describe_images
    # Possible arch choices on AWS: 'i386'|'x86_64'|'arm64'|'x86_64_mac'
    architecture_mapping = {'amd64': 'x86_64',
                            'arm64': 'arm64'}
    response: dict = ec2_client.describe_images(Owners=['aws-marketplace'],  # type: ignore
                                                Filters=[{'Name': 'name', 'Values': ['Flatcar-stable-*']}])
    latest: Dict[str, str] = {'CreationDate': '0lder than atoms.'}
    for image in response['Images']:
        if image['Architecture'] == architecture_mapping[architecture] and image['State'] == 'available':
            if image['CreationDate'] > latest['CreationDate']:
                latest = image
    return latest.get('ImageId', None)
