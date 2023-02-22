import json
import logging
import os
import time
import urllib.request
from urllib.error import HTTPError
from typing import Dict, Optional, Iterator, cast

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

    # Take a user override
    ami = os.environ.get('TOIL_AWS_AMI')
    try_number = 0
    if not ami:
        logger.debug('No AMI found in TOIL_AWS_AMI; checking stable Flatcar release feed')
        ami = feed_flatcar_ami_release(ec2_client=ec2_client, architecture=architecture, source='stable')
    if not ami:
        logger.warning('No available AMI found in Flatcar release feed; checking marketplace')
        ami = aws_marketplace_flatcar_ami_search(ec2_client=ec2_client, architecture=architecture)
    if not ami:
        logger.debug('No AMI found in marketplace; checking Toil Flatcar release feed')
        ami = feed_flatcar_ami_release(ec2_client=ec2_client, architecture=architecture, source='toil')
    if not ami:
        logger.debug('No AMI found in Toil project feed; checking beta Flatcar release feed')
        ami = feed_flatcar_ami_release(ec2_client=ec2_client, architecture=architecture, source='beta')
    if not ami:
        logger.debug('No AMI found in beta Flatcar release feed; checking archived Flatcar release feed')
        ami = feed_flatcar_ami_release(ec2_client=ec2_client, architecture=architecture, source='archive')
    if not ami:
        logger.critical('No available Flatcar AMI in any source!')
        raise RuntimeError(f'Unable to fetch the latest flatcar image. Upload '
                           f'https://stable.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_image.bin.bz2 '
                           f'to AWS as am AMI and set TOIL_AWS_AMI in the environment to its AMI ID.')
    logger.info('Selected Flatcar AMI: %s', ami)
    return ami

@retry(errors=[HTTPError])
def _fetch_flatcar_feed(architecture: str = 'amd64', source: str = 'stable') -> bytes:
    """
    Get the binary data of the Flatcar release feed for the given architecture.
    
    :param source: can be set to a Flatcar release channel ('stable', 'beta',
           or 'alpha'), 'archive' to check the Internet Archive for a feed,
           and 'toil' to check if the Toil project has put up a feed.
    
    :raises HTTPError: if the feed cannot be fetched.
    """
    
    # We have a few places we know to get the feed from.
    JSON_FEED_URL = {
        'stable': f'https://stable.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json',
        'beta': f'https://beta.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json',
        'alpha': f'https://alpha.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json',
        'archive': f'https://web.archive.org/web/20220625112618if_/https://stable.release.flatcar-linux.net/{architecture}-usr/current/flatcar_production_ami_all.json',
        'toil': f'https://raw.githubusercontent.com/DataBiosphere/toil/master/contrib/flatcar/{architecture}-usr/current/flatcar_production_ami_all.json'
    }[source]
    return cast(bytes, urllib.request.urlopen(JSON_FEED_URL).read())

def flatcar_release_feed_amis(region: str, architecture: str = 'amd64', source: str = 'stable') -> Iterator[str]:
    """
    Yield AMI IDs for the given architecture from the Flatcar release feed.
    
    :param source: can be set to a Flatcar release channel ('stable', 'beta',
           or 'alpha'), 'archive' to check the Internet Archive for a feed,
           and 'toil' to check if the Toil project has put up a feed.
    
    Retries if the release feed cannot be fetched. If the release feed has a
    permanent error, yields nothing. If some entries in the release feed are
    unparseable, yields the others.
    """
    
    # If we get non-JSON content we want to retry.
    MAX_TRIES = 3
    
    try_number = 0
    while try_number < MAX_TRIES:
        try:
            feed = json.loads(_fetch_flatcar_feed(architecture, source))
            break
        except HTTPError:
            # Flatcar servers did not return the feed
            logger.exception(f'Could not retrieve {source} Flatcar release feed JSON')
            # Don't retry
            return
        except json.JSONDecodeError:
            # Feed is not JSON
            logger.exception(f'Could not decode {source} Flatcar release feed JSON')
            # Try again
            try_number += 1
            continue
    if try_number == MAX_TRIES:
        # We could not get the JSON
        logger.error(f'Could not get a readable {source} Flatcar release feed JSON')
        # Bail on this method
        return

    for ami_record in feed.get('amis', []):
        # Scan the list of regions
        if ami_record.get('name', None) == region:
            # When we find ours, return the AMI ID
            if 'hvm' in ami_record:
                yield ami_record['hvm']
            # And stop, there should be one per region.
            return
    # We didn't find our region
    logger.warning(f'Flatcar {source} release feed does not have an image for region {region}')
    
    

@retry()  # TODO: What errors do we get for timeout, JSON parse failure, etc?
def feed_flatcar_ami_release(ec2_client: BaseClient, architecture: str = 'amd64', source: str = 'stable') -> Optional[str]:
    """
    Check a Flatcar release feed for the latest flatcar AMI.
    
    Verify it's on AWS.

    :param ec2_client: Boto3 EC2 Client
    :param architecture: The architecture type for the new AWS machine. Can be either amd64 or arm64
    :param source: can be set to a Flatcar release channel ('stable', 'beta',
           or 'alpha'), 'archive' to check the Internet Archive for a feed,
           and 'toil' to check if the Toil project has put up a feed.
    """
    # Flatcar images only live for 9 months.
    # Rather than hardcode a list of AMIs by region that will die, we use
    # their JSON feed of the current ones.

    region = ec2_client._client_config.region_name  # type: ignore 
                
    for ami in flatcar_release_feed_amis(region, architecture, source):
        # verify it exists on AWS
        response = ec2_client.describe_images(Filters=[{'Name': 'image-id', 'Values': [ami]}])  # type: ignore
        if len(response['Images']) == 1 and response['Images'][0]['State'] == 'available':
            return ami 
        else:
            logger.warning(f'Flatcar release feed suggests image {ami} which does not exist on AWS in {region}')
    # We didn't find it
    logger.warning(f'Flatcar release feed does not have an image for region {region} that exists on AWS')
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
