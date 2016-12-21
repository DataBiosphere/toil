# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import logging
import os
from collections import namedtuple
from operator import attrgetter
import datetime
from cgcloud.lib.util import std_dev, mean

logger = logging.getLogger(__name__)

ZoneTuple = namedtuple('ZoneTuple', ['name', 'price_deviation'])


def getSpotZone(spotBid, nodeType, ctx):
    return _getCurrentAWSZone(spotBid, nodeType, ctx)


def getCurrentAWSZone():
    return _getCurrentAWSZone()


def _getCurrentAWSZone(spotBid=None, nodeType=None, ctx=None):
    zone = None
    try:
        import boto
        from boto.utils import get_instance_metadata
    except ImportError:
        pass
    else:
        zone = os.environ.get('TOIL_AWS_ZONE', None)
        if spotBid:
            # if spot bid is present, all the other parameters must be as well
            assert bool(spotBid) == bool(nodeType) == bool(ctx)
            # if the zone is unset and we are using the spot market, optimize our
            # choice based on the spot history
            return optimize_spot_bid(ctx=ctx, instance_type=nodeType, spot_bid=float(spotBid))
        if not zone:
            zone = boto.config.get('Boto', 'ec2_region_name')
            if zone is not None:
                zone += 'a'  # derive an availability zone in the region
        if not zone:
            try:
                zone = get_instance_metadata()['placement']['availability-zone']
            except KeyError:
                pass
    return zone


def choose_spot_zone(zones, bid, spot_history):
    """
    Returns the zone to put the spot request based on, in order of priority:

       1) zones with prices currently under the bid

       2) zones with the most stable price

    :param list[boto.ec2.zone.Zone] zones:
    :param float bid:
    :param list[boto.ec2.spotpricehistory.SpotPriceHistory] spot_history:

    :rtype: str
    :return: the name of the selected zone

    >>> from collections import namedtuple
    >>> FauxHistory = namedtuple( 'FauxHistory', [ 'price', 'availability_zone' ] )
    >>> ZoneTuple = namedtuple( 'ZoneTuple', [ 'name' ] )

    >>> zones = [ ZoneTuple( 'us-west-2a' ), ZoneTuple( 'us-west-2b' ) ]
    >>> spot_history = [ FauxHistory( 0.1, 'us-west-2a' ), \
                         FauxHistory( 0.2,'us-west-2a'), \
                         FauxHistory( 0.3,'us-west-2b'), \
                         FauxHistory( 0.6,'us-west-2b')]
    >>> # noinspection PyProtectedMember
    >>> choose_spot_zone( zones, 0.15, spot_history )
    'us-west-2a'

    >>> spot_history=[ FauxHistory( 0.3, 'us-west-2a' ), \
                       FauxHistory( 0.2, 'us-west-2a' ), \
                       FauxHistory( 0.1, 'us-west-2b'), \
                       FauxHistory( 0.6, 'us-west-2b') ]
    >>> # noinspection PyProtectedMember
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'

    >>> spot_history={ FauxHistory( 0.1, 'us-west-2a' ), \
                       FauxHistory( 0.7, 'us-west-2a' ), \
                       FauxHistory( 0.1, "us-west-2b" ), \
                       FauxHistory( 0.6, 'us-west-2b' ) }
    >>> # noinspection PyProtectedMember
    >>> choose_spot_zone(zones, 0.15, spot_history)
    'us-west-2b'
   """

    # Create two lists of tuples of form: [ (zone.name, std_deviation), ... ] one for zones
    # over the bid price and one for zones under bid price. Each are sorted by increasing
    # standard deviation values.
    #
    markets_under_bid, markets_over_bid = [], []
    for zone in zones:
        zone_histories = filter(lambda zone_history:
                                zone_history.availability_zone == zone.name, spot_history)
        price_deviation = std_dev([history.price for history in zone_histories])
        recent_price = zone_histories[0]
        zone_tuple = ZoneTuple(name=zone.name, price_deviation=price_deviation)
        (markets_over_bid, markets_under_bid)[recent_price.price < bid].append(zone_tuple)

    return min(markets_under_bid or markets_over_bid,
               key=attrgetter('price_deviation')).name


def optimize_spot_bid(ctx, instance_type, spot_bid):
    """
    Check whether the bid is sane and makes an effort to place the instance in a sensible zone.
    """
    spot_history = _get_spot_history(ctx, instance_type)
    _check_spot_bid(spot_bid, spot_history)
    zones = ctx.ec2.get_all_zones()
    most_stable_zone = choose_spot_zone(zones, spot_bid, spot_history)
    logger.info("Placing spot instances in zone %s.", most_stable_zone)
    return most_stable_zone


def _check_spot_bid(spot_bid, spot_history):
    """
    Prevents users from potentially over-paying for instances

    Note: this checks over the whole region, not a particular zone

    :param spot_bid: float

    :type spot_history: list[SpotPriceHistory]

    :raises UserError: if bid is > 2X the spot price's average

    >>> from collections import namedtuple
    >>> FauxHistory = namedtuple( "FauxHistory", [ "price", "availability_zone" ] )
    >>> spot_data = [ FauxHistory( 0.1, "us-west-2a" ), \
                      FauxHistory( 0.2, "us-west-2a" ), \
                      FauxHistory( 0.3, "us-west-2b" ), \
                      FauxHistory( 0.6, "us-west-2b" ) ]
    >>> # noinspection PyProtectedMember
    >>> _check_spot_bid( 0.1, spot_data )
    >>> # noinspection PyProtectedMember

    # >>> Box._check_spot_bid( 2, spot_data )
    Traceback (most recent call last):
    ...
    UserError: Your bid $ 2.000000 is more than double this instance type's average spot price ($ 0.300000) over the last week
    """
    average = mean([datum.price for datum in spot_history])
    if spot_bid > average * 2:
        logger.warn("Your bid $ %f is more than double this instance type's average "
                 "spot price ($ %f) over the last week", spot_bid, average)

def _get_spot_history(ctx, instance_type):
    """
    Returns list of 1,000 most recent spot market data points represented as SpotPriceHistory
    objects. Note: The most recent object/data point will be first in the list.

    :rtype: list[SpotPriceHistory]
    """

    one_week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
    spot_data = ctx.ec2.get_spot_price_history(start_time=one_week_ago.isoformat(),
                                               instance_type=instance_type,
                                               product_description="Linux/UNIX")
    spot_data.sort(key=attrgetter("timestamp"), reverse=True)
    return spot_data

ec2FullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="ec2:*")])

s3FullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="s3:*")])

sdbFullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="sdb:*")])

iamFullPolicy = dict(Version="2012-10-17", Statement=[
    dict(Effect="Allow", Resource="*", Action="iam:*")])


logDir = '--log_dir=/var/lib/mesos'
leaderArgs = logDir + ' --registry=in_memory --cluster={name}'
workerArgs = '{keyPath} --work_dir=/var/lib/mesos --master={ip}:5050 --attributes=preemptable:{preemptable} ' + logDir

awsUserData = """#cloud-config

write_files:
    - path: "/home/core/volumes.sh"
      permissions: "0777"
      owner: "root"
      content: |
        #!/bin/bash
        set -x
        ephemeral_count=0
        possible_drives="/dev/xvdb /dev/xvdc /dev/xvdd /dev/xvde"
        drives=""
        directories="toil mesos docker"
        for drive in $possible_drives; do
            echo checking for $drive
            if [ -b $drive ]; then
                echo found it
                ephemeral_count=$((ephemeral_count + 1 ))
                drives="$drives $drive"
                echo increased ephemeral count by one
            fi
        done
        if (("$ephemeral_count" == "0" )); then
            echo no ephemeral drive
            for directory in $directories; do
                sudo mkdir -p /var/lib/$directory
            done
            exit 0
        fi
        sudo mkdir /mnt/ephemeral
        if (("$ephemeral_count" == "1" )); then
            echo one ephemeral drive to mount
            sudo mkfs.ext4 -F $drives
            sudo mount $drives /mnt/ephemeral
        fi
        if (("$ephemeral_count" > "1" )); then
            echo multiple drives
            for drive in $drives; do
                dd if=/dev/zero of=$drive bs=4096 count=1024
            done
            sudo mdadm --create -f --verbose /dev/md0 --level=0 --raid-devices=$ephemeral_count $drives # determine force flag
            sudo mkfs.ext4 -F /dev/md0
            sudo mount /dev/md0 /mnt/ephemeral
        fi
        for directory in $directories; do
            sudo mkdir -p /mnt/ephemeral/var/lib/$directory
            sudo mkdir -p /var/lib/$directory
            sudo mount --bind /mnt/ephemeral/var/lib/$directory /var/lib/$directory
        done

coreos:
    update:
      reboot-strategy: off
    units:
    - name: "volume-mounting.service"
      command: "start"
      content: |
        [Unit]
        Description=mounts ephemeral volumes & bind mounts toil directories
        Author=cketchum@ucsc.edu
        Before=docker.service

        [Service]
        Type=oneshot
        Restart=no
        ExecStart=/usr/bin/bash /home/core/volumes.sh

    - name: "toil-{role}.service"
      command: "start"
      content: |
        [Unit]
        Description=toil-{role} container
        Author=cketchum@ucsc.edu
        After=docker.service

        [Service]
        Restart=on-failure
        ExecStart=/usr/bin/docker run \
            --entrypoint={entrypoint} \
            --net=host \
            -v /var/run/docker.sock:/var/run/docker.sock \
            -v /var/lib/mesos:/var/lib/mesos \
            -v /var/lib/docker:/var/lib/docker \
            -v /var/lib/toil:/var/lib/toil \
            --name=toil_{role} \
            {image} \
            {args}

ssh_authorized_keys:
    - "ssh-rsa {sshKey}"
"""
