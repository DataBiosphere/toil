# Copyright (C) 2015-2018 Regents of the University of California
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
from __future__ import absolute_import
import requests
import json


EC2Regions = {'us-west-1': 'US West (N. California)',
              'us-west-2': 'US West (Oregon)',
              'us-east-1': 'US East (N. Virginia)',
              'us-east-2': 'US East (Ohio)',
              'us-gov-west-1': 'AWS GovCloud (US)',
              'ca-central-1': 'Canada (Central)',
              'ap-northeast-1': 'Asia Pacific (Tokyo)',
              'ap-northeast-2': 'Asia Pacific (Seoul)',
              'ap-northeast-3': 'Asia Pacific (Osaka-Local)',
              'ap-southeast-1': 'Asia Pacific (Singapore)',
              'ap-southeast-2': 'Asia Pacific (Sydney)',
              'ap-south-1': 'Asia Pacific (Mumbai)',
              'eu-west-1': 'EU (Ireland)',
              'eu-west-2': 'EU (London)',
              'eu-west-3': 'EU (Paris)',
              'eu-central-1': 'EU (Frankfurt)',
              'sa-east-1': 'South America (Sao Paulo)'}


class InstanceType(object):
    __slots__ = ('name', 'cores', 'memory', 'disks', 'disk_capacity', 'cost')
    def __init__(self, name, cores, memory, disks, disk_capacity, cost):
        self.name = name # the API name of the instance type
        self.cores = cores  # the number of cores
        self.memory = memory # RAM in GB
        self.disks = disks # the number of ephemeral (aka 'instance store') volumes
        self.disk_capacity = disk_capacity # the capacity of each ephemeral volume in GB
        self.cost = cost

    def __str__(self):
        return ("Type: {}\n"
                "Cores: {}\n"
                "Disks: {}\n"
                "Memory: {}\n"
                "Disk Capacity: {}\n"
                "Cost: {}\n".format(self.name,
                                             self.cores,
                                             self.disks,
                                             self.memory,
                                             self.disk_capacity,
                                             self.cost))

    def __eq__(self, other):
        # exclude cost
        if (self.name == other.name and
            self.cores == other.cores and
            self.memory == other.memory and
            self.disks == other.disks and
            self.disk_capacity == other.disk_capacity):
            return True
        return False


# backup
defaultE2List = [
    InstanceType(name='t2.nano', cores=1, memory=0.5, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='t2.micro', cores=1, memory=1, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='t2.small', cores=1, memory=2, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='t2.medium', cores=2, memory=4, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='t2.large', cores=2, memory=8, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='t2.xlarge', cores=4, memory=16, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='t2.2xlarge', cores=8, memory=32, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='m3.medium', cores=1, memory=3.75, disks=1, disk_capacity=4, cost=0),
    InstanceType(name='m3.large', cores=2, memory=7.5, disks=1, disk_capacity=32, cost=0),
    InstanceType(name='m3.xlarge', cores=4, memory=15, disks=2, disk_capacity=40, cost=0),
    InstanceType(name='m3.2xlarge', cores=8, memory=30, disks=2, disk_capacity=80, cost=0),
    InstanceType(name='m4.large', cores=2, memory=8, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='m4.xlarge', cores=4, memory=16, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='m4.2xlarge', cores=8, memory=32, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='m4.4xlarge', cores=16, memory=64, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='m4.10xlarge', cores=40, memory=160, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='m4.16xlarge', cores=64, memory=256, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='c4.large', cores=2, memory=3.75, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='c4.xlarge', cores=4, memory=7.5, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='c4.2xlarge', cores=8, memory=15, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='c4.4xlarge', cores=16, memory=30, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='c4.8xlarge', cores=36, memory=60, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='c3.large', cores=2, memory=3.75, disks=2, disk_capacity=16, cost=0),
    InstanceType(name='c3.xlarge', cores=4, memory=7.5, disks=2, disk_capacity=40, cost=0),
    InstanceType(name='c3.2xlarge', cores=8, memory=15, disks=2, disk_capacity=80, cost=0),
    InstanceType(name='c3.4xlarge', cores=16, memory=30, disks=2, disk_capacity=160, cost=0),
    InstanceType(name='c3.8xlarge', cores=32, memory=60, disks=2, disk_capacity=320, cost=0),
    InstanceType(name='p2.xlarge', cores=4, memory=61, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='p2.8xlarge', cores=32, memory=488, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='p2.16xlarge', cores=64, memory=732, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='g3.4xlarge', cores=16, memory=122, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='g3.8xlarge', cores=32, memory=244, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='g3.16xlarge', cores=64, memory=488, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='g2.2xlarge', cores=8, memory=15, disks=1, disk_capacity=60, cost=0),
    InstanceType(name='g2.8xlarge', cores=32, memory=60, disks=2, disk_capacity=120, cost=0),
    InstanceType(name='x1.16large', cores=64, memory=976, disks=1, disk_capacity=1920, cost=0),
    InstanceType(name='x1.32large', cores=128, memory=1952, disks=2, disk_capacity=1920, cost=0),
    InstanceType(name='x1e.32large', cores=128, memory=3904, disks=2, disk_capacity=1920, cost=0),
    InstanceType(name='r3.large', cores=2, memory=15, disks=1, disk_capacity=32, cost=0),
    InstanceType(name='r3.xlarge', cores=4, memory=30.5, disks=1, disk_capacity=80, cost=0),
    InstanceType(name='r3.2xlarge', cores=8, memory=61, disks=1, disk_capacity=160, cost=0),
    InstanceType(name='r3.4xlarge', cores=16, memory=122, disks=1, disk_capacity=320, cost=0),
    InstanceType(name='r3.8xlarge', cores=32, memory=244, disks=2, disk_capacity=320, cost=0),
    InstanceType(name='r4.large', cores=2, memory=15, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='r4.xlarge', cores=4, memory=30.5, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='r4.2xlarge', cores=8, memory=61, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='r4.4xlarge', cores=16, memory=122, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='r4.8xlarge', cores=32, memory=244, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='r4.16xlarge', cores=64, memory=488, disks=0, disk_capacity=0, cost=0),
    InstanceType(name='i2.xlarge', cores=4, memory=30.5, disks=1, disk_capacity=800, cost=0),
    InstanceType(name='i2.2xlarge', cores=8, memory=61, disks=2, disk_capacity=800, cost=0),
    InstanceType(name='i2.4xlarge', cores=16, memory=122, disks=4, disk_capacity=800, cost=0),
    InstanceType(name='i2.8xlarge', cores=32, memory=244, disks=8, disk_capacity=800, cost=0),
    InstanceType(name='i3.large', cores=2, memory=15.25, disks=1, disk_capacity=475, cost=0),
    InstanceType(name='i3.xlarge', cores=4, memory=30.5, disks=1, disk_capacity=950, cost=0),
    InstanceType(name='i3.2xlarge', cores=8, memory=61, disks=1, disk_capacity=1900, cost=0),
    InstanceType(name='i3.4xlarge', cores=16, memory=122, disks=2, disk_capacity=1900, cost=0),
    InstanceType(name='i3.8xlarge', cores=32, memory=244, disks=4, disk_capacity=1900, cost=0),
    InstanceType(name='i3.16xlarge', cores=64, memory=488, disks=8, disk_capacity=1900, cost=0),
    InstanceType(name='d2.xlarge', cores=4, memory=30.5, disks=3, disk_capacity=2000, cost=0),
    InstanceType(name='d2.2xlarge', cores=8, memory=61, disks=6, disk_capacity=2000, cost=0),
    InstanceType(name='d2.4xlarge', cores=16, memory=122, disks=12, disk_capacity=2000, cost=0),
    InstanceType(name='d2.8xlarge', cores=36, memory=244, disks=24, disk_capacity=2000, cost=0),
    InstanceType(name='m1.small', cores=1, memory=1.7, disks=1, disk_capacity=160, cost=0),
    InstanceType(name='m1.medium', cores=1, memory=3.75, disks=1, disk_capacity=410, cost=0),
    InstanceType(name='m1.large', cores=2, memory=7.5, disks=2, disk_capacity=420, cost=0),
    InstanceType(name='m1.xlarge', cores=4, memory=15, disks=4, disk_capacity=420, cost=0),
    InstanceType(name='c1.medium', cores=2, memory=1.7, disks=1, disk_capacity=350, cost=0),
    InstanceType(name='c1.xlarge', cores=8, memory=7, disks=4, disk_capacity=420, cost=0),
    InstanceType(name='cc2.8xlarge', cores=32, memory=60.5, disks=4, disk_capacity=840, cost=0),
    InstanceType(name='m2.xlarge', cores=2, memory=17.1, disks=1, disk_capacity=420, cost=0),
    InstanceType(name='m2.2xlarge', cores=4, memory=34.2, disks=1, disk_capacity=850, cost=0),
    InstanceType(name='m2.4xlarge', cores=8, memory=68.4, disks=2, disk_capacity=840, cost=0),
    InstanceType(name='cr1.8xlarge', cores=32, memory=244, disks=2, disk_capacity=120, cost=0),
    InstanceType(name='hi1.4xlarge', cores=16, memory=60.5, disks=2, disk_capacity=1024, cost=0),
    InstanceType(name='hs1.8xlarge', cores=16, memory=117, disks=24, disk_capacity=2048, cost=0),
    InstanceType(name='t1.micro', cores=1, memory=0.615, disks=0, disk_capacity=0, cost=0)]


def is_number(s):
    """
    Determines if a unicode string (that may include commas) is a number.

    :param s: Any unicode string.
    :return: True if s represents a number, False otherwise.
    """
    s = s.replace(',', '')
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError) as e:
        pass
    return False


def parseStorage(storageData):
    """
    Parses EC2 JSON storage param string into a number.

    Examples:
        "2 x 160 SSD"
        "3 x 2000 HDD"
        "EBS only"
        "1 x 410"
        "8 x 1.9 NVMe SSD"

    :param str storageData: EC2 JSON storage param string.
    :return: Two floats representing: (# of disks), and (disk_capacity in GiB of each disk).
    """
    if storageData == "EBS only":
        return [0, 0]
    else:
        specs = storageData.strip().split()
        if is_number(specs[0]) and specs[1] == 'x' and is_number(specs[2]):
            return float(specs[0].replace(',', '')), float(specs[2].replace(',', ''))
        else:
            raise RuntimeError('EC2 JSON format has likely changed.  Error parsing disk specs.')


def parseMemory(memAttribute):
    """
    Returns EC2 'memory' string as a float.

    Format should always be '#' GiB (example: '244 GiB').
    If the syntax ever changes, this will raise.

    :param memAttribute: EC2 JSON memory param string.
    :return: A float representing memory in GiB.
    """
    mem = memAttribute.split()
    if mem[1] == 'GiB':
        return float(mem[0])
    else:
        raise RuntimeError('EC2 JSON format has likely changed.  Error parsing memory.')


def fetchEC2InstanceList(regionNickname=None, listSource=None, latest=False):
    """
    Fetches EC2 instances types by region programmatically using the AWS pricing API.

    See: https://aws.amazon.com/blogs/aws/new-aws-price-list-api/

    :return: A list of InstanceType objects.
    """
    if listSource is None:
        ec2Source = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json'
    if regionNickname is None:
        regionNickname = 'us-west-2'
    region = EC2Regions[regionNickname] # JSON uses verbose region names as keys

    ec2InstanceList = []

    if latest:
        # summon the API to grab the latest instance types/prices/specs
        response = requests.get(ec2Source)
        if response.ok:
            ec2InstanceList = parseEC2Json2List(jsontext=response.text, region=region)

    if ec2InstanceList:
        return dict((_.name, _) for _ in ec2InstanceList)
    else:
        return dict((_.name, _) for _ in defaultE2List)


def parseEC2Json2List(jsontext, region):
    """
    Takes a JSON and returns a list of InstanceType objects representing EC2 instance params.

    :param jsontext:
    :param region:
    :return:
    """
    currentList = json.loads(jsontext)
    ec2InstanceList = []
    for k, v in currentList["products"].iteritems():
        if "location" in v["attributes"] and v["attributes"]["location"] == region:
            # 3 tenant types: 'Host' (always $0.00; just a template?)
            #                 'Dedicated' (toil does not support; these are pricier)
            #                 'Shared' (AWS default and what toil uses)
            if "tenancy" in v["attributes"] and v["attributes"]["tenancy"] == "Shared":
                if v["attributes"]["operatingSystem"] == "Linux":
                    disks, disk_capacity = parseStorage(v["attributes"]["storage"])
                    memory = parseMemory(v["attributes"]["memory"])
                    cost = fetchE2Cost(k, currentList["terms"]["OnDemand"])
                    instance = InstanceType(name=v["attributes"]["instanceType"],
                                            cores=v["attributes"]["vcpu"],
                                            memory=memory,
                                            disks=disks,
                                            disk_capacity=disk_capacity,
                                            cost=cost)
                    assert cost >= 0 # seems good to check
                    if instance not in ec2InstanceList:
                        ec2InstanceList.append(instance)
                    else:
                        raise RuntimeError('EC2 JSON format has likely changed.  '
                                           'Duplicate instances found.')
    return ec2InstanceList


def fetchE2Cost(productID, priceDict):
    """
    Returns the current price of an EC2 instance in USD given an EC2 json dictionary.

    Requires a unique key hash identifier matching an instance type from
    the "products" section.

    Example:

      "terms" : { <----- Start of the entire Pricing Section.
    "OnDemand" : {
      "9ZEEN7WWWQKAG292" : {
        "9ZEEN7WWWQKAG292.JRTCKXETXF" : {
          "priceDimensions" : {
            "9ZEEN7WWWQKAG292.JRTCKXETXF.6YS6EN2CT7" : {
              "pricePerUnit" : {
                "USD" : "12.2400000000"},
              "unit": "Hrs"}},}}

    :param str k: Unique hash key paired to an instance type.  Example: "9ZEEN7WWWQKAG292"
    :param dict priceDict: A subsection of the EC2 JSON representing the pricing section ("terms").
    :return:
    """
    for thisProduct in priceDict:
        if productID == thisProduct:
            for k, v in priceDict[thisProduct].iteritems():
                # each verboseHashID has a different "unit" of price
                for verboseHashID in v['priceDimensions']:
                    # only fetch hourly rates
                    if v['priceDimensions'][verboseHashID]["unit"] == "Hrs":
                        return float(v['priceDimensions'][verboseHashID]['pricePerUnit']['USD'])
    raise RuntimeError('EC2 JSON format has likely changed.  No price found for this instance.')
