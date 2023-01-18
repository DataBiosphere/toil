# Copyright (C) 2015-2021 Regents of the University of California
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
import datetime
import json
import logging
import os
import re
import textwrap
from typing import Any, Dict, List, Tuple, Union

import requests

logger = logging.getLogger(__name__)
dirname = os.path.dirname(__file__)


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


class InstanceType:
    __slots__ = ('name', 'cores', 'memory', 'disks', 'disk_capacity', 'architecture')

    def __init__(self, name: str, cores: int, memory: float, disks: float, disk_capacity: float, architecture: str):
        self.name = name  # the API name of the instance type
        self.cores = cores  # the number of cores
        self.memory = memory  # RAM in GiB
        self.disks = disks  # the number of ephemeral (aka 'instance store') volumes
        self.disk_capacity = disk_capacity  # the capacity of each ephemeral volume in GiB
        self.architecture = architecture # the architecture of the instance type. Can be either amd64 or arm64

    def __str__(self) -> str:
        return ("Type: {}\n"
                "Cores: {}\n"
                "Disks: {}\n"
                "Memory: {}\n"
                "Disk Capacity: {}\n"
                "Architecture: {}\n"
                "".format(
                self.name,
                self.cores,
                self.disks,
                self.memory,
                self.disk_capacity,
                self.architecture))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, InstanceType):
            return NotImplemented
        if (self.name == other.name and
            self.cores == other.cores and
            self.memory == other.memory and
            self.disks == other.disks and
            self.disk_capacity == other.disk_capacity and
            self.architecture == other.architecture):
            return True
        return False


def isNumber(s: str) -> bool:
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


def parseStorage(storageData: str) -> Union[List[int], Tuple[Union[int, float], float]]:
    """
    Parses EC2 JSON storage param string into a number.

    Examples:
        "2 x 160 SSD"
        "3 x 2000 HDD"
        "EBS only"
        "1 x 410"
        "8 x 1.9 NVMe SSD"
        "900 GB NVMe SSD"

    :param str storageData: EC2 JSON storage param string.
    :return: Two floats representing: (# of disks), and (disk_capacity in GiB of each disk).
    """
    if storageData == "EBS only":
        return [0, 0]
    else:
        specs = storageData.strip().split()
        if isNumber(specs[0]) and specs[1] == 'x' and isNumber(specs[2]):
            return float(specs[0].replace(',', '')), float(specs[2].replace(',', ''))
        elif isNumber(specs[0]) and specs[1] == 'GB' and specs[2] == 'NVMe' and specs[3] == 'SSD':
            return 1, float(specs[0].replace(',', ''))
        else:
            raise RuntimeError('EC2 JSON format has likely changed.  Error parsing disk specs.')


def parseMemory(memAttribute: str) -> float:
    """
    Returns EC2 'memory' string as a float.

    Format should always be '#' GiB (example: '244 GiB' or '1,952 GiB').
    Amazon loves to put commas in their numbers, so we have to accommodate that.
    If the syntax ever changes, this will raise.

    :param memAttribute: EC2 JSON memory param string.
    :return: A float representing memory in GiB.
    """
    mem = memAttribute.replace(',', '').split()
    if mem[1] == 'GiB':
        return float(mem[0])
    else:
        raise RuntimeError('EC2 JSON format has likely changed.  Error parsing memory.')


def fetchEC2Index(filename: str) -> None:
    """Downloads and writes the AWS Billing JSON to a file using the AWS pricing API.

    See: https://aws.amazon.com/blogs/aws/new-aws-price-list-api/

    :return: A dict of InstanceType objects, where the key is the string:
             aws instance name (example: 't2.micro'), and the value is an
             InstanceType object representing that aws instance name.
    """
    print('Downloading ~1Gb AWS billing file to parse for information.\n')

    response = requests.get('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/index.json')
    if response.ok:
        with open(filename, 'w') as f:
            f.write(str(json.dumps(json.loads(response.text), indent=4)))
            print('Download completed successfully!\n')
    else:
        raise RuntimeError('Error: ' + str(response) + ' :: ' + str(response.text))


def fetchEC2InstanceDict(awsBillingJson: Dict[str, Any], region: str) -> Dict[str, InstanceType]:
    """
    Takes a JSON and returns a list of InstanceType objects representing EC2 instance params.

    :param region:
    :return:
    """
    ec2InstanceList = []
    for k, v in awsBillingJson['products'].items():
        i = v['attributes']
        # NOTES:
        #
        # 3 tenant types: 'Host' (always $0.00; just a template?)
        #                 'Dedicated' (toil does not support; these are pricier)
        #                 'Shared' (AWS default and what toil uses)
        #
        # The same instance can appear with multiple "operation" values;
        # "RunInstances" is normal
        # "RunInstances:<code>" is e.g. Linux with MS SQL Server installed.
        if (i.get('location') == region and
            i.get('tenancy') == 'Shared' and
            i.get('operatingSystem') == 'Linux' and
            i.get('operation') == 'RunInstances'):

            normal_use = i.get('usagetype').endswith('BoxUsage:' + i['instanceType'])  # not reserved or unused
            if normal_use:
                disks, disk_capacity = parseStorage(v["attributes"]["storage"])

                # Determines whether the instance type is from an ARM or AMD family
                # ARM instance names include a digit followed by a 'g' before the instance size
                architecture = 'arm64' if re.search(r".*\dg.*\..*", i["instanceType"]) else 'amd64'

                instance = InstanceType(name=i["instanceType"],
                                        cores=i["vcpu"],
                                        memory=parseMemory(i["memory"]),
                                        disks=disks,
                                        disk_capacity=disk_capacity,
                                        architecture=architecture)
                if instance in ec2InstanceList:
                    raise RuntimeError('EC2 JSON format has likely changed.  '
                                       'Duplicate instance {} found.'.format(instance))
                ec2InstanceList.append(instance)
    print('Finished for ' + str(region) + '.  ' + str(len(ec2InstanceList)) + ' added.')
    return {_.name: _ for _ in ec2InstanceList}


def updateStaticEC2Instances() -> None:
    """
    Generates a new python file of fetchable EC2 Instances by region with current prices and specs.

    Takes a few (~3+) minutes to run (you'll need decent internet).

    :return: Nothing.  Writes a new 'generatedEC2Lists.py' file.
    """
    print("Updating Toil's EC2 lists to the most current version from AWS's bulk API.\n"
          "This may take a while, depending on your internet connection (~1Gb file).\n")

    origFile = os.path.join(dirname, 'generatedEC2Lists.py')  # original
    assert os.path.exists(origFile)
    # use a temporary file until all info is fetched
    genFile = os.path.join(dirname, 'generatedEC2Lists_tmp.py')  # temp
    if os.path.exists(genFile):
        os.remove(genFile)

    # filepath to store the aws json request (will be cleaned up)
    # this is done because AWS changes their json format from time to time
    # and debugging is faster with the file stored locally
    awsJsonIndex = os.path.join(dirname, 'index.json')

    if not os.path.exists(awsJsonIndex):
        fetchEC2Index(filename=awsJsonIndex)
    else:
        print('Reusing previously downloaded json @: ' + awsJsonIndex)

    with open(awsJsonIndex) as f:
        awsProductDict = json.loads(f.read())

    currentEC2List = []
    instancesByRegion: Dict[str, List[str]] = {}
    for regionNickname in EC2Regions:
        currentEC2Dict = fetchEC2InstanceDict(awsProductDict, region=EC2Regions[regionNickname])
        for instanceName, instanceTypeObj in currentEC2Dict.items():
            if instanceTypeObj not in currentEC2List:
                currentEC2List.append(instanceTypeObj)
            instancesByRegion.setdefault(regionNickname, []).append(instanceName)

    # write provenance note, copyright and imports
    with open(genFile, 'w') as f:
        f.write(textwrap.dedent('''
        # !!! AUTOGENERATED FILE !!!
        # Update with: src/toil/utils/toilUpdateEC2Instances.py
        #
        # Copyright (C) 2015-{year} UCSC Computational Genomics Lab
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
        from toil.lib.ec2nodes import InstanceType\n\n\n''').format(year=datetime.date.today().strftime("%Y"))[1:])

    # write header of total EC2 instance type list
    genString = "# {num} Instance Types.  Generated {date}.\n".format(
                num=str(len(currentEC2List)), date=str(datetime.datetime.now()))
    genString = genString + "E2Instances = {\n"
    sortedCurrentEC2List = sorted(currentEC2List, key=lambda x: x.name)

    # write the list of all instances types
    for i in sortedCurrentEC2List:
        z = "    '{name}': InstanceType(name='{name}', cores={cores}, memory={memory}, disks={disks}, disk_capacity={disk_capacity}, architecture='{architecture}')," \
            "\n".format(name=i.name, cores=i.cores, memory=i.memory, disks=i.disks, disk_capacity=i.disk_capacity, architecture=i.architecture)
        genString = genString + z
    genString = genString + '}\n\n'

    genString = genString + 'regionDict = {\n'
    for regionName, instanceList in instancesByRegion.items():
        genString = genString + f"              '{regionName}': ["
        for instance in sorted(instanceList):
            genString = genString + f"'{instance}', "
        if genString.endswith(', '):
            genString = genString[:-2]
        genString = genString + '],\n'
    if genString.endswith(',\n'):
        genString = genString[:-len(',\n')]
    genString = genString + '}\n'
    with open(genFile, 'a+') as f:
        f.write(genString)

    # append key for fetching at the end
    regionKey = '\nec2InstancesByRegion = {region: [E2Instances[i] for i in instances] for region, instances in regionDict.items()}\n'

    with open(genFile, 'a+') as f:
        f.write(regionKey)
    # delete the original file
    if os.path.exists(origFile):
        os.remove(origFile)
    # replace the instance list with a current list
    os.rename(genFile, origFile)
    # delete the aws billing json file
    if os.path.exists(awsJsonIndex):
        os.remove(awsJsonIndex)
