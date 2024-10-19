# Copyright (C) 2015-2024 Regents of the University of California
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
import shutil
import textwrap
from typing import Any, Union

import enlighten  # type: ignore
import requests

logger = logging.getLogger(__name__)
manager = enlighten.get_manager()
dirname = os.path.dirname(__file__)
region_json_dirname = os.path.join(dirname, "region_jsons")


EC2Regions = {
    "us-west-1": "US West (N. California)",
    "us-west-2": "US West (Oregon)",
    "us-east-1": "US East (N. Virginia)",
    "us-east-2": "US East (Ohio)",
    "us-gov-west-1": "AWS GovCloud (US)",
    "ca-central-1": "Canada (Central)",
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka-Local)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-south-1": "Asia Pacific (Mumbai)",
    "eu-west-1": "EU (Ireland)",
    "eu-west-2": "EU (London)",
    "eu-west-3": "EU (Paris)",
    "eu-central-1": "EU (Frankfurt)",
    "sa-east-1": "South America (Sao Paulo)",
}


class InstanceType:
    __slots__ = ("name", "cores", "memory", "disks", "disk_capacity", "architecture")

    def __init__(
        self,
        name: str,
        cores: int,
        memory: float,
        disks: float,
        disk_capacity: float,
        architecture: str,
    ):
        self.name = name  # the API name of the instance type
        self.cores = cores  # the number of cores
        self.memory = memory  # RAM in GiB
        self.disks = disks  # the number of ephemeral (aka 'instance store') volumes
        self.disk_capacity = (
            disk_capacity  # the capacity of each ephemeral volume in GiB
        )
        self.architecture = architecture  # the architecture of the instance type. Can be either amd64 or arm64

    def __str__(self) -> str:
        return (
            "Type: {}\n"
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
                self.architecture,
            )
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, InstanceType):
            return NotImplemented
        if (
            self.name == other.name
            and self.cores == other.cores
            and self.memory == other.memory
            and self.disks == other.disks
            and self.disk_capacity == other.disk_capacity
            and self.architecture == other.architecture
        ):
            return True
        return False


def is_number(s: str) -> bool:
    """
    Determines if a unicode string (that may include commas) is a number.

    :param s: Any unicode string.
    :return: True if s represents a number, False otherwise.
    """
    s = s.replace(",", "")
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


def parse_storage(
    storage_info: str,
) -> Union[list[int], tuple[Union[int, float], float]]:
    """
    Parses EC2 JSON storage param string into a number.

    Examples:
        "2 x 160 SSD"
        "3 x 2000 HDD"
        "EBS only"
        "1 x 410"
        "8 x 1.9 NVMe SSD"
        "900 GB NVMe SSD"

    :param str storage_info: EC2 JSON storage param string.
    :return: Two floats representing: (# of disks), and (disk_capacity in GiB of each disk).
    """
    if storage_info == "EBS only":
        return [0, 0]
    else:
        specs = storage_info.strip().split()
        if is_number(specs[0]) and specs[1] == "x" and is_number(specs[2]):
            return float(specs[0].replace(",", "")), float(specs[2].replace(",", ""))
        elif (
            is_number(specs[0])
            and specs[1] == "GB"
            and specs[2] == "NVMe"
            and specs[3] == "SSD"
        ):
            return 1, float(specs[0].replace(",", ""))
        else:
            raise RuntimeError(
                "EC2 JSON format has likely changed.  Error parsing disk specs."
            )


def parse_memory(mem_info: str) -> float:
    """
    Returns EC2 'memory' string as a float.

    Format should always be '#' GiB (example: '244 GiB' or '1,952 GiB').
    Amazon loves to put commas in their numbers, so we have to accommodate that.
    If the syntax ever changes, this will raise.

    :param mem_info: EC2 JSON memory param string.
    :return: A float representing memory in GiB.
    """
    mem = mem_info.replace(",", "").split()
    if mem[1] == "GiB":
        return float(mem[0])
    else:
        raise RuntimeError("EC2 JSON format has likely changed.  Error parsing memory.")


def download_region_json(filename: str, region: str = "us-east-1") -> None:
    """
    Downloads and writes the AWS Billing JSON to a file using the AWS pricing API.

    See: https://aws.amazon.com/blogs/aws/new-aws-price-list-api/

    :return: A dict of InstanceType objects, where the key is the string:
             aws instance name (example: 't2.micro'), and the value is an
             InstanceType object representing that aws instance name.
    """
    response = requests.get(
        f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/{region}/index.json",
        stream=True,
    )
    file_size = int(response.headers.get("content-length", 0))
    print(
        f"Downloading ~{file_size / 1000000000}Gb {region} AWS billing file to: {filename}"
    )

    with manager.counter(
        total=file_size, desc=os.path.basename(filename), unit="bytes", leave=False
    ) as progress_bar:
        with open(filename, "wb") as file:
            for data in response.iter_content(1048576):
                progress_bar.update(len(data))
                file.write(data)


def reduce_region_json_size(filename: str) -> list[dict[str, Any]]:
    """
    Deletes information in the json file that we don't need, and rewrites it.  This makes the file smaller.

    The reason being: we used to download the unified AWS Bulk API JSON, which eventually crept up to 5.6Gb,
    the loading of which could not be done on a 32Gb RAM machine.  Now we download each region JSON individually
    (with AWS's new Query API), but even those may eventually one day grow ridiculously large, so we do what we can to
    keep the file sizes down (and thus also the amount loaded into memory) to keep this script working for longer.
    """
    with open(filename) as f:
        aws_products = json.loads(f.read())["products"]
    aws_product_list = list()
    for k in list(aws_products.keys()):
        ec2_attributes = aws_products[k]["attributes"]
        if (
            ec2_attributes.get("tenancy") == "Shared"
            and ec2_attributes.get("operatingSystem") == "Linux"
            and ec2_attributes.get("operation") == "RunInstances"
            and ec2_attributes.get("usagetype").endswith(
                "BoxUsage:" + ec2_attributes["instanceType"]
            )
        ):
            aws_product_list.append(
                dict(
                    disk=ec2_attributes["storage"],
                    loc=ec2_attributes["location"],
                    name=ec2_attributes["instanceType"],
                    mem=ec2_attributes["memory"],
                    cpu=ec2_attributes["vcpu"],
                )
            )
        del aws_products[k]
    del aws_products
    with open(filename, "w") as f:
        f.write(json.dumps(dict(aws=aws_product_list), indent=2))
    return aws_product_list


def updateStaticEC2Instances() -> None:
    """
    Generates a new python file of fetchable EC2 Instances by region with current prices and specs.

    Takes a few (~3+) minutes to run (you'll need decent internet).

    :return: Nothing.  Writes a new 'generatedEC2Lists.py' file.
    """
    print(
        "Updating Toil's EC2 lists to the most current version from AWS's bulk API.\n"
        "This may take a while, depending on your internet connection.\n"
    )

    original_aws_instance_list = os.path.join(
        dirname, "generatedEC2Lists.py"
    )  # original
    if not os.path.exists(original_aws_instance_list):
        raise RuntimeError(f"Path {original_aws_instance_list} does not exist.")
    # use a temporary file until all info is fetched
    updated_aws_instance_list = os.path.join(
        dirname, "generatedEC2Lists_tmp.py"
    )  # temp
    if os.path.exists(updated_aws_instance_list):
        os.remove(updated_aws_instance_list)

    if not os.path.exists(region_json_dirname):
        os.mkdir(region_json_dirname)

    currentEC2List = []
    instancesByRegion: dict[str, list[str]] = {}
    for region in EC2Regions.keys():
        region_json = os.path.join(region_json_dirname, f"{region}.json")

        if os.path.exists(region_json):
            try:
                with open(region_json) as f:
                    aws_products = json.loads(f.read())["aws"]
                print(f"Reusing previously downloaded json @: {region_json}")
            except:
                os.remove(region_json)
                download_region_json(filename=region_json, region=region)
                aws_products = reduce_region_json_size(filename=region_json)
        else:
            download_region_json(filename=region_json, region=region)
            aws_products = reduce_region_json_size(filename=region_json)

        ec2InstanceList = []
        for i in aws_products:
            disks, disk_capacity = parse_storage(i["disk"])
            # Determines whether the instance type is from an ARM or AMD family
            # ARM instance names include a digit followed by a 'g' before the instance size
            architecture = "arm64" if re.search(r".*\dg.*\..*", i["name"]) else "amd64"
            ec2InstanceList.append(
                InstanceType(
                    name=i["name"],
                    cores=i["cpu"],
                    memory=parse_memory(i["mem"]),
                    disks=disks,
                    disk_capacity=disk_capacity,
                    architecture=architecture,
                )
            )
        print(
            "Finished for "
            + str(region)
            + ".  "
            + str(len(ec2InstanceList))
            + " added.\n"
        )
        currentEC2Dict = {_.name: _ for _ in ec2InstanceList}
        for instanceName, instanceTypeObj in currentEC2Dict.items():
            if instanceTypeObj not in currentEC2List:
                currentEC2List.append(instanceTypeObj)
            instancesByRegion.setdefault(region, []).append(instanceName)

    # write provenance note, copyright and imports
    with open(updated_aws_instance_list, "w") as f:
        f.write(
            textwrap.dedent(
                """
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
        from toil.lib.ec2nodes import InstanceType\n\n\n"""
            ).format(year=datetime.date.today().strftime("%Y"))[1:]
        )

    # write header of total EC2 instance type list
    genString = f"# {len(currentEC2List)} Instance Types.  Generated {datetime.datetime.now()}.\n"
    genString = genString + "E2Instances = {\n"
    sortedCurrentEC2List = sorted(currentEC2List, key=lambda x: x.name)

    # write the list of all instances types
    for i in sortedCurrentEC2List:
        genString = (
            genString
            + f"    '{i.name}': InstanceType(name='{i.name}', cores={i.cores}, memory={i.memory}, disks={i.disks}, disk_capacity={i.disk_capacity}, architecture='{i.architecture}'),\n"
        )
    genString = genString + "}\n\n"

    genString = genString + "regionDict = {\n"
    for regionName, instanceList in instancesByRegion.items():
        genString = genString + f"              '{regionName}': ["
        for instance in sorted(instanceList):
            genString = genString + f"'{instance}', "
        if genString.endswith(", "):
            genString = genString[:-2]
        genString = genString + "],\n"
    if genString.endswith(",\n"):
        genString = genString[: -len(",\n")]
    genString = genString + "}\n"
    with open(updated_aws_instance_list, "a+") as f:
        f.write(genString)

    # append key for fetching at the end
    regionKey = "\nec2InstancesByRegion = {region: [E2Instances[i] for i in instances] for region, instances in regionDict.items()}\n"

    with open(updated_aws_instance_list, "a+") as f:
        f.write(regionKey)

    # replace the instance list with a current list
    os.rename(updated_aws_instance_list, original_aws_instance_list)

    # delete the aws region json file directory
    if os.path.exists(region_json_dirname):
        print(
            f"Update Successful!  Removing AWS Region JSON Files @: {region_json_dirname}"
        )
        shutil.rmtree(region_json_dirname)
