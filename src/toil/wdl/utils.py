# Copyright (C) 2020-2021 UCSC Computational Genomics Lab
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
import json


def dict_from_JSON(JSON_file):
    """
    Takes a WDL-mapped json file and creates a dict containing the bindings.

    :param JSON_file: A required JSON file containing WDL variable bindings.
    """
    json_dict = {}

    # TODO: Add context support for variables within multiple wdl files

    with open(JSON_file) as data_file:
        data = json.load(data_file)
    for d in data:
        if isinstance(data[d], str):
            json_dict[d] = f'"{data[d]}"'
        else:
            json_dict[d] = data[d]
    return json_dict
