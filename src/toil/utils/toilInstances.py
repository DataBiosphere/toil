# Copyright (C) 2015-2016 Regents of the University of California
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
"""Displays information about instances opened with Toil."""

import logging
import os
from argparse import ArgumentParser
from toil.lib.bioio import addLoggingOptions

log = logging.getLogger(__name__)

def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--provisioners', dest="provisioners", choices=['aws', 'gce', 'azure'], nargs='*',
                        help='Provisioners from which instances will be displayed.')
    parser.add_argument('-n', '--name', dest='name', nargs='*', help='The name of the instance to be displayed.') # TODO allow unix-like matching of expressions: ab* matches abcd and abc
    parser.add_argument('-d', '--date', dest='date', help='The date the instance was created. (YYYY-MM-DD format)')

    reqs = dict()
    for k, v in vars(parser.parse_args()).items():
        reqs[k] = v

    indecies = {'name': 0,
                'provisioners': 1,
                'zone': 2,
                'instanceType': 3,
                'date': 4,
                'time': 5}

    if os.path.exists('/tmp/toilClusterList.txt'):
        with open('/tmp/toilClusterList.txt', 'r') as f:
            matches = []
            for line in f:
                parts = line.split('\t')
                match = True
                for req, value in reqs.items():
                    if value and parts[indecies[req]] not in value:
                        match = False
                        break
                if match:
                    matches.append(line.rstrip('\n'))

        if matches:
            print('NAME\tPROVISIONER\tZONE\tTYPE\tDATE\tTIME')
            for entry in matches:
                print(entry)
        else:
            print('No tracked instances match your request.')
    else:
        log.debug('/tmp/toilClusterList.txt does not exist')
        print('Toil is not tracking any instances.')
