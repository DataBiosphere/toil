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
import pandas as pd

log = logging.getLogger(__name__)


def valsToList(reqs):
    """Turn the values of a dictionary into a list. (Assumes each value is a string or evaluates to False.)"""
    for k, v in reqs.items():
        if v:
            reqs[k] = reqs[k][0].split(',')
    return reqs

def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--provisioners', dest="provisioner", nargs='*', type=str,
                        help='Provisioners from which instances will be displayed.')
    parser.add_argument('-n', '--name', dest='name', nargs='*', type=str,
                        help='The name of the instance to be displayed.') # TODO allow unix-like matching of expressions: ab* matches abcd and abc
    parser.add_argument('-d', '--date', dest='date', nargs='*', type=str,
                        help='The date the instance was created. (YYYY-MM-DD format)')

    # Change 'arg1,arg2...argN' to ['arg1','arg2',...,'argN'] for easy filtering below.
    reqs = valsToList(vars(parser.parse_args()))

    # While this type of checking could be done utilizing the 'choices' argument of add_argument, it is being done
    # this way to maintain a CLI argument input style consistent with Toil.
    # (The cartesian product of 'aws', 'gce', 'azure' would have to be the value of the 'choices' argument.)
    supported_provisioners = ['aws', 'gce', 'azure']
    if reqs['provisioner']:
        for prov in reqs['provisioner']:
            if prov not in supported_provisioners:
                print('{} is not a Toil supported provisioner. ({})'.format(prov, supported_provisioners))
                reqs.remove(prov)

    if os.path.exists('/tmp/toilClusterList.csv'):
        df = pd.read_csv('/tmp/toilClusterList.csv')
        df.columns = ['name', 'provisioner', 'zone', 'type', 'date', 'time']

        for k, v in reqs.items():
            if v:
                df = df[df[k].isin(v)]  # Filter by requirement.

        if df.empty:
            print('No matching instances...')
        else:
            df.columns = [i.upper() for i in df.columns]
            # print(df.to_string(justify='right', col_space=20, index=False))
            # ^ This should be the line but there is a bug with pandas that makes the formatting look weird.
            print(df.to_string(justify='left', col_space=20, index=False))
    else:
        log.debug('/tmp/toilClusterList.csv does not exist')
        print('Toil is not tracking any instances.')
