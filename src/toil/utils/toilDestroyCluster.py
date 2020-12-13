# Copyright (C) 2015-2020 UCSC Computational Genomics Lab
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
"""Terminates the specified cluster and associated resources."""
from toil.common import parser_with_common_options
from toil.statsAndLogging import set_logging_from_options
from toil.provisioners import clusterFactory


def main():
    parser = parser_with_common_options(provisioner_options=True)
    options = parser.parse_args()
    set_logging_from_options(options)
    cluster = clusterFactory(provisioner=options.provisioner,
                             clusterName=options.clusterName,
                             zone=options.zone)
    cluster.destroyCluster()
