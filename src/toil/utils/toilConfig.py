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
"""Create a config file with all default Toil options."""
import logging
import os

from configargparse import ArgParser

from toil.common import generate_config
from toil.statsAndLogging import set_logging_from_options, add_logging_options

logger = logging.getLogger(__name__)


def main() -> None:
    parser = ArgParser()

    parser.add_argument("output", default="config.yaml", help="Filepath to write the config file too. Default=%(default)s")
    parser.add_argument("--include", default=None, choices=["cwl", "wdl", "CWL", "WDL"], help="Include CWL or WDL options in the config file. Set to "
                                                        "\"cwl\" to include CWL options and \"wdl\" to include WDL options. Default=%(default)s")

    add_logging_options(parser)
    options = parser.parse_args()
    set_logging_from_options(options)
    include = None if options.include is None else options.include.lower()
    if include == "cwl":
        s = "with Toil CWL options"
    elif include == "wdl":
        s = "with Toil WDL options"
    else:
        s = "with base Toil options"
    logger.debug(f"Attempting to write a default config file {s} to %s.", os.path.abspath(options.output))
    generate_config(os.path.abspath(options.output), include=include)
    logger.info(f"Successfully wrote a default config file {s} to %s.", os.path.abspath(options.output))

