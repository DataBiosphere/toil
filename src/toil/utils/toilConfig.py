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
from toil.statsAndLogging import add_logging_options, set_logging_from_options

logger = logging.getLogger(__name__)


def main() -> None:
    parser = ArgParser()

    parser.add_argument(
        "output",
        default="config.yaml",
        help="Filepath to write the config file too. Default=%(" "default)s",
    )
    add_logging_options(parser)
    options = parser.parse_args()
    set_logging_from_options(options)
    logger.debug(
        "Attempting to write a default config file to %s.",
        os.path.abspath(options.output),
    )
    generate_config(os.path.abspath(options.output))
    logger.info(
        "Successfully wrote a default config file to %s.",
        os.path.abspath(options.output),
    )
