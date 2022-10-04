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
"""CLI entry for the Toil servers."""
import logging
import sys

from toil.statsAndLogging import add_logging_options, set_logging_from_options

logger = logging.getLogger(__name__)


def main() -> None:
    try:
        from toil.server.app import parser_with_server_options, start_server
    except ImportError:
        logger.warning("The toil[server] extra is not installed.")
        sys.exit(1)

    parser = parser_with_server_options()
    add_logging_options(parser)
    args = parser.parse_args()
    set_logging_from_options(args)

    start_server(args)
