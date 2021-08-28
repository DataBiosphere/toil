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
import argparse
import logging

from toil.server.app import start_server
from toil.version import version

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="The Toil Workflow Execution Service Server.")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--swagger_ui", action="store_true", default=False)
    parser.add_argument("--opt", "-o", type=str, action="append",
                        help="Example: '--opt runner=cwltoil --opt extra=--logLevel=CRITICAL' "
                             "or '--opt extra=--workDir=/'.  Accepts multiple values.")
    parser.add_argument("--version", action='version', version=version)
    args = parser.parse_args()

    start_server(args)
