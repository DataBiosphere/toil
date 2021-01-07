# Copyright (C) 2018-2020 UCSC Computational Genomics Lab
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
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)


class AnalyzeWDL:
    """
    An interface to analyze a WDL file. Each version corresponds to a subclass that
    restructures the WDL document into 2 intermediate data structures (python
    dictionaries):
        "workflows_dictionary": containing the parsed workflow information.
        "tasks_dictionary": containing the parsed task information.

    These are then fed into wdl_synthesis.py which uses them to write a native python
    script for use with Toil.

    Requires a WDL file.  The WDL file contains ordered commands.
    """
    def __init__(self, wdl_file: str):
        self.wdl_file = wdl_file

        # holds task skeletons from WDL task objects
        self.tasks_dictionary = OrderedDict()

        # holds workflow structure from WDL workflow objects
        self.workflows_dictionary = OrderedDict()

        # unique iterator to add to cmd names
        self.command_number = 0

        # unique iterator to add to call names
        self.call_number = 0

        # unique iterator to add to scatter names
        self.scatter_number = 0

        # unique iterator to add to if names
        self.if_number = 0

    def analyze(self):
        """
        Analyzes the WDL file passed into the constructor and generates the two
        intermediate data structures: `self.workflows_dictionary` and
        `self.tasks_dictionary`.

        :return: Returns nothing.
        """
        pass

    def write_AST(self, out_dir):
        """
        Writes a file with the AST for a wdl file in the out_dir.
        """
        pass
