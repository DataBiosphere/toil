# Copyright (C) 2018-2021 UCSC Computational Genomics Lab
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

from toil.wdl.wdl_types import (WDLArrayType,
                                WDLBooleanType,
                                WDLFileType,
                                WDLFloatType,
                                WDLIntType,
                                WDLMapType,
                                WDLPairType,
                                WDLStringType)

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

        # unique iterator to add to declaration names
        self.declaration_number = 0

        # unique iterator to add to call names
        self.call_number = 0

        # unique iterator to add to scatter names
        self.scatter_number = 0

        # unique iterator to add to if names
        self.if_number = 0

    @property
    def version(self) -> str:
        """
        Returns the version of the WDL document as a string.
        """
        raise NotImplementedError

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

    primitive_types = {
        'String': WDLStringType,
        'Int': WDLIntType,
        'Float': WDLFloatType,
        'Boolean': WDLBooleanType,
        'File': WDLFileType
    }

    compound_types = {
        'Array': WDLArrayType,
        'Pair': WDLPairType,
        'Map': WDLMapType
    }

    def create_wdl_primitive_type(self, key: str, optional: bool = False):
        """
        Returns an instance of WDLType.
        """
        type_ = self.primitive_types.get(key)
        if type_:
            return type_(optional=optional)
        else:
            raise RuntimeError(f'Unsupported primitive type: {key}')

    def create_wdl_compound_type(self, key: str, elements: list, optional: bool = False):
        """
        Returns an instance of WDLCompoundType.
        """
        type_ = self.compound_types.get(key)
        if type_:
            return type_(*elements, optional=optional)
        else:
            raise RuntimeError(f'Unsupported compound type: {key}')
