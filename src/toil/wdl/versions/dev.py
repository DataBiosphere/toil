# Copyright (C) 2020-2021 Regents of the University of California
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

from wdlparse.dev.WdlLexer import WdlLexer, FileStream
from wdlparse.dev.WdlParser import WdlParser, CommonTokenStream

from toil.wdl.versions.v1 import AnalyzeV1WDL, is_context

logger = logging.getLogger(__name__)


class AnalyzeDevelopmentWDL(AnalyzeV1WDL):  # extend from 1.0
    """
    AnalyzeWDL implementation for the development version using ANTLR4.

    See: https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md
         https://github.com/openwdl/wdl/blob/main/versions/development/parsers/antlr4/WdlParser.g4
    """

    @property
    def version(self) -> str:
        return 'development'

    def analyze(self):
        """
        Analyzes the WDL file passed into the constructor and generates the two
        intermediate data structures: `self.workflows_dictionary` and
        `self.tasks_dictionary`.
        """
        lexer = WdlLexer(FileStream(self.wdl_file))
        parser = WdlParser(input=CommonTokenStream(lexer))
        tree = parser.document()
        self.visit_document(tree)

    def visit_document(self, ctx):
        """
        Similar to version 1.0, except the 'workflow' element is included in
        `ctx.document_element()`.
        """
        for element in ctx.document_element():
            self.visit_document_element(element)

    def visit_document_element(self, ctx):
        """
        Similar to version 1.0, except this also contains 'workflow'.
        """
        element = ctx.children[0]

        # workflow
        if is_context(element, 'WorkflowContext'):
            return self.visit_workflow(element)
        else:
            # hand the rest to super.
            return super(AnalyzeDevelopmentWDL, self).visit_document_element(ctx)

    # TODO: implement the rest of the changes since WDL 1.0.
