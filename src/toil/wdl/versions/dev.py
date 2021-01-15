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
from wdlparse.dev.WdlParserVisitor import WdlParserVisitor

from toil.wdl.versions.v1 import AnalyzeV1WDL, is_context

logger = logging.getLogger(__name__)


class AnalyzeDevelopmentWDL(AnalyzeV1WDL, WdlParserVisitor):  # extend from 1.0
    """
    AnalyzeWDL implementation for the development version.
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
        self.visitDocument(tree)

    def visitDocument(self, ctx):
        """
        Similar to version 1.0, except the 'workflow' element is included in
        `ctx.document_element()` instead of being `ctx.workflow()`.
        """
        for element in ctx.document_element():
            self.visitDocument_element(element)

    def visitDocument_element(self, ctx):
        """
        Similar to version 1.0, except this also contains 'workflow'.
        """
        element = ctx.children[0]

        # workflow
        if is_context(element, 'WorkflowContext'):
            return self.visitWorkflow(element)
        else:
            # hand the rest to super.
            return super(AnalyzeDevelopmentWDL, self).visitDocument_element(ctx)

    def visitCall(self, ctx):
        # TODO: call_afters is newly added. implement it here and in wdl_synthesis.

        return super(AnalyzeDevelopmentWDL, self).visitCall(ctx)

    # task hints
    # def visitTask(self, ctx):
    #     # Task_hintsContext

    # task_command_expr_part: removed expression_placeholder_option
    # string_expr_part:       removed expression_placeholder_option

    # task_runtime_kv: more specific with the keywords

    # primitive_literal: added NONE LITERAL

    # type_base: added DIRECTORY

    def visitStruct_literal(self, ctx):
        """
        Pattern: Identifier LBRACE (Identifier COLON expr (COMMA Identifier COLON expr)*)* RBRACE
        """
        raise NotImplementedError(f'Structs are not implemented yet :(')
