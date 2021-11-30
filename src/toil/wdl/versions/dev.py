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

from wdlparse.dev.WdlLexer import FileStream, WdlLexer
from wdlparse.dev.WdlParser import CommonTokenStream, WdlParser

from toil.wdl.versions.v1 import AnalyzeV1WDL, is_context
from toil.wdl.wdl_types import WDLType

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

    def visit_document(self, ctx: WdlParser.DocumentContext) -> None:
        """
        Similar to version 1.0, except the 'workflow' element is included in
        `ctx.document_element()`.
        """
        for element in ctx.document_element():
            self.visit_document_element(element)

    def visit_document_element(self, ctx: WdlParser.Document_elementContext) -> None:
        """
        Similar to version 1.0, except this also contains 'workflow'.
        """
        element = ctx.children[0]
        if is_context(element, 'WorkflowContext'):
            self.visit_workflow(element)
        else:
            # let super take care of the rest.
            super().visit_document_element(ctx)

    def visit_call(self, ctx: WdlParser.CallContext) -> dict:
        """
        Similar to version 1.0, except `ctx.call_afters()` is added.
        """
        # TODO: implement call_afters
        # See: https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#call-statement
        return super().visit_call(ctx)

    def visit_string_expr_part(self, ctx: WdlParser.String_expr_partContext) -> str:
        """
        Similar to version 1.0, except `ctx.expression_placeholder_option()`
        is removed.
        """
        # expression placeholder options are removed in development
        # See: https://github.com/openwdl/wdl/blob/main/versions/development/parsers/antlr4/WdlParser.g4#L55

        return self.visit_expr(ctx.expr())

    def visit_wdl_type(self, ctx: WdlParser.Wdl_typeContext) -> WDLType:
        """
        Similar to version 1.0, except Directory type is added.
        """
        identifier = ctx.type_base().children[0]

        if identifier == 'Directory':
            # TODO: implement Directory type
            raise NotImplementedError('Directory type is not implemented.')
        else:
            # let super take care of the rest.
            return super().visit_wdl_type(ctx)

    def visit_expr_core(self, expr: WdlParser.Expr_coreContext) -> str:
        """
        Similar to version 1.0, except struct literal is added.
        """
        if is_context(expr, 'Struct_literalContext'):
            # TODO: implement struct literal
            raise NotImplementedError(f'WDL struct is not implemented.')
        else:
            # let super take care of the rest.
            return super().visit_expr_core(expr)
