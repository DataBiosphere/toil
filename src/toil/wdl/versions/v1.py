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
from collections import OrderedDict
from typing import Union

from wdlparse.v1.WdlV1Lexer import WdlV1Lexer, FileStream
from wdlparse.v1.WdlV1Parser import WdlV1Parser, CommonTokenStream
from wdlparse.v1.WdlV1ParserVisitor import WdlV1ParserVisitor

from toil.wdl.wdl_analysis import AnalyzeWDL

logger = logging.getLogger(__name__)


def is_context(ctx, classname: Union[str, tuple]):
    """
    Checks if the input context object is an instance of the string classname.
    """
    if isinstance(classname, str):
        return ctx.__class__.__name__ == classname
    return ctx.__class__.__name__ in classname


class AnalyzeV1WDL(AnalyzeWDL, WdlV1ParserVisitor):
    """
    AnalyzeWDL implementation for the 1.0 version.
    """

    @property
    def version(self) -> str:
        return '1.0'

    def analyze(self):
        """
        Analyzes the WDL file passed into the constructor and generates the two
        intermediate data structures: `self.workflows_dictionary` and
        `self.tasks_dictionary`.
        """
        lexer = WdlV1Lexer(FileStream(self.wdl_file))
        parser = WdlV1Parser(input=CommonTokenStream(lexer))
        tree = parser.document()
        self.visit(tree)

    def visitDocument(self, ctx):
        """
        Root of tree. Contains `version` followed by an optional workflow and
        any number of `document_element`s.
        """
        wf = ctx.workflow()
        if wf:
            self.visitWorkflow(wf)

        for element in ctx.document_element():
            self.visitDocument_element(element)

    def visitDocument_element(self, ctx):
        """
        Contains one of the following: 'import_doc', 'struct', or 'task'.
        """
        element = ctx.children[0]
        # task
        if is_context(element, 'TaskContext'):
            return self.visitTask(element)
        # struct
        elif is_context(element, 'StructContext'):
            # TODO: add support for structs.
            raise NotImplementedError('Struct is not supported.')
        # import_doc
        elif is_context(element, 'Import_docContext'):
            # TODO: add support for imports.
            raise NotImplementedError('Import other WDL files is not supported.')
        else:
            raise RuntimeError(f'Unrecognized document element in visitDocument(): {type(element)}')

    # Workflow section

    def visitWorkflow(self, ctx):
        """
        Contains an 'identifier' and an array of `workflow_element`s.
        """
        identifier = ctx.Identifier().getText()
        wf = self.workflows_dictionary.setdefault(identifier, OrderedDict())

        for element in ctx.workflow_element():
            section = element.children[0]
            # input
            if is_context(section, 'Workflow_inputContext'):
                # loop through all inputs and add to the workflow dictionary.
                # Treating this the same as workflow declarations for now
                for wf_input in self.visitWorkflow_input(section):
                    wf[f'declaration{self.declaration_number}'] = wf_input
                    self.declaration_number += 1
            # output
            elif is_context(section, 'Workflow_outputContext'):
                # TODO: add support for workflow level outputs in wdl_synthesis
                wf['wf_outputs'] = self.visitWorkflow_output(section)
            # inner_element
            # i.e.: non-input declarations, scatters, calls, and conditionals
            elif is_context(section, 'Inner_workflow_elementContext'):
                wf_key, contents = self.visitInner_workflow_element(section)
                wf[wf_key] = contents
            # parameter_meta and meta
            elif is_context(section, ('Parameter_meta_elementContext', 'Meta_elementContext')):
                pass
            else:
                raise RuntimeError(f'Unrecognized workflow element in visitWorkflow(): {type(section)}')

    def visitWorkflow_input(self, ctx):
        """
        Contains an array of 'any_decls', which can be unbounded or bounded declarations.
        Example:
            input {
              String in_str = "twenty"
              Int in_int
            }

        Returns a list of tuple=(name, type, expr).
        """
        return [self.visitAny_decls(decl) for decl in ctx.any_decls()]

    def visitWorkflow_output(self, ctx):
        """
        Contains an array of 'bound_decls' (unbound_decls not allowed).
        Example:
            output {
              String out_str = "output"
            }

        Returns a list of tuple=(name, type, expr).
        """
        return [self.visitBound_decls(decl) for decl in ctx.bound_decls()]

    def visitInner_workflow_element(self, ctx):
        """
        Returns a tuple=(unique_key, dict), where dict contains the contents of
        the given inner workflow element.
        """
        element = ctx.children[0]

        # bound_decls
        # i.e.: declarations declared outside of input section
        if is_context(element, 'Bound_declsContext'):
            key = f'declaration{self.declaration_number}'
            self.declaration_number += 1
            return key, self.visitBound_decls(element)
        # call
        elif is_context(element, 'CallContext'):
            key = f'call{self.call_number}'
            self.call_number += 1
            return key, self.visitCall(element)
        # scatter
        elif is_context(element, 'ScatterContext'):
            key = f'scatter{self.scatter_number}'
            self.scatter_number += 1
            return key, self.visitScatter(element)
        # conditional
        elif is_context(element, 'ConditionalContext'):
            key = f'if{self.if_number}'
            self.if_number += 1
            return key, self.visitConditional(element)
        else:
            raise RuntimeError(f'Unrecognized workflow element in visitInner_workflow_element(): {type(element)}')

    def visitCall(self, ctx):
        """
        Pattern: CALL call_name call_alias? call_body?
        Example WDL syntax: call task_1 {input: arr=arr}

        Returns a dict={task, alias, io}.
        """
        name = '.'.join(identifier.getText() for identifier in ctx.call_name().Identifier())
        alias = ctx.call_alias().Identifier().getText() if ctx.call_alias() else name

        body = OrderedDict({  # kvp generator
            input_.Identifier().getText(): self.visitExpr(input_.expr())
            for input_ in ctx.call_body().call_inputs().call_input()

            # check if {} and {input: ...} are provided
        }) if ctx.call_body() and ctx.call_body().call_inputs() else OrderedDict()

        return {
            'task': name,
            'alias': alias,
            'io': body
        }

    def visitScatter(self, ctx):
        """
        Pattern: SCATTER LPAREN Identifier In expr RPAREN LBRACE inner_workflow_element* RBRACE
        Example WDL syntax: scatter ( i in items) { ... }

        Returns a dict={item, collection, body}.
        """
        item = ctx.Identifier().getText()
        expr = self.visitExpr(ctx.expr())
        body = OrderedDict()
        for element in ctx.inner_workflow_element():
            body_key, contents = self.visitInner_workflow_element(element)
            body[body_key] = contents
        return {
            'item': item,
            'collection': expr,
            'body': body
        }

    def visitConditional(self, ctx):
        """
        Pattern: IF LPAREN expr RPAREN LBRACE inner_workflow_element* RBRACE
        Example WDL syntax: if (condition) { ... }

        Returns a dict={expression, body}.
        """
        # see https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#conditionals

        expr = self.visitExpr(ctx.expr())

        body = OrderedDict()
        for element in ctx.inner_workflow_element():
            body_key, contents = self.visitInner_workflow_element(element)
            body[body_key] = contents

        return {
            'expression': expr,
            'body': body
        }

    # Task section

    def visitTask(self, ctx):
        """
        Root of a task definition. Contains an `identifier` and an array of
        `task_element`s.
        """
        identifier = ctx.Identifier().getText()
        task = self.tasks_dictionary.setdefault(identifier, OrderedDict())
        # print(f'Visiting task: {identifier}')

        for element in ctx.task_element():
            section = element.children[0]
            # input
            if is_context(section, 'Task_inputContext'):
                # treating this the same as task declarations for now
                task.setdefault('inputs', []).extend(self.visitTask_input(section))
            # output
            elif is_context(section, 'Task_outputContext'):
                task['outputs'] = self.visitTask_output(section)
            # command
            elif is_context(section, 'Task_commandContext'):
                task['raw_commandline'] = self.visitTask_command(section)
            # runtime
            elif is_context(section, 'Task_runtimeContext'):
                task['runtime'] = self.visitTask_runtime(section)
            # bound_decls
            elif is_context(section, 'Bound_declsContext'):
                decl = self.visitBound_decls(section)
                task.setdefault('inputs', []).append(decl)
            # parameter_meta, and meta
            elif is_context(section, ('Parameter_meta_elementContext', 'Meta_elementContext')):
                pass
            else:
                raise RuntimeError(f'Unrecognized task element in visitTask(): {type(section)}')

    def visitTask_input(self, ctx):
        """
        Contains an array of 'any_decls', which can be unbounded or bounded declarations.
        Example:
            input {
              String in_str = "twenty"
              Int in_int
            }

        Returns a list of tuple=(name, type, expr)
        """
        return [self.visitAny_decls(decl) for decl in ctx.any_decls()]

    def visitTask_output(self, ctx):
        """
        Contains an array of 'bound_decls' (unbound_decls not allowed).
        Example:
            output {
              String out_str = read_string(stdout())
            }

        Returns a list of tuple=(name, type, expr)
        """
        return [self.visitBound_decls(decl) for decl in ctx.bound_decls()]

    def visitTask_command(self, ctx):
        """
        Parses the command section of the WDL task.

        Contains a `string_part` plus any number of `expr_with_string`s.
        The following example command:
            'echo ${var1} ${var2} > output_file.txt'
        Has 3 parts:
                1. string_part: 'echo '
                2. expr_with_string, which has two parts:
                        - expr_part: 'var1'
                        - string_part: ' '
                1. expr_with_string, which has two parts:
                        - expr_part: 'var2'
                        - string_part: ' > output_file.txt'

        Returns a list=[] of strings representing the parts of the command.
            e.g. [string_part, expr_part, string_part, ...]
        """
        parts = []

        # add the first part
        str_part = self.visitTask_command_string_part(ctx.task_command_string_part())
        if str_part:
            parts.append(f"r'''{str_part}'''")

        # add the rest
        for group in ctx.task_command_expr_with_string():
            expr_part, str_part = self.visitTask_command_expr_with_string(group)
            parts.append(expr_part)
            if str_part:
                parts.append(f"r'''{str_part}'''")

        return parts

    def visitTask_command_string_part(self, ctx):
        """
        Returns a string representing the string_part.
        """
        # join here because a string that contains $, {, or } is split
        return ''.join(part.getText() for part in ctx.CommandStringPart())

    def visitTask_command_expr_with_string(self, ctx):
        """
        Returns a tuple=(expr_part, string_part).
        """
        return (self.visitTask_command_expr_part(ctx.task_command_expr_part()),
                self.visitTask_command_string_part(ctx.task_command_string_part()))

    def visitTask_command_expr_part(self, ctx):
        """
        Contains the expression inside ${expr}.

        Returns the expression.
        """

        # [!!] TODO: expression_placeholder_option
        # https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#expression-placeholder-options

        #  e.g.: ${sep=", " array_value}
        #  e.g.: ${true="--yes" false="--no" boolean_value}
        #  e.g.: ${default="foo" optional_value}

        return self.visitExpr(ctx.expr())

    def visitTask_runtime(self, ctx):
        """
        Contains an array of `task_runtime_kv`s.

        Returns a dict={key: value} where key can be 'docker', 'cpu', 'memory',
        'cores', or 'disks'.
        """
        return OrderedDict((kv.children[0].getText(),  # runtime key
                            self.visitExpr(kv.expr()))  # runtime value
                           for kv in ctx.task_runtime_kv())

    # Shared

    def visitUnbound_decls(self, ctx):
        """
        Contains an unbounded input declaration. E.g.: `String in_str`.

        Returns a tuple=(name, type, expr), where `expr` is None.
        """
        name = ctx.Identifier().getText()
        type_ = self.visitWdl_type(ctx.wdl_type())
        return name, type_, None

    def visitBound_decls(self, ctx):
        """
        Contains a bounded input declaration. E.g.: `String in_str = "some string"`.

        Returns a tuple=(name, type, expr).
        """
        name = ctx.Identifier().getText()
        type_ = self.visitWdl_type(ctx.wdl_type())
        expr = self.visitExpr(ctx.expr())

        return name, type_, expr

    def visitWdl_type(self, ctx):
        """
        Returns a WDLType instance.
        """
        identifier = ctx.type_base().children[0]
        optional = ctx.OPTIONAL() is not None

        # TODO: OBJECT type

        # primitives
        if is_context(identifier, 'TerminalNodeImpl'):
            return self.create_wdl_primitive_type(key=identifier.getText(), optional=optional)
        # compound types
        else:
            name = identifier.children[0].getText()  # the first child is the name of the type.
            type_ = identifier.wdl_type()
            if isinstance(type_, list):
                elements = [self.visitWdl_type(element) for element in type_]
            else:
                elements = [self.visitWdl_type(type_)]
            return self.create_wdl_compound_type(key=name, elements=elements, optional=optional)

    def visitPrimitive_literal(self, ctx):
        """
        Returns the primitive literal as a string.
        """
        is_bool = ctx.BoolLiteral()
        if is_bool:
            val = is_bool.getText()
            if val not in ('true', 'false'):
                raise TypeError(f'Parsed boolean ({val}) must be expressed as "true" or "false".')
            return val.capitalize()
        elif is_context(ctx.children[0], ('TerminalNodeImpl',  # this also includes variables
                                          'StringContext', 'NumberContext')):
            return ctx.children[0].getText()
        else:
            raise RuntimeError(f'Primitive literal has unknown child: {type(ctx.children[0])}.')

    def visitInfix0(self, ctx):
        """
        Expression infix0 (LOR).
        """
        infix = ctx.expr_infix0()
        if is_context(infix, 'LorContext'):
            return self.visitLor(infix)
        return self.visitInfix1(infix)

    def visitLor(self, ctx):
        """
        Logical OR expression.
        """
        lhs = self.visitInfix0(ctx)
        rhs = self.visitInfix1(ctx)
        return f'{lhs} or {rhs}'

    def visitInfix1(self, ctx):
        """
        Expression infix1 (LAND).
        """
        infix = ctx.expr_infix1()
        if is_context(infix, 'LandContext'):
            return self.visitLand(infix)
        return self.visitInfix2(infix)

    def visitLand(self, ctx):
        """
        Logical AND expresion.
        """
        lhs = self.visitInfix1(ctx)
        rhs = self.visitInfix2(ctx)
        return f'{lhs} and {rhs}'

    def visitInfix2(self, ctx):
        """
        Expression infix2 (comparisons).
        """
        infix = ctx.expr_infix2()
        if is_context(infix, 'EqeqContext'):
            return self._visitInfix2(infix, '==')
        elif is_context(infix, 'NeqContext'):
            return self._visitInfix2(infix, '!=')
        elif is_context(infix, 'LteContext'):
            return self._visitInfix2(infix, '<=')
        elif is_context(infix, 'GteContext'):
            return self._visitInfix2(infix, '>=')
        elif is_context(infix, 'LtContext'):
            return self._visitInfix2(infix, '<')
        elif is_context(infix, 'GtContext'):
            return self._visitInfix2(infix, '>')
        # continue down our path
        return self.visitInfix3(infix)

    def _visitInfix2(self, ctx, operation: str):
        """
        :param operation: Operation as a string.
        """
        lhs = self.visitInfix2(ctx)
        rhs = self.visitInfix3(ctx)
        return f'{lhs} {operation} {rhs}'

    def visitInfix3(self, ctx):
        """
        Expression infix3 (add/subtract).
        """
        infix = ctx.expr_infix3()
        if is_context(infix, 'AddContext'):
            return self._visitInfix3(infix, '+')
        elif is_context(infix, 'SubContext'):
            return self._visitInfix3(infix, '-')
        # continue down our path
        return self.visitInfix4(infix)

    def _visitInfix3(self, ctx, operation: str):
        """
        :param operation: Operation as a string.
        """
        lhs = self.visitInfix3(ctx)
        rhs = self.visitInfix4(ctx)
        return f'{lhs} {operation} {rhs}'

    def visitInfix4(self, ctx):
        """
        Expression infix4 (multiply/divide/modulo).
        """
        infix = ctx.expr_infix4()
        if is_context(infix, 'MulContext'):
            return self._visitInfix4(infix, '*')
        elif is_context(infix, 'DivideContext'):
            return self._visitInfix4(infix, '/')
        elif is_context(infix, 'ModContext'):
            return self._visitInfix4(infix, '%')
        # continue down our path
        return self.visitInfix5(infix)

    def _visitInfix4(self, ctx, operation: str):
        """
        :param operation: Operation as a string.
        """
        lhs = self.visitInfix4(ctx)
        rhs = self.visitInfix5(ctx)
        return f'{lhs} {operation} {rhs}'

    # expr_core
    # see: https://github.com/w-gao/wdl/blob/main/versions/development/parsers/antlr4/WdlParser.g4#L121

    def visitApply(self, ctx):
        """
        A function call.
        Pattern: Identifier LPAREN (expr (COMMA expr)*)? RPAREN
        """
        pass

    def visitArray_literal(self, ctx):
        """
        Pattern: LBRACK (expr (COMMA expr)*)* RBRACK
        """
        return f"[{', '.join(self.visitExpr(expr) for expr in ctx.expr())}]"

    def visitPair_literal(self, ctx):
        """
        Pattern: LPAREN expr COMMA expr RPAREN
        """
        return f"({self.visitExpr(ctx.expr(0))}, {self.visitExpr(ctx.expr(1))})"

    def visitMap_literal(self, ctx):
        """
        Pattern: LBRACE (expr COLON expr (COMMA expr COLON expr)*)* RBRACE
        """
        # return f"{{{', '.join()}}}"
        pass

    # [!!] TODO: visitObject_literal

    def visitIfthenelse(self, ctx):
        """
        Ternary expression.
        Pattern: IF expr THEN expr ELSE expr
        """
        if_true = self.visitExpr(ctx.expr(0))
        condition = self.visitExpr(ctx.expr(1))
        if_false = self.visitExpr(ctx.expr(2))

        # this should also work without parenthesis
        return f'({condition} if {if_true} else {if_false})'

    def visitExpression_group(self, ctx):
        """
        Pattern: LPAREN expr RPAREN
        """
        return f'({self.visitExpr(ctx.expr())})'

    def visitAt(self, ctx):
        """
        Array or map lookup.
        Pattern: expr_core LBRACK expr RBRACK
        """
        pass

    def visitGet_name(self, ctx):
        """
        Member access.
        Pattern: expr_core DOT Identifier
        """
        pass

    def visitNegate(self, ctx):
        """
        Pattern: NOT expr
        """
        pass

    def visitUnarysigned(self, ctx):
        """
        Pattern: (PLUS | MINUS) expr
        """
        pass
