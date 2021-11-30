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

from wdlparse.v1.WdlV1Lexer import FileStream, WdlV1Lexer
from wdlparse.v1.WdlV1Parser import CommonTokenStream, WdlV1Parser

from toil.wdl.wdl_analysis import AnalyzeWDL

logger = logging.getLogger(__name__)


def is_context(ctx, classname: Union[str, tuple]) -> bool:
    """
    Returns whether an ANTLR4 context object is of the precise type `classname`.

    :param ctx: An ANTLR4 context object.
    :param classname: The class name(s) as a string or a tuple of strings. If a
                      tuple is provided, this returns True if the context object
                      matches one of the class names.
    """
    # we check for `ctx.__class__.__name__` so that it's portable across multiple similar auto-generated parsers.
    if isinstance(classname, str):
        return ctx.__class__.__name__ == classname
    return ctx.__class__.__name__ in classname


class AnalyzeV1WDL(AnalyzeWDL):
    """
    AnalyzeWDL implementation for the 1.0 version using ANTLR4.

    See: https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md
         https://github.com/openwdl/wdl/blob/main/versions/1.0/parsers/antlr4/WdlV1Parser.g4
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
        self.visit_document(tree)

    def visit_document(self, ctx):
        """
        Root of tree. Contains `version` followed by an optional workflow and
        any number of `document_element`s.
        """
        wf = ctx.workflow()
        if wf:
            self.visit_workflow(wf)

        for element in ctx.document_element():
            self.visit_document_element(element)

    def visit_document_element(self, ctx):
        """
        Contains one of the following: 'import_doc', 'struct', or 'task'.
        """
        element = ctx.children[0]
        # task
        if is_context(element, 'TaskContext'):
            return self.visit_task(element)
        # struct
        elif is_context(element, 'StructContext'):
            # TODO: add support for structs.
            raise NotImplementedError('Struct is not supported.')
        # import_doc
        elif is_context(element, 'Import_docContext'):
            # TODO: add support for imports.
            raise NotImplementedError('Import is not supported.')
        else:
            raise RuntimeError(f'Unrecognized document element in visitDocument(): {type(element)}')

    # Workflow section

    def visit_workflow(self, ctx):
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
                # treating this the same as workflow declarations for now
                for wf_input in self.visit_workflow_input(section):
                    wf[f'declaration{self.declaration_number}'] = wf_input
                    self.declaration_number += 1
            # output
            elif is_context(section, 'Workflow_outputContext'):
                # TODO: add support for workflow level outputs in wdl_synthesis
                wf['wf_outputs'] = self.visit_workflow_output(section)
            # inner_element
            # i.e.: non-input declarations, scatters, calls, and conditionals
            elif is_context(section, 'Inner_workflow_elementContext'):
                wf_key, contents = self.visit_inner_workflow_element(section)
                wf[wf_key] = contents
            # parameter_meta and meta
            elif is_context(section, ('Parameter_meta_elementContext', 'Meta_elementContext')):
                # ignore additional metadata information for now.
                pass
            else:
                raise RuntimeError(f'Unrecognized workflow element in visitWorkflow(): {type(section)}')

    def visit_workflow_input(self, ctx):
        """
        Contains an array of 'any_decls', which can be unbound or bound declarations.
        Example:
            input {
              String in_str = "twenty"
              Int in_int
            }

        Returns a list of tuple=(name, type, expr).
        """
        return [self.visit_any_decls(decl) for decl in ctx.any_decls()]

    def visit_workflow_output(self, ctx):
        """
        Contains an array of 'bound_decls' (unbound_decls not allowed).
        Example:
            output {
              String out_str = "output"
            }

        Returns a list of tuple=(name, type, expr).
        """
        return [self.visit_bound_decls(decl) for decl in ctx.bound_decls()]

    def visit_inner_workflow_element(self, ctx):
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
            return key, self.visit_bound_decls(element)
        # call
        elif is_context(element, 'CallContext'):
            key = f'call{self.call_number}'
            self.call_number += 1
            return key, self.visit_call(element)
        # scatter
        elif is_context(element, 'ScatterContext'):
            key = f'scatter{self.scatter_number}'
            self.scatter_number += 1
            return key, self.visit_scatter(element)
        # conditional
        elif is_context(element, 'ConditionalContext'):
            key = f'if{self.if_number}'
            self.if_number += 1
            return key, self.visit_conditional(element)
        else:
            raise RuntimeError(f'Unrecognized workflow element in visitInner_workflow_element(): {type(element)}')

    def visit_call(self, ctx):
        """
        Pattern: CALL call_name call_alias? call_body?
        Example WDL syntax: call task_1 {input: arr=arr}

        Returns a dict={task, alias, io}.
        """
        name = '.'.join(identifier.getText() for identifier in ctx.call_name().Identifier())
        alias = ctx.call_alias().Identifier().getText() if ctx.call_alias() else name

        body = OrderedDict()
        # make sure that '{}' and '{input: ...}' are provided
        if ctx.call_body() and ctx.call_body().call_inputs():
            for input_ in ctx.call_body().call_inputs().call_input():
                body[input_.Identifier().getText()] = self.visit_expr(input_.expr())

        return {
            'task': name,
            'alias': alias,
            'io': body
        }

    def visit_scatter(self, ctx):
        """
        Pattern: SCATTER LPAREN Identifier In expr RPAREN LBRACE inner_workflow_element* RBRACE
        Example WDL syntax: scatter ( i in items) { ... }

        Returns a dict={item, collection, body}.
        """
        item = ctx.Identifier().getText()
        expr = self.visit_expr(ctx.expr())
        body = OrderedDict()
        for element in ctx.inner_workflow_element():
            body_key, contents = self.visit_inner_workflow_element(element)
            body[body_key] = contents
        return {
            'item': item,
            'collection': expr,
            'body': body
        }

    def visit_conditional(self, ctx):
        """
        Pattern: IF LPAREN expr RPAREN LBRACE inner_workflow_element* RBRACE
        Example WDL syntax: if (condition) { ... }

        Returns a dict={expression, body}.
        """
        # see https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#conditionals
        expr = self.visit_expr(ctx.expr())

        body = OrderedDict()
        for element in ctx.inner_workflow_element():
            body_key, contents = self.visit_inner_workflow_element(element)
            body[body_key] = contents

        return {
            'expression': expr,
            'body': body
        }

    # Task section

    def visit_task(self, ctx):
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
                task.setdefault('inputs', []).extend(self.visit_task_input(section))
            # output
            elif is_context(section, 'Task_outputContext'):
                task['outputs'] = self.visit_task_output(section)
            # command
            elif is_context(section, 'Task_commandContext'):
                task['raw_commandline'] = self.visit_task_command(section)
            # runtime
            elif is_context(section, 'Task_runtimeContext'):
                task['runtime'] = self.visit_task_runtime(section)
            # bound_decls
            elif is_context(section, 'Bound_declsContext'):
                # treating this the same as inputs for now
                decl = self.visit_bound_decls(section)
                task.setdefault('inputs', []).append(decl)
            # parameter_meta, and meta
            elif is_context(section, ('Parameter_meta_elementContext', 'Meta_elementContext')):
                pass
            else:
                raise RuntimeError(f'Unrecognized task element in visitTask(): {type(section)}')

    def visit_task_input(self, ctx):
        """
        Contains an array of 'any_decls', which can be unbound or bound declarations.
        Example:
            input {
              String in_str = "twenty"
              Int in_int
            }

        Returns a list of tuple=(name, type, expr)
        """
        return [self.visit_any_decls(decl) for decl in ctx.any_decls()]

    def visit_task_output(self, ctx):
        """
        Contains an array of 'bound_decls' (unbound_decls not allowed).
        Example:
            output {
              String out_str = read_string(stdout())
            }

        Returns a list of tuple=(name, type, expr)
        """
        return [self.visit_bound_decls(decl) for decl in ctx.bound_decls()]

    def visit_task_command(self, ctx):
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
        str_part = self.visit_task_command_string_part(ctx.task_command_string_part())
        if str_part:
            parts.append(f"r'''{str_part}'''")

        # add the rest
        for group in ctx.task_command_expr_with_string():
            expr_part, str_part = self.visit_task_command_expr_with_string(group)
            parts.append(expr_part)
            if str_part:
                parts.append(f"r'''{str_part}'''")

        return parts

    def visit_task_command_string_part(self, ctx):
        """
        Returns a string representing the string_part.
        """
        # join here because a string that contains '$', '{', or '}' is split
        return ''.join(part.getText() for part in ctx.CommandStringPart())

    def visit_task_command_expr_with_string(self, ctx):
        """
        Returns a tuple=(expr_part, string_part).
        """
        return (self.visit_task_command_expr_part(ctx.task_command_expr_part()),
                self.visit_task_command_string_part(ctx.task_command_string_part()))

    def visit_task_command_expr_part(self, ctx):
        """
        Contains the expression inside ${expr}. Same function as `self.visit_string_expr_part()`.

        Returns the expression.
        """
        return self.visit_string_expr_part(ctx)

    def visit_task_runtime(self, ctx):
        """
        Contains an array of `task_runtime_kv`s.

        Returns a dict={key: value} where key can be 'docker', 'cpu', 'memory',
        'cores', or 'disks'.
        """
        return OrderedDict((kv.children[0].getText(),  # runtime key
                            self.visit_expr(kv.expr()))  # runtime value
                           for kv in ctx.task_runtime_kv())

    # Shared

    def visit_any_decls(self, ctx):
        """
        Contains a bound or unbound declaration.
        """
        if ctx.bound_decls():
            return self.visit_bound_decls(ctx.bound_decls())
        elif ctx.unbound_decls():
            return self.visit_unbound_decls(ctx.unbound_decls())
        else:
            raise RuntimeError(f'Unrecognized declaration: {type(ctx)}')

    def visit_unbound_decls(self, ctx):
        """
        Contains an unbound declaration. E.g.: `String in_str`.

        Returns a tuple=(name, type, expr), where `expr` is None.
        """
        name = ctx.Identifier().getText()
        type_ = self.visit_wdl_type(ctx.wdl_type())
        return name, type_, None

    def visit_bound_decls(self, ctx):
        """
        Contains a bound declaration. E.g.: `String in_str = "some string"`.

        Returns a tuple=(name, type, expr).
        """
        name = ctx.Identifier().getText()
        type_ = self.visit_wdl_type(ctx.wdl_type())
        expr = self.visit_expr(ctx.expr())

        return name, type_, expr

    def visit_wdl_type(self, ctx):
        """
        Returns a WDLType instance.
        """
        identifier = ctx.type_base().children[0]
        optional = ctx.OPTIONAL() is not None

        # primitives
        if is_context(identifier, 'TerminalNodeImpl'):

            # TODO: implement Object type
            return self.create_wdl_primitive_type(key=identifier.getText(), optional=optional)
        # compound types
        else:
            name = identifier.children[0].getText()  # the first child is the name of the type.
            type_ = identifier.wdl_type()
            if isinstance(type_, list):
                elements = [self.visit_wdl_type(element) for element in type_]
            else:
                elements = [self.visit_wdl_type(type_)]
            return self.create_wdl_compound_type(key=name, elements=elements, optional=optional)

    def visit_primitive_literal(self, ctx):
        """
        Returns the primitive literal as a string.
        """
        is_bool = ctx.BoolLiteral()
        if is_bool:
            val = is_bool.getText()
            if val not in ('true', 'false'):
                raise TypeError(f'Parsed boolean ({val}) must be expressed as "true" or "false".')
            return val.capitalize()
        elif is_context(ctx.children[0], 'StringContext'):
            return self.visit_string(ctx.children[0])
        elif is_context(ctx.children[0], ('TerminalNodeImpl',  # this also includes variables
                                          'NumberContext')):
            return ctx.children[0].getText()
        else:
            raise RuntimeError(f'Primitive literal has unknown child: {type(ctx.children[0])}.')

    def visit_number(self, ctx):
        """
        Contains an `IntLiteral` or a `FloatLiteral`.
        """
        return ctx.children[0].getText()

    def visit_string(self, ctx):
        """
        Contains a `string_part` followed by an array of `string_expr_with_string_part`s.
        """
        string = self.visit_string_part(ctx.string_part())

        for part in ctx.string_expr_with_string_part():
            string += f' + {self.visit_string_expr_with_string_part(part)}'

        return string

    def visit_string_expr_with_string_part(self, ctx):
        """
        Contains a `string_expr_part` and a `string_part`.
        """
        expr = self.visit_string_expr_part(ctx.string_expr_part())
        part = self.visit_string_part(ctx.string_part())

        if not part:
            return expr

        return f'{expr} + {part}'

    def visit_string_expr_part(self, ctx):
        """
        Contains an array of `expression_placeholder_option`s and an `expr`.
        """
        # See https://github.com/openwdl/wdl/blob/main/versions/1.0/parsers/antlr4/WdlV1Parser.g4#L56

        options = {}

        for opt in ctx.expression_placeholder_option():
            key, val = self.visit_expression_placeholder_option(opt)
            options[key] = val

        expr = self.visit_expr(ctx.expr())

        if len(options) == 0:
            return expr
        elif 'sep' in options:
            sep = options['sep']
            return f'{sep}.join(str(x) for x in {expr})'
        elif 'default' in options:
            default = options['default']
            return f'({expr} if {expr} else {default})'
        else:
            raise NotImplementedError(options)

    def visit_string_part(self, ctx):
        """
        Returns a string representing the string_part.
        """
        # join here because a string that contains '$', '{', or '}' is split
        part = ''.join(part.getText() for part in ctx.StringPart())

        if part:
            return f"'{part}'"
        return None

    def visit_expression_placeholder_option(self, ctx):
        """
        Expression placeholder options.

        Can match one of the following:
              BoolLiteral EQUAL (string | number)
              DEFAULT EQUAL (string | number)
              SEP EQUAL (string | number)

        See https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#expression-placeholder-options

        e.g.: ${sep=", " array_value}
        e.g.: ${true="--yes" false="--no" boolean_value}
        e.g.: ${default="foo" optional_value}

        Returns a tuple=(key, value)
        """
        assert len(ctx.children) == 3

        param = ctx.children[0].getText()
        str_or_num = ctx.children[2]
        val = self.visit_string(str_or_num) \
            if is_context(str_or_num, 'StringContext') else self.visit_number(str_or_num)

        return param, val

    def visit_expr(self, ctx):
        """
        Expression root.
        """
        return self.visit_infix0(ctx.expr_infix())

    def visit_infix0(self, ctx):
        """
        Expression infix0 (LOR).
        """
        infix = ctx.expr_infix0()
        if is_context(infix, 'LorContext'):
            return self.visit_lor(infix)
        return self.visit_infix1(infix)

    def visit_lor(self, ctx):
        """
        Logical OR expression.
        """
        lhs = self.visit_infix0(ctx)
        rhs = self.visit_infix1(ctx)
        return f'{lhs} or {rhs}'

    def visit_infix1(self, ctx):
        """
        Expression infix1 (LAND).
        """
        infix = ctx.expr_infix1()
        if is_context(infix, 'LandContext'):
            return self.visit_land(infix)
        return self.visit_infix2(infix)

    def visit_land(self, ctx):
        """
        Logical AND expresion.
        """
        lhs = self.visit_infix1(ctx)
        rhs = self.visit_infix2(ctx)
        return f'{lhs} and {rhs}'

    def visit_infix2(self, ctx):
        """
        Expression infix2 (comparisons).
        """
        infix = ctx.expr_infix2()
        if is_context(infix, 'EqeqContext'):
            return self._visit_infix2(infix, '==')
        elif is_context(infix, 'NeqContext'):
            return self._visit_infix2(infix, '!=')
        elif is_context(infix, 'LteContext'):
            return self._visit_infix2(infix, '<=')
        elif is_context(infix, 'GteContext'):
            return self._visit_infix2(infix, '>=')
        elif is_context(infix, 'LtContext'):
            return self._visit_infix2(infix, '<')
        elif is_context(infix, 'GtContext'):
            return self._visit_infix2(infix, '>')
        # continue down our path
        return self.visit_infix3(infix)

    def _visit_infix2(self, ctx, operation: str):
        """
        :param operation: Operation as a string.
        """
        lhs = self.visit_infix2(ctx)
        rhs = self.visit_infix3(ctx)
        return f'{lhs} {operation} {rhs}'

    def visit_infix3(self, ctx):
        """
        Expression infix3 (add/subtract).
        """
        infix = ctx.expr_infix3()
        if is_context(infix, 'AddContext'):
            return self._visit_infix3(infix, '+')
        elif is_context(infix, 'SubContext'):
            return self._visit_infix3(infix, '-')
        # continue down our path
        return self.visit_infix4(infix)

    def _visit_infix3(self, ctx, operation: str):
        """
        :param operation: Operation as a string.
        """
        lhs = self.visit_infix3(ctx)
        rhs = self.visit_infix4(ctx)
        return f'{lhs} {operation} {rhs}'

    def visit_infix4(self, ctx):
        """
        Expression infix4 (multiply/divide/modulo).
        """
        infix = ctx.expr_infix4()
        if is_context(infix, 'MulContext'):
            return self._visit_infix4(infix, '*')
        elif is_context(infix, 'DivideContext'):
            return self._visit_infix4(infix, '/')
        elif is_context(infix, 'ModContext'):
            return self._visit_infix4(infix, '%')
        # continue down our path
        return self.visit_infix5(infix)

    def _visit_infix4(self, ctx, operation: str):
        """
        :param operation: Operation as a string.
        """
        lhs = self.visit_infix4(ctx)
        rhs = self.visit_infix5(ctx)
        return f'{lhs} {operation} {rhs}'

    def visit_infix5(self, ctx):
        """
        Expression infix5.
        """
        return self.visit_expr_core(ctx.expr_infix5().expr_core())

    def visit_expr_core(self, expr):
        """
        Expression core.
        """
        # TODO: implement map_literal, object_literal, and left_name

        if is_context(expr, 'ApplyContext'):
            return self.visit_apply(expr)
        elif is_context(expr, 'Array_literalContext'):
            return self.visit_array_literal(expr)
        elif is_context(expr, 'Pair_literalContext'):
            return self.visit_pair_literal(expr)
        elif is_context(expr, 'IfthenelseContext'):
            return self.visit_ifthenelse(expr)
        elif is_context(expr, 'Expression_groupContext'):
            return self.visit_expression_group(expr)
        elif is_context(expr, 'AtContext'):
            return self.visit_at(expr)
        elif is_context(expr, 'Get_nameContext'):
            return self.visit_get_name(expr)
        elif is_context(expr, 'NegateContext'):
            return self.visit_negate(expr)
        elif is_context(expr, 'UnarysignedContext'):
            return self.visit_unarysigned(expr)
        elif is_context(expr, 'PrimitivesContext'):
            return self.visit_primitives(expr)

        raise NotImplementedError(f"Expression context '{type(expr)}' is not supported.")

    # expr_core

    def visit_apply(self, ctx):
        """
        A function call.
        Pattern: Identifier LPAREN (expr (COMMA expr)*)? RPAREN
        """
        fn = ctx.Identifier().getText()
        params = ', '.join(self.visit_expr(expr) for expr in ctx.expr())

        if fn == 'stdout':
            return '_toil_wdl_internal__stdout_file'
        elif fn == 'stderr':
            return '_toil_wdl_internal__stderr_file'
        elif fn in ('range', 'zip'):
            # replace python built-in functions
            return f'wdl_{fn}'

        call = f'{fn}({params}'

        # append necessary params for i/o functions
        if fn == 'glob':
            return call + ', tempDir)'
        elif fn == 'size':
            return call + (params + ', ' if params else '') + 'fileStore=fileStore)'
        elif fn in ('write_lines', 'write_tsv', 'write_json', 'write_map'):
            return call + ', temp_dir=tempDir, file_store=fileStore)'
        else:
            return call + ')'

    def visit_array_literal(self, ctx):
        """
        Pattern: LBRACK (expr (COMMA expr)*)* RBRACK
        """
        return f"[{', '.join(self.visit_expr(expr) for expr in ctx.expr())}]"

    def visit_pair_literal(self, ctx):
        """
        Pattern: LPAREN expr COMMA expr RPAREN
        """
        return f"({self.visit_expr(ctx.expr(0))}, {self.visit_expr(ctx.expr(1))})"

    def visit_ifthenelse(self, ctx):
        """
        Ternary expression.
        Pattern: IF expr THEN expr ELSE expr
        """
        if_true = self.visit_expr(ctx.expr(0))
        condition = self.visit_expr(ctx.expr(1))
        if_false = self.visit_expr(ctx.expr(2))

        return f'({condition} if {if_true} else {if_false})'

    def visit_expression_group(self, ctx):
        """
        Pattern: LPAREN expr RPAREN
        """
        return f'({self.visit_expr(ctx.expr())})'

    def visit_at(self, ctx):
        """
        Array or map lookup.
        Pattern: expr_core LBRACK expr RBRACK
        """
        expr_core = self.visit_expr_core(ctx.expr_core())
        expr = self.visit_expr(ctx.expr())

        # parenthesis must be removed because 'i[0]' works, but '(i)[0]' does not
        if expr_core[0] == '(' and expr_core[-1] == ')':
            expr_core = expr_core[1:-1]

        return f'{expr_core}[{expr}]'

    def visit_get_name(self, ctx):
        """
        Member access.
        Pattern: expr_core DOT Identifier
        """
        expr_core = self.visit_expr_core(ctx.expr_core())
        identifier = ctx.Identifier().getText()

        if identifier in ('left', 'right'):
            # hack-y way to make sure pair.left and pair.right are parsed correctly.
            return f'({expr_core}.{identifier})'

        return f'({expr_core}_{identifier})'

    def visit_negate(self, ctx):
        """
        Pattern: NOT expr
        """
        return f'(not {self.visit_expr(ctx.expr())})'

    def visit_unarysigned(self, ctx):
        """
        Pattern: (PLUS | MINUS) expr
        """
        plus: bool = ctx.PLUS() is not None
        expr = self.visit_expr(ctx.expr())

        if plus:
            return f'(+{expr})'
        return f'(-{expr})'

    def visit_primitives(self, ctx):
        """
        Expression alias for primitive literal.
        """
        return self.visit_primitive_literal(ctx.primitive_literal())
