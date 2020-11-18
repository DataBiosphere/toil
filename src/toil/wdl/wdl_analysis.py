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
import json
import os
import logging
from collections import OrderedDict

import toil.wdl.wdl_parser as wdl_parser
from toil.wdl.wdl_types import WDLPairType, WDLMapType

wdllogger = logging.getLogger(__name__)


class AnalyzeWDL:
    '''
    Analyzes a wdl file, and associated json and/or extraneous files and restructures them
    into 2 intermediate data structures (python dictionaries):
        "workflows_dictionary": containing the parsed workflow information.
        "tasks_dictionary": containing the parsed task information.

    These are then fed into wdl_synthesis.py which uses them to write a native python
    script for use with Toil.

    Requires a WDL file, and a JSON file.  The WDL file contains ordered commands,
    and the JSON file contains input values for those commands.  In addition, this
    also takes potential accessory files like csv/tsv potentially also containing
    variables which need to be incorporated.
    '''

    def __init__(self, wdl_filename, secondary_filename, output_directory):

        # inputs
        self.wdl_file = wdl_filename
        self.secondary_file = secondary_filename
        self.output_directory = output_directory

        if not os.path.exists(self.output_directory):
            try:
                os.makedirs(self.output_directory)
            except:
                raise OSError(
                    'Could not create directory.  Insufficient permissions or disk space most likely.')

        self.output_file = os.path.join(self.output_directory, 'toilwdl_compiled.py')

        # only json is required; tsv/csv are optional
        self.json_dict = {}

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

    def find_asts(self, ast_root, name):
        '''
        Finds an AST node with the given name and the entire subtree under it.
        A function borrowed from scottfrazer.  Thank you Scott Frazer!

        :param ast_root: The WDL AST.  The whole thing generally, but really
                         any portion that you wish to search.
        :param name: The name of the subtree you're looking for, like "Task".
        :return: nodes representing the AST subtrees matching the "name" given.
        '''
        nodes = []
        if isinstance(ast_root, wdl_parser.AstList):
            for node in ast_root:
                nodes.extend(self.find_asts(node, name))
        elif isinstance(ast_root, wdl_parser.Ast):
            if ast_root.name == name:
                nodes.append(ast_root)
            for attr_name, attr in ast_root.attributes.items():
                nodes.extend(self.find_asts(attr, name))
        return nodes

    def dict_from_YML(self, YML_file):
        '''
        Not written yet.  Use JSON.  It's better anyway.

        :param YML_file: A yml file with extension '*.yml' or '*.yaml'.
        :return: Nothing.
        '''
        raise NotImplementedError('.y(a)ml support is currently underwhelming.')

    def dict_from_JSON(self, JSON_file):
        '''
        Takes a WDL-mapped json file and creates a dict containing the bindings.
        The 'return' value is only used for unittests.

        :param JSON_file: A required JSON file containing WDL variable bindings.
        :return: Returns the self.json_dict purely for unittests.
        '''

        # TODO: Add context support for variables within multiple wdl files
        with open(JSON_file) as data_file:
            data = json.load(data_file)
        for d in data:
            if isinstance(data[d], str):
                self.json_dict[d] = '"' + data[d] + '"'
            else:
                self.json_dict[d] = data[d]
        return self.json_dict

    def create_tasks_dict(self, ast):
        '''
        Parse each "Task" in the AST.  This will create self.tasks_dictionary,
        where each task name is a key.

        :return: Creates the self.tasks_dictionary necessary for much of the
        parser.  Returning it is only necessary for unittests.
        '''
        tasks = self.find_asts(ast, 'Task')
        for task in tasks:
            self.parse_task(task)
        return self.tasks_dictionary

    def parse_task(self, task):
        '''
        Parses a WDL task AST subtree.

        Currently looks at and parses 4 sections:
        1. Declarations (e.g. string x = 'helloworld')
        2. Commandline (a bash command with dynamic variables inserted)
        3. Runtime (docker image; disk; CPU; RAM; etc.)
        4. Outputs (expected return values/files)

        :param task: An AST subtree of a WDL "Task".
        :return: Returns nothing but adds a task to the self.tasks_dictionary
        necessary for much of the parser.
        '''

        task_name = task.attributes["name"].source_string

        # task declarations
        declaration_array = []
        for declaration_subAST in task.attr("declarations"):
            declaration_array.append(self.parse_task_declaration(declaration_subAST))
            self.tasks_dictionary.setdefault(task_name, OrderedDict())['inputs'] = declaration_array

        for section in task.attr("sections"):

            # task commandline entries section [command(s) to run]
            if section.name == "RawCommand":
                command_array = self.parse_task_rawcommand(section)
                self.tasks_dictionary.setdefault(task_name, OrderedDict())['raw_commandline'] = command_array

            # task runtime section (docker image; disk; CPU; RAM; etc.)
            if section.name == "Runtime":
                runtime_dict = self.parse_task_runtime(section.attr("map"))
                self.tasks_dictionary.setdefault(task_name, OrderedDict())['runtime'] = runtime_dict

            # task output filenames section (expected return values/files)
            if section.name == "Outputs":
                output_array = self.parse_task_outputs(section)
                self.tasks_dictionary.setdefault(task_name, OrderedDict())['outputs'] = output_array

    def parse_task_declaration(self, declaration_subAST):
        '''
        Parses the declaration section of the WDL task AST subtree.

        Examples:

        String my_name
        String your_name
        Int two_chains_i_mean_names = 0

        :param declaration_subAST: Some subAST representing a task declaration
                                   like: 'String file_name'
        :return: var_name, var_type, var_value
            Example:
                Input subAST representing:   'String file_name'
                Output:  var_name='file_name', var_type='String', var_value=None
        '''
        var_name = self.parse_declaration_name(declaration_subAST.attr("name"))
        var_type = self.parse_declaration_type(declaration_subAST.attr("type"))
        var_expressn = self.parse_declaration_expressn(declaration_subAST.attr("expression"), es='')

        return (var_name, var_type, var_expressn)

    def parse_task_rawcommand_attributes(self, code_snippet):
        """

        :param code_snippet:
        :return:
        """
        attr_dict = OrderedDict()
        if isinstance(code_snippet, wdl_parser.Terminal):
            raise NotImplementedError
        if isinstance(code_snippet, wdl_parser.Ast):
            raise NotImplementedError
        if isinstance(code_snippet, wdl_parser.AstList):
            for ast in code_snippet:
                if ast.name == 'CommandParameterAttr':
                    # TODO rewrite
                    if ast.attributes['value'].str == 'string':
                        attr_dict[ast.attributes['key'].source_string] = "'" + ast.attributes['value'].source_string + "'"
                    else:
                        attr_dict[ast.attributes['key'].source_string] = ast.attributes['value'].source_string
        return attr_dict

    def parse_task_rawcommand(self, rawcommand_subAST):
        '''
        Parses the rawcommand section of the WDL task AST subtree.

        Task "rawcommands" are divided into many parts.  There are 2 types of
        parts: normal strings, & variables that can serve as changeable inputs.

        The following example command:
            'echo ${variable1} ${variable2} > output_file.txt'

        Has 5 parts:
                     Normal  String: 'echo '
                     Variable Input: variable1
                     Normal  String: ' '
                     Variable Input: variable2
                     Normal  String: ' > output_file.txt'

        Variables can also have additional conditions, like 'sep', which is like
        the python ''.join() function and in WDL looks like: ${sep=" -V " GVCFs}
        and would be translated as: ' -V '.join(GVCFs).

        :param rawcommand_subAST: A subAST representing some bash command.
        :return: A list=[] of tuples=() representing the parts of the command:
             e.g. [(command_var, command_type, additional_conditions_list), ...]
                  Where: command_var = 'GVCFs'
                         command_type = 'variable'
                         command_actions = {'sep': ' -V '}
        '''
        command_array = []
        for code_snippet in rawcommand_subAST.attributes["parts"]:

            # normal string
            if isinstance(code_snippet, wdl_parser.Terminal):
                command_var = "r'''" + code_snippet.source_string + "'''"

            # a variable like ${dinosaurDNA}
            if isinstance(code_snippet, wdl_parser.Ast):
                if code_snippet.name == 'CommandParameter':
                    # change in the future?  seems to be a different parameter but works for all cases it seems?
                    code_expr = self.parse_declaration_expressn(code_snippet.attr('expr'), es='')
                    code_attributes = self.parse_task_rawcommand_attributes(code_snippet.attr('attributes'))
                    command_var = self.modify_cmd_expr_w_attributes(code_expr, code_attributes)

            if isinstance(code_snippet, wdl_parser.AstList):
                raise NotImplementedError
            command_array.append(command_var)

        return command_array

    def modify_cmd_expr_w_attributes(self, code_expr, code_attr):
        """

        :param code_expr:
        :param code_attr:
        :return:
        """
        for param in code_attr:
            if param == 'sep':
                code_expr = "{sep}.join(str(x) for x in {expr})".format(sep=code_attr[param], expr=code_expr)
            elif param == 'default':
                code_expr = "{expr} if {expr} else {default}".format(default=code_attr[param], expr=code_expr)
            else:
                raise NotImplementedError
        return code_expr

    def parse_task_runtime_key(self, i):
        """

        :param runtime_subAST:
        :return:
        """
        if isinstance(i, wdl_parser.Terminal):
            return i.source_string
        if isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        if isinstance(i, wdl_parser.AstList):
            raise NotImplementedError

    def parse_task_runtime(self, runtime_subAST):
        '''
        Parses the runtime section of the WDL task AST subtree.

        The task "runtime" section currently supports context fields for a
        docker container, CPU resources, RAM resources, and disk resources.

        :param runtime_subAST: A subAST representing runtime parameters.
        :return: A list=[] of runtime attributes, for example:
                 runtime_attributes = [('docker','quay.io/encode-dcc/map:v1.0'),
                                       ('cpu','2'),
                                       ('memory','17.1 GB'),
                                       ('disks','local-disk 420 HDD')]
        '''
        runtime_attributes = OrderedDict()
        if isinstance(runtime_subAST, wdl_parser.Terminal):
            raise NotImplementedError
        elif isinstance(runtime_subAST, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(runtime_subAST, wdl_parser.AstList):
            for ast in runtime_subAST:
                key = self.parse_task_runtime_key(ast.attr('key'))
                value = self.parse_declaration_expressn(ast.attr('value'), es='')
                if value.startswith('"'):
                    value = self.translate_wdl_string_to_python_string(value[1:-1])
                runtime_attributes[key] = value
        return runtime_attributes

    def parse_task_outputs(self, i):
        '''
        Parse the WDL output section.

        Outputs are like declarations, with a type, name, and value.  Examples:

        ------------
        Simple Cases
        ------------

        'Int num = 7'
            var_name: 'num'
            var_type: 'Int'
            var_value: 7

        String idea = 'Lab grown golden eagle burgers.'
            var_name: 'idea'
            var_type: 'String'
            var_value: 'Lab grown golden eagle burgers.'

        File ideaFile = 'goldenEagleStemCellStartUpDisrupt.txt'
            var_name: 'ideaFile'
            var_type: 'File'
            var_value: 'goldenEagleStemCellStartUpDisrupt.txt'

        -------------------
        More Abstract Cases
        -------------------

        Array[File] allOfMyTerribleIdeas = glob(*.txt)[0]
            var_name:      'allOfMyTerribleIdeas'
            var_type**:    'File'
            var_value:     [*.txt]
            var_actions:   {'index_lookup': '0', 'glob': 'None'}

        **toilwdl.py converts 'Array[File]' to 'ArrayFile'

        :return: output_array representing outputs generated by the job/task:
                e.g. x = [(var_name, var_type, var_value, var_actions), ...]
        '''
        output_array = []
        for j in i.attributes['attributes']:
            if j.name == 'Output':
                var_name = self.parse_declaration_name(j.attr("name"))
                var_type = self.parse_declaration_type(j.attr("type"))
                var_expressn = self.parse_declaration_expressn(j.attr("expression"), es='', output_expressn=True)
                if not (var_expressn.startswith('(') and var_expressn.endswith(')')):
                    var_expressn = self.translate_wdl_string_to_python_string(var_expressn)
                output_array.append((var_name, var_type, var_expressn))
            else:
                raise NotImplementedError
        return output_array

    def translate_wdl_string_to_python_string(self, some_string):
        '''
        Parses a string representing a given job's output filename into something
        python can read.  Replaces ${string}'s with normal variables and the rest
        with normal strings all concatenated with ' + '.

        Will not work with additional parameters, such as:
        ${default="foo" bar}
        or
        ${true="foo" false="bar" Boolean baz}

        This method expects to be passed only strings with some combination of
        "${abc}" and "abc" blocks.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param some_string: e.g. '${sampleName}.vcf'
        :return: output_string, e.g. 'sampleName + ".vcf"'
        '''

        try:
            # add support for 'sep'
            output_string = ''
            edited_string = some_string.strip()

            if edited_string.find('${') != -1:
                continue_loop = True
                while (continue_loop):
                    index_start = edited_string.find('${')
                    index_end = edited_string.find('}', index_start)

                    stringword = edited_string[:index_start]

                    if index_start != 0:
                        output_string = output_string + "'" + stringword + "' + "

                    keyword = edited_string[index_start + 2:index_end]
                    output_string = output_string + "str(" + keyword + ") + "

                    edited_string = edited_string[index_end + 1:]
                    if edited_string.find('${') == -1:
                        continue_loop = False
                        if edited_string:
                            output_string = output_string + "'" + edited_string + "' + "
            else:
                output_string = "'" + edited_string + "'"

            if output_string.endswith(' + '):
                output_string = output_string[:-3]

            return output_string
        except:
            return ''

    def create_workflows_dict(self, ast):
        '''
        Parse each "Workflow" in the AST.  This will create self.workflows_dictionary,
        where each called job is a tuple key of the form: (priority#, job#, name, alias).

        :return: Creates the self.workflows_dictionary necessary for much of the
        parser.  Returning it is only necessary for unittests.
        '''
        workflows = self.find_asts(ast, 'Workflow')
        for workflow in workflows:
            self.parse_workflow(workflow)
        return self.workflows_dictionary

    def parse_workflow(self, workflow):
        '''
        Parses a WDL workflow AST subtree.

        Currently looks at and parses 3 sections:
        1. Declarations (e.g. string x = 'helloworld')
        2. Calls (similar to a python def)
        3. Scatter (which expects to map to a Call or multiple Calls)

        Returns nothing but creates the self.workflows_dictionary necessary for much
        of the parser.

        :param workflow: An AST subtree of a WDL "Workflow".
        :return: Returns nothing but adds a workflow to the
                 self.workflows_dictionary necessary for much of the parser.
        '''
        workflow_name = workflow.attr('name').source_string

        wf_declared_dict = OrderedDict()
        for section in workflow.attr("body"):

            if section.name == "Declaration":
                var_name, var_map = self.parse_workflow_declaration(section)
                wf_declared_dict[var_name] = var_map
            self.workflows_dictionary.setdefault(workflow_name, OrderedDict())['wf_declarations'] = wf_declared_dict

            if section.name == "Scatter":
                scattertask = self.parse_workflow_scatter(section)
                self.workflows_dictionary.setdefault(workflow_name, OrderedDict())['scatter' + str(self.scatter_number)] = scattertask
                self.scatter_number += 1

            if section.name == "Call":
                task = self.parse_workflow_call(section)
                self.workflows_dictionary.setdefault(workflow_name, OrderedDict())['call' + str(self.call_number)] = task
                self.call_number += 1

            if section.name == "If":
                task = self.parse_workflow_if(section)
                self.workflows_dictionary.setdefault(workflow_name, OrderedDict())['if' + str(self.if_number)] = task
                self.if_number += 1

    def parse_workflow_if(self, ifAST):
        expression = self.parse_workflow_if_expression(ifAST.attr('expression'))
        body = self.parse_workflow_if_body(ifAST.attr('body'))
        return {'expression': expression, 'body': body}

    def parse_workflow_if_body(self, i):
        subworkflow_dict = OrderedDict()
        wf_declared_dict = OrderedDict()
        if isinstance(i, wdl_parser.Terminal):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            for ast in i:
                if ast.name == "Declaration":
                    var_name, var_map = self.parse_workflow_declaration(ast)
                    wf_declared_dict[var_name] = var_map
                subworkflow_dict['wf_declarations'] = wf_declared_dict

                if ast.name == "Scatter":
                    scattertask = self.parse_workflow_scatter(ast)
                    subworkflow_dict['scatter' + str(self.scatter_number)] = scattertask
                    self.scatter_number += 1

                if ast.name == "Call":
                    task = self.parse_workflow_call(ast)
                    subworkflow_dict['call' + str(self.call_number)] = task
                    self.call_number += 1

                if ast.name == "If":
                    task = self.parse_workflow_if(ast)
                    subworkflow_dict['if' + str(self.if_number)] = task
                    self.if_number += 1
        return subworkflow_dict

    def parse_workflow_if_expression(self, i):
        if isinstance(i, wdl_parser.Terminal):
            ifthis = i.source_string
        elif isinstance(i, wdl_parser.Ast):
            ifthis = self.parse_declaration_expressn(i, es='')
        elif isinstance(i, wdl_parser.AstList):
            raise NotImplementedError
        return ifthis

    def parse_workflow_scatter(self, scatterAST):
        item = self.parse_workflow_scatter_item(scatterAST.attr('item'))
        collection = self.parse_workflow_scatter_collection(scatterAST.attr('collection'))
        body = self.parse_workflow_scatter_body(scatterAST.attr('body'))
        return {'item': item, 'collection': collection, 'body': body}

    def parse_workflow_scatter_item(self, i):
        if isinstance(i, wdl_parser.Terminal):
            return i.source_string
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            raise NotImplementedError

    def parse_workflow_scatter_collection(self, i):
        if isinstance(i, wdl_parser.Terminal):
            return i.source_string
        elif isinstance(i, wdl_parser.Ast):
            return self.parse_declaration_expressn(i, es='')
        elif isinstance(i, wdl_parser.AstList):
            raise NotImplementedError

    def parse_workflow_scatter_body(self, i):
        if isinstance(i, wdl_parser.Terminal):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            scatterbody = OrderedDict()
            element = 0
            for ast in i:
                if ast.name == "Declaration":
                    var_name, var_map = self.parse_workflow_declaration(ast)
                    scatterbody['variable' + str(element)] = var_map
                    element += 1

                if ast.name == "Scatter":
                    scattertask = self.parse_workflow_scatter(ast)
                    scatterbody['scatter' + str(self.scatter_number)] = scattertask
                    self.scatter_number += 1
                    # TODO test this
                    raise NotImplementedError

                if ast.name == "Call":
                    task = self.parse_workflow_call(ast)
                    scatterbody['call' + str(self.call_number)] = task
                    self.call_number += 1

                if ast.name == "If":
                    task = self.parse_workflow_if(ast)
                    scatterbody['if' + str(self.if_number)] = task
                    self.if_number += 1
        return scatterbody


    def parse_declaration_name(self, nameAST):
        """
        Required.

        Nothing fancy here.  Just the name of the workflow
        function.  For example: "rnaseqexample" would be the following
        wdl workflow's name:

        workflow rnaseqexample {File y; call a {inputs: y}; call b;}
        task a {File y}
        task b {command{"echo 'ATCG'"}}

        :param nameAST:
        :return:
        """
        if isinstance(nameAST, wdl_parser.Terminal):
            return nameAST.source_string
        elif isinstance(nameAST, wdl_parser.Ast):
            return nameAST.source_string
        elif isinstance(nameAST, wdl_parser.AstList):
            raise NotImplementedError

    def parse_declaration_type(self, typeAST):
        """
        Required.

        Currently supported:
        Types are: Boolean, Float, Int, File, String, Array[subtype],
                    Pair[subtype, subtype], and Map[subtype, subtype].
        OptionalTypes are: Boolean?, Float?, Int?, File?, String?, Array[subtype]?,
                            Pair[subtype, subtype]?, and Map[subtype, subtype].

        Python is not typed, so we don't need typing except to identify type: "File",
        which Toil needs to import, so we recursively travel down to the innermost
        type which will tell us if the variables are files that need importing.

        For Pair and Map compound types, we recursively travel down the subtypes and
        store them as attributes of a `WDLType` string. This way, the type structure is
        preserved, which will allow us to import files appropriately.

        :param typeAST:
        :return:
        """
        # TODO: represent all WDL types as WDLType subclasses, even terminal ones.

        if isinstance(typeAST, wdl_parser.Terminal):
            return typeAST.source_string
        elif isinstance(typeAST, wdl_parser.Ast):
            if typeAST.name == 'Type':
                subtype = typeAST.attr('subtype')
            elif typeAST.name == 'OptionalType':
                subtype = typeAST.attr('innerType')
            else:
                raise NotImplementedError

            if isinstance(subtype, wdl_parser.AstList):
                # we're looking at a compound type
                name = self.parse_declaration_type(typeAST.attr('name'))
                elements = [self.parse_declaration_type(element) for element in subtype]

                if name == 'Array':
                    # for arrays, recursively travel down to the innermost type
                    return self.parse_declaration_type(subtype)
                if name == 'Pair':
                    return WDLPairType(*elements)
                elif name == 'Map':
                    return WDLMapType(*elements)
                else:
                    raise NotImplementedError

            return self.parse_declaration_type(subtype)

        elif isinstance(typeAST, wdl_parser.AstList):
            for ast in typeAST:
                # TODO only ever seen one element lists.
                return self.parse_declaration_type(ast)

    def parse_declaration_expressn(self, expressionAST, es, output_expressn=False):
        """
        Expressions are optional.  Workflow declaration valid examples:

        File x

        or

        File x = '/x/x.tmp'

        :param expressionAST:
        :return:
        """
        if not expressionAST:
            return None
        else:
            if isinstance(expressionAST, wdl_parser.Terminal):
                if expressionAST.str == 'boolean':
                    if expressionAST.source_string == 'false':
                        return 'False'
                    elif expressionAST.source_string == 'true':
                        return 'True'
                    else:
                        raise TypeError('Parsed boolean ({}) must be expressed as "true" or "false".'
                                        ''.format(expressionAST.source_string))
                elif expressionAST.str == 'string' and not output_expressn:
                    parsed_string = self.translate_wdl_string_to_python_string(expressionAST.source_string)
                    return '{string}'.format(string=parsed_string)
                else:
                    return '{string}'.format(string=expressionAST.source_string)
            elif isinstance(expressionAST, wdl_parser.Ast):
                if expressionAST.name == 'Add':
                    es = es + self.parse_declaration_expressn_operator(expressionAST.attr('lhs'),
                                                                       expressionAST.attr('rhs'),
                                                                       es,
                                                                       operator=' + ')
                elif expressionAST.name == 'Subtract':
                    es = es + self.parse_declaration_expressn_operator(expressionAST.attr('lhs'),
                                                                       expressionAST.attr('rhs'),
                                                                       es,
                                                                       operator=' - ')
                elif expressionAST.name == 'Multiply':
                    es = es + self.parse_declaration_expressn_operator(expressionAST.attr('lhs'),
                                                                       expressionAST.attr('rhs'),
                                                                       es,
                                                                       operator=' * ')
                elif expressionAST.name == 'Divide':
                    es = es + self.parse_declaration_expressn_operator(expressionAST.attr('lhs'),
                                                                       expressionAST.attr('rhs'),
                                                                       es,
                                                                       operator=' / ')
                elif expressionAST.name == 'GreaterThan':
                    es = es + self.parse_declaration_expressn_operator(expressionAST.attr('lhs'),
                                                                       expressionAST.attr('rhs'),
                                                                       es,
                                                                       operator=' > ')
                elif expressionAST.name == 'LessThan':
                    es = es + self.parse_declaration_expressn_operator(expressionAST.attr('lhs'),
                                                                       expressionAST.attr('rhs'),
                                                                       es,
                                                                       operator=' < ')
                elif expressionAST.name == 'FunctionCall':
                    es = es + self.parse_declaration_expressn_fncall(expressionAST.attr('name'),
                                                                     expressionAST.attr('params'),
                                                                     es)
                elif expressionAST.name == 'TernaryIf':
                    es = es + self.parse_declaration_expressn_ternaryif(expressionAST.attr('cond'),
                                                                        expressionAST.attr('iftrue'),
                                                                        expressionAST.attr('iffalse'),
                                                                        es)
                elif expressionAST.name == 'MemberAccess':
                    es = es + self.parse_declaration_expressn_memberaccess(expressionAST.attr('lhs'),
                                                                           expressionAST.attr('rhs'),
                                                                           es)
                elif expressionAST.name == 'ArrayLiteral':
                    es = es + self.parse_declaration_expressn_arrayliteral(expressionAST.attr('values'),
                                                                           es)
                elif expressionAST.name == 'TupleLiteral':
                    es = es + self.parse_declaration_expressn_tupleliteral(expressionAST.attr('values'),
                                                                           es)
                elif expressionAST.name == 'ArrayOrMapLookup':
                    es = es + self.parse_declaration_expressn_arraymaplookup(expressionAST.attr('lhs'),
                                                                             expressionAST.attr('rhs'),
                                                                             es)
                elif expressionAST.name == 'LogicalNot':
                    es = es + self.parse_declaration_expressn_logicalnot(expressionAST.attr('expression'),
                                                                         es)
                else:
                    raise NotImplementedError
            elif isinstance(expressionAST, wdl_parser.AstList):
                raise NotImplementedError
            return '(' + es + ')'

    def parse_declaration_expressn_logicalnot(self, exprssn, es):
        if isinstance(exprssn, wdl_parser.Terminal):
            es = es + exprssn.source_string
        elif isinstance(exprssn, wdl_parser.Ast):
            es = es + self.parse_declaration_expressn(exprssn, es='')
        elif isinstance(exprssn, wdl_parser.AstList):
            raise NotImplementedError
        return ' not ' + es

    def parse_declaration_expressn_arraymaplookup(self, lhsAST, rhsAST, es):
        """

        :param lhsAST:
        :param rhsAST:
        :param es:
        :return:
        """
        if isinstance(lhsAST, wdl_parser.Terminal):
            es = es + lhsAST.source_string
        elif isinstance(lhsAST, wdl_parser.Ast):
            # parenthesis must be removed because 'i[0]' works, but '(i)[0]' does not
            es = es + self.parse_declaration_expressn(lhsAST, es='')[1:-1]
        elif isinstance(lhsAST, wdl_parser.AstList):
            raise NotImplementedError

        if isinstance(rhsAST, wdl_parser.Terminal):
            indexnum = rhsAST.source_string
        elif isinstance(rhsAST, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(rhsAST, wdl_parser.AstList):
            raise NotImplementedError

        return es + '[{index}]'.format(index=indexnum)

    def parse_declaration_expressn_memberaccess(self, lhsAST, rhsAST, es):
        """
        Instead of "Class.variablename", use "Class.rv('variablename')".

        :param lhsAST:
        :param rhsAST:
        :param es:
        :return:
        """
        if isinstance(lhsAST, wdl_parser.Terminal):
            es = es + lhsAST.source_string
        elif isinstance(lhsAST, wdl_parser.Ast):
            es = es + self.parse_declaration_expressn(lhsAST, es)
        elif isinstance(lhsAST, wdl_parser.AstList):
            raise NotImplementedError

        # hack-y way to make sure pair.left and pair.right are parsed correctly.
        if isinstance(rhsAST, wdl_parser.Terminal) and (
                rhsAST.source_string == 'left' or rhsAST.source_string == 'right'):
            es = es + '.'
        else:
            es = es + '_'

        if isinstance(rhsAST, wdl_parser.Terminal):
            es = es + rhsAST.source_string
        elif isinstance(rhsAST, wdl_parser.Ast):
            es = es + self.parse_declaration_expressn(rhsAST, es)
        elif isinstance(rhsAST, wdl_parser.AstList):
            raise NotImplementedError

        return es

    def parse_declaration_expressn_ternaryif(self, cond, iftrue, iffalse, es):
        """
        Classic if statement.  This needs to be rearranged.

        In wdl, this looks like:
        if <condition> then <iftrue> else <iffalse>

        In python, this needs to be:
        <iftrue> if <condition> else <iffalse>

        :param cond:
        :param iftrue:
        :param iffalse:
        :param es:
        :return:
        """
        es = es + self.parse_declaration_expressn(iftrue, es='')
        es = es + ' if ' + self.parse_declaration_expressn(cond, es='')
        es = es + ' else ' + self.parse_declaration_expressn(iffalse, es='')
        return es

    def parse_declaration_expressn_tupleliteral(self, values, es):
        """
        Same in python.  Just a parenthesis enclosed tuple.

        :param values:
        :param es:
        :return:
        """
        es = es + '('
        for ast in values:
            es = es + self.parse_declaration_expressn(ast, es='') + ', '
        if es.endswith(', '):
            es = es[:-2]
        return es + ')'

    def parse_declaration_expressn_arrayliteral(self, values, es):
        """
        Same in python.  Just a square bracket enclosed array.

        :param values:
        :param es:
        :return:
        """
        es = es + '['
        for ast in values:
            es = es + self.parse_declaration_expressn(ast, es='') + ', '
        if es.endswith(', '):
            es = es[:-2]
        return es + ']'

    def parse_declaration_expressn_operator(self, lhsAST, rhsAST, es, operator):
        """
        Simply joins the left and right hand arguments lhs and rhs with an operator.

        :param lhsAST:
        :param rhsAST:
        :param es:
        :param operator:
        :return:
        """
        if isinstance(lhsAST, wdl_parser.Terminal):
            if lhsAST.str == 'string':
                es = es + '"{string}"'.format(string=lhsAST.source_string)
            else:
                es = es + '{string}'.format(string=lhsAST.source_string)
        elif isinstance(lhsAST, wdl_parser.Ast):
            es = es + self.parse_declaration_expressn(lhsAST, es='')
        elif isinstance(lhsAST, wdl_parser.AstList):
            raise NotImplementedError

        es = es + operator

        if isinstance(rhsAST, wdl_parser.Terminal):
            if rhsAST.str == 'string':
                es = es + '"{string}"'.format(string=rhsAST.source_string)
            else:
                es = es + '{string}'.format(string=rhsAST.source_string)
        elif isinstance(rhsAST, wdl_parser.Ast):
            es = es + self.parse_declaration_expressn(rhsAST, es='')
        elif isinstance(rhsAST, wdl_parser.AstList):
            raise NotImplementedError
        return es

    def parse_declaration_expressn_fncall(self, name, params, es):
        """
        Parses out cromwell's built-in function calls.

        Some of these are special and need minor adjustments,
        for example size() requires a fileStore.

        :param name:
        :param params:
        :param es:
        :return:
        """
        # name of the function
        if isinstance(name, wdl_parser.Terminal):
            if name.str:
                if name.source_string == 'stdout':
                    # let the stdout() function reference the generated stdout file path.
                    return es + '_toil_wdl_internal__stdout_file'
                elif name.source_string == 'stderr':
                    return es + '_toil_wdl_internal__stderr_file'
                elif name.source_string in ('range', 'zip'):
                    # replace python built-in functions
                    es += f'wdl_{name.source_string}('
                else:
                    es = es + name.source_string + '('
            else:
                raise NotImplementedError
        elif isinstance(name, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(name, wdl_parser.AstList):
            raise NotImplementedError

        es_params = self.parse_declaration_expressn_fncall_normalparams(params)

        if name.source_string == 'glob':
            return es + es_params + ', tempDir)'
        elif name.source_string == 'size':
            return es + (es_params + ', ' if es_params else '') + 'fileStore=fileStore)'
        elif name.source_string in ('write_lines', 'write_tsv', 'write_json', 'write_map'):
            return es + es_params + ', temp_dir=tempDir, file_store=fileStore)'
        else:
            return es + es_params + ')'

    def parse_declaration_expressn_fncall_normalparams(self, params):

        # arguments passed to the function
        if isinstance(params, wdl_parser.Terminal):
            raise NotImplementedError
        elif isinstance(params, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(params, wdl_parser.AstList):
            es_param = ''
            for ast in params:
                es_param = es_param + self.parse_declaration_expressn(ast, es='') + ', '
            if es_param.endswith(', '):
                es_param = es_param[:-2]
            return es_param

    def parse_workflow_declaration(self, wf_declaration_subAST):
        '''
        Parses a WDL declaration AST subtree into a string and a python
        dictionary containing its 'type' and 'value'.

        For example:
        var_name = refIndex
        var_map = {'type': File,
                   'value': bamIndex}

        :param wf_declaration_subAST: An AST subtree of a workflow declaration.
        :return: var_name, which is the name of the declared variable
        :return: var_map, a dictionary with keys for type and value.
                          e.g. {'type': File, 'value': bamIndex}
        '''
        var_map = OrderedDict()
        var_name = self.parse_declaration_name(wf_declaration_subAST.attr("name"))
        var_type = self.parse_declaration_type(wf_declaration_subAST.attr("type"))
        var_expressn = self.parse_declaration_expressn(wf_declaration_subAST.attr("expression"), es='')

        var_map['name'] = var_name
        var_map['type'] = var_type
        var_map['value'] = var_expressn

        return var_name, var_map

    def parse_workflow_call_taskname(self, i):
        """
        Required.

        :param i:
        :return:
        """
        if isinstance(i, wdl_parser.Terminal):
            return i.source_string
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            raise NotImplementedError

    def parse_workflow_call_taskalias(self, i):
        """
        Required.

        :param i:
        :return:
        """
        if isinstance(i, wdl_parser.Terminal):
            return i.source_string
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            raise NotImplementedError

    def parse_workflow_call_body_declarations(self, i):
        """
        Have not seen this used, so expects to return "[]".

        :param i:
        :return:
        """
        declaration_array = []
        if isinstance(i, wdl_parser.Terminal):
            declaration_array = [i.source_string]
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            for ast in i:
                declaration_array.append(self.parse_task_declaration(ast))

        # have not seen this used so raise to check
        if declaration_array:
            raise NotImplementedError

        return declaration_array

    def parse_workflow_call_body_io(self, i):
        """
        Required.

        :param i:
        :return:
        """
        if isinstance(i, wdl_parser.Terminal):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            for ast in i:
                assert len(i) == 1
                if ast.name == 'Inputs':
                    return self.parse_workflow_call_body_io_map(ast.attr('map'))
                else:
                    raise NotImplementedError

    def parse_workflow_call_body_io_map(self, i):
        """
        Required.

        :param i:
        :return:
        """
        io_map = OrderedDict()
        if isinstance(i, wdl_parser.Terminal):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            for ast in i:
                if ast.name == 'IOMapping':
                    key = self.parse_declaration_expressn(ast.attr("key"), es='')
                    value = self.parse_declaration_expressn(ast.attr("value"), es='')
                    io_map[key] = value
                else:
                    raise NotImplementedError
        return io_map

    def parse_workflow_call_body(self, i):
        """
        Required.

        :param i:
        :return:
        """
        io_map = OrderedDict()

        if isinstance(i, wdl_parser.Terminal):
            return i.source_string # no io mappings; represents just a blank call
        elif isinstance(i, wdl_parser.Ast):
            if i.name == 'CallBody':
                declarations = self.parse_workflow_call_body_declarations(i.attr("declarations")) # have not seen this used
                io_map = self.parse_workflow_call_body_io(i.attr('io'))
            else:
                raise NotImplementedError
        elif isinstance(i, wdl_parser.AstList):
            raise NotImplementedError

        return io_map


    def parse_workflow_call(self, i):
        '''
        Parses a WDL workflow call AST subtree to give the variable mappings for
        that particular job/task "call".

        :param i: WDL workflow job object
        :return: python dictionary of io mappings for that job call
        '''
        task_being_called = self.parse_workflow_call_taskname(i.attr("task"))
        task_alias = self.parse_workflow_call_taskalias(i.attr("alias"))
        io_map = self.parse_workflow_call_body(i.attr("body"))

        if not task_alias:
            task_alias = task_being_called

        return {'task': task_being_called, 'alias': task_alias, 'io': io_map}
