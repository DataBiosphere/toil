# Copyright (C) 2018 UCSC Computational Genomics Lab
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
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import json
import csv
import os
import logging

import toil.wdl.wdl_parser as wdl_parser

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

        self.output_file = os.path.join(self.output_directory,
                                        'toilwdl_compiled.py')

        # only json is required; tsv/csv are optional
        self.json_dict = {}
        self.tsv_dict = {}
        self.csv_dict = {}

        # holds task skeletons from WDL task objects
        self.tasks_dictionary = {}

        # holds workflow structure from WDL workflow objects
        self.workflows_dictionary = {}

        # unique iterator to add to cmd names
        self.command_number = 0

        # unique number for a job
        self.task_number = 0

        # a job's 'level' on the DAG
        self.task_priority = 0

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

    def create_tsv_array(self, tsv_filepath):
        '''
        Take a tsv filepath and return an array; e.g. [[],[],[]].

        For example, a file containing:

        1   2   3
        4   5   6
        7   8   9

        would return the array: [['1','2','3'], ['4','5','6'], ['7','8','9']]

        :param tsv_filepath:
        :return: tsv_array
        '''
        tsv_array = []
        with open(tsv_filepath, "r") as f:
            data_file = csv.reader(f, delimiter="\t")
            for line in data_file:
                tsv_array.append(line)
        return (tsv_array)

    def create_csv_array(self, csv_filepath):
        '''
        Take a csv filepath and return an array; e.g. [[],[],[]].

        For example, a file containing:

        1,2,3
        4,5,6
        7,8,9

        would return the array: [['1','2','3'], ['4','5','6'], ['7','8','9']]

        :param csv_filepath:
        :return: csv_array
        '''
        csv_array = []
        with open(csv_filepath, "r") as f:
            data_file = csv.reader(f)
            for line in data_file:
                csv_array.append(line)
        return (csv_array)

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
            d_list = d.split('.')
            self.json_dict[d_list[-1]] = data[d]
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
            var_name, var_type, var_value = self.parse_task_declaration(
                declaration_subAST)
            var_truple = (var_name, var_type, var_value)
            declaration_array.append(var_truple)
            self.tasks_dictionary.setdefault(task_name, {})[
                'inputs'] = declaration_array

        for section in task.attr("sections"):

            # task commandline entries section [command(s) to run]
            if section.name == "RawCommand":
                command_array = self.parse_task_rawcommand(section)
                self.tasks_dictionary.setdefault(task_name, {})[
                    'raw_commandline'] = command_array

            # task runtime section (docker image; disk; CPU; RAM; etc.)
            if section.name == "Runtime":
                runtime_array = self.parse_task_runtime(section)
                self.tasks_dictionary.setdefault(task_name, {})[
                    'runtime'] = runtime_array

            # task output filenames section (expected return values/files)
            if section.name == "Outputs":
                output_array = self.parse_task_outputs(section)
                self.tasks_dictionary.setdefault(task_name, {})[
                    'outputs'] = output_array

    def parse_task_declaration(self, declaration_subAST):
        '''
        Parses the declaration section of the WDL task AST subtree.

        So far tasks only contain stubs without value assignment, such as:

        String my_name
        String your_name
        Int two_chains_i_mean_names

        But in the future who knows.  The var_value variable below is a stub to
        potentially allow for variable assignment in the future.

        :param declaration_subAST: Some subAST representing a task declaration
                                   like: 'String file_name'
        :return: var_name, var_type, var_value
            Example:
                Input subAST representing:   'String file_name'
                Output:  var_name='file_name', var_type='String', var_value=None
        '''

        # variable name
        if declaration_subAST.attr("name"):
            if isinstance(declaration_subAST.attr("name"), wdl_parser.Terminal):
                var_name = declaration_subAST.attr("name").source_string
            elif isinstance(declaration_subAST.attr("name"), wdl_parser.Ast):
                raise NotImplementedError
            elif isinstance(declaration_subAST.attr("name"),
                            wdl_parser.AstList):
                raise NotImplementedError

        # variable type
        if declaration_subAST.attr("type"):

            # if the variable type is a primitive
            if isinstance(declaration_subAST.attr("type"), wdl_parser.Terminal):
                var_type = declaration_subAST.attr("type").source_string

            # if the variable type is not a primitive (i.e. an Array)
            elif isinstance(declaration_subAST.attr("type"), wdl_parser.Ast):

                if declaration_subAST.attr("type").attr("name"):
                    if isinstance(declaration_subAST.attr("type").attr("name"),
                                  wdl_parser.Terminal):
                        var_type = declaration_subAST.attr("type").attr(
                            "name").source_string
                    if isinstance(declaration_subAST.attr("type").attr("name"),
                                  wdl_parser.Ast):
                        raise NotImplementedError
                    if isinstance(declaration_subAST.attr("type").attr("name"),
                                  wdl_parser.AstList):
                        raise NotImplementedError

                # if the variable type goes deeper and is for instance: Array[Array[File]]
                if declaration_subAST.attr("type").attr("subtype"):
                    if isinstance(
                            declaration_subAST.attr("type").attr("subtype"),
                            wdl_parser.Terminal):
                        raise NotImplementedError
                    if isinstance(
                            declaration_subAST.attr("type").attr("subtype"),
                            wdl_parser.Ast):
                        raise NotImplementedError
                    if isinstance(
                            declaration_subAST.attr("type").attr("subtype"),
                            wdl_parser.AstList):
                        for subtype in declaration_subAST.attr("type").attr(
                                "subtype"):
                            var_type = var_type + subtype.source_string

            elif isinstance(declaration_subAST.attr("type"),
                            wdl_parser.AstList):
                raise NotImplementedError

        var_value = None  # placeholder to be implemented potentially later

        return var_name, var_type, var_value

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
            command_actions = {}

            # normal string
            if isinstance(code_snippet, wdl_parser.Terminal):
                command_var = code_snippet.source_string
                command_type = 'normal_string'

            # a variable like ${dinosaurDNA}
            if isinstance(code_snippet, wdl_parser.Ast):

                if isinstance(code_snippet.attributes["expr"],
                              wdl_parser.Terminal):
                    command_var = code_snippet.attributes["expr"].source_string
                    command_type = 'variable'
                if isinstance(code_snippet.attributes["expr"], wdl_parser.Ast):

                    if code_snippet.attributes["expr"].attributes['lhs']:
                        if isinstance(
                                code_snippet.attributes["expr"].attributes[
                                    'lhs'], wdl_parser.Terminal):
                            command_var = \
                                code_snippet.attributes["expr"].attributes[
                                    'lhs'].source_string
                            command_type = 'variable'
                        if isinstance(
                                code_snippet.attributes["expr"].attributes[
                                    'lhs'], wdl_parser.Ast):
                            raise NotImplementedError
                        if isinstance(
                                code_snippet.attributes["expr"].attributes[
                                    'lhs'], wdl_parser.Ast):
                            raise NotImplementedError

                    if code_snippet.attributes["expr"].attributes['rhs']:
                        raise NotImplementedError

                if isinstance(code_snippet.attributes["expr"],
                              wdl_parser.AstList):
                    raise NotImplementedError

                # variables with context like ${sep=" -V " GVCFs}
                if code_snippet.attributes['attributes']:
                    for additional_conditions in code_snippet.attributes[
                        'attributes']:
                        keyword_for_a_command = \
                            additional_conditions.attributes['key'].source_string
                        some_value_used_by_the_keyword = \
                            additional_conditions.attributes['value'].source_string
                        command_actions[
                            keyword_for_a_command] = some_value_used_by_the_keyword

            if isinstance(code_snippet, wdl_parser.AstList):
                raise NotImplementedError

            command_array.append((command_var,
                                  command_type,
                                  command_actions))
        return command_array

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
        # map
        runtime_attributes = []
        if isinstance(runtime_subAST.attr("map"), wdl_parser.Terminal):
            raise NotImplementedError
        elif isinstance(runtime_subAST.attr("map"), wdl_parser.Ast):
            raise NotImplementedError
        elif isinstance(runtime_subAST.attr("map"), wdl_parser.AstList):
            for mapping in runtime_subAST.attr("map"):
                if isinstance(mapping, wdl_parser.Terminal):
                    raise NotImplementedError
                elif isinstance(mapping, wdl_parser.Ast):
                    map_key = mapping.attr("key").source_string
                    map_value = mapping.attr("value").source_string
                    runtime_attributes.append((map_key, map_value))
                elif isinstance(mapping, wdl_parser.AstList):
                    raise NotImplementedError
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
                var_base_type = j.attributes['type']
                var_base_name = j.attributes['name']
                var_base_value = j.attributes['expression']

                var_name = self.parse_task_output_name(var_base_name)
                var_type = self.parse_task_output_type(var_base_type)
                var_value, var_actions = self.parse_task_output_value(
                    var_base_value)

                output_array.append(
                    (var_name, var_type, var_value, var_actions))
        return output_array

    def parse_task_output_name(self, base_name_AST):
        '''
        Discern a task output's var_name.

        Example:
        'Int num = 7'
            var_name: 'num'
            var_type: 'Int'
            var_value: 7

        :param base_name_AST: An AST subTree representing a task output's name.
        :return: var_name
        '''
        if isinstance(base_name_AST, wdl_parser.Terminal):
            # "txtFiles" for Array[File] txtFiles = glob(*.txt)
            var_name = base_name_AST.source_string
        if isinstance(base_name_AST, wdl_parser.Ast):
            raise NotImplementedError
        if isinstance(base_name_AST, wdl_parser.AstList):
            raise NotImplementedError

        return var_name

    def parse_task_output_type(self, base_type_AST):
        '''
        Discern a task output's var_type.

        Example:
        'Int num = 7'
            var_name: 'num'
            var_type: 'Int'
            var_value: 7

        :param base_type_AST: An AST subTree representing a task output's type.
        :return: var_type
        '''

        if isinstance(base_type_AST, wdl_parser.Terminal):
            # primitive_type: 'Boolean' | 'Int' | 'Float' | 'File' | 'String'
            var_type = base_type_AST.source_string
        if isinstance(base_type_AST, wdl_parser.Ast):
            # array_type: 'Array' '[' ($primitive_type | $object_type | $array_type) ']'
            # concatenate into type + subtype1 + subtype2 + ...
            if isinstance(base_type_AST.attributes['name'],
                          wdl_parser.Terminal):
                # Something like "Array" for Array[File] txtFiles = glob(*.txt)
                var_type = base_type_AST.attributes['name'].source_string
            if isinstance(base_type_AST.attributes['subtype'],
                          wdl_parser.AstList):
                for each_subtype in base_type_AST.attributes['subtype']:
                    # "File" for Array[File] txtFiles = glob(*.txt)
                    var_type = var_type + each_subtype.source_string
        if isinstance(base_type_AST, wdl_parser.AstList):
            raise NotImplementedError

        return var_type

    def parse_task_output_value(self, base_value_AST):
        '''
        Discern a task output's var_value.

        Example:
        'Int num = 7'
            var_name: 'num'
            var_type: 'Int'
            var_value: 7

        Sometimes this does not exist though, for example:
        'File x = stdout()'
            var_name: 'x'
            var_type: 'File'
            var_value:

        In which case, default to just ''.  'stdout' is added to the var actions.

        :param base_value_AST: An AST subTree representing a task output's value.
        :return var_value, var_action: The variable's declared value and any
                                       special actions that need to be taken.
        '''

        var_action = {}

        # a primitive var_value like '7' (shown above)
        if isinstance(base_value_AST, wdl_parser.Terminal):
            var_value = base_value_AST.source_string

        # this is not a primitive
        if isinstance(base_value_AST, wdl_parser.Ast):
            orderedDictOfVars = base_value_AST.attributes

            if 'name' in orderedDictOfVars:
                var_value_name = orderedDictOfVars['name']
                if isinstance(var_value_name, wdl_parser.Terminal):
                    var_action[var_value_name.source_string] = 'None'

            if 'params' in orderedDictOfVars:
                var_value_params = orderedDictOfVars['params']
                if isinstance(var_value_params, wdl_parser.AstList):
                    var_value = []
                    for param in var_value_params:
                        if isinstance(param, wdl_parser.Terminal):
                            var_value.append(param.source_string)

            # mostly determine actions for specific outputs
            if 'lhs' in orderedDictOfVars:
                var_value_lhs = base_value_AST.attributes['lhs']
                if isinstance(var_value_lhs, wdl_parser.Ast):
                    orderedDictOfVars = var_value_lhs.attributes
                    if 'name' in orderedDictOfVars:
                        var_value_name = orderedDictOfVars['name']
                        if isinstance(var_value_name, wdl_parser.Terminal):
                            var_action[var_value_name.source_string] = 'None'
                    if 'params' in orderedDictOfVars:
                        var_value_params = orderedDictOfVars['params']
                        if isinstance(var_value_params, wdl_parser.Terminal):
                            var_value = [var_value_params]
                        if isinstance(var_value_params, wdl_parser.AstList):
                            var_value = []
                            for param in var_value_params:
                                if isinstance(param, wdl_parser.Terminal):
                                    var_value.append(param.source_string)

            # this is not implemented at the moment, but later will be important
            # for returning index values and should be incorporated below for
            # 'ArrayOrMapLookup' and such-like.
            if 'rhs' in orderedDictOfVars:
                var_value_rhs = orderedDictOfVars['rhs']
                if isinstance(var_value_rhs, wdl_parser.Terminal):
                    raise NotImplementedError
                if isinstance(var_value_rhs, wdl_parser.Ast):
                    raise NotImplementedError
                if isinstance(var_value_rhs, wdl_parser.AstList):
                    raise NotImplementedError

            if base_value_AST.name == 'ArrayOrMapLookup':
                try:
                    index_value = base_value_AST.attributes['rhs'].source_string
                    var_action['index_lookup'] = index_value
                except:
                    raise NotImplementedError

        if not var_value:
            var_value = ''

        return var_value, var_action

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

        wf_declared_dict = {}
        for section in workflow.attr("body"):

            if section.name == "Declaration":
                var_name, var_map = self.parse_workflow_declaration(section)
                wf_declared_dict[var_name] = var_map
            self.workflows_dictionary.setdefault(workflow_name, {})[
                'wf_declarations'] = wf_declared_dict

            if section.name == "Scatter":
                self.parse_workflow_scatter(section, workflow_name)
                self.task_priority = self.task_priority + 1

            if section.name == "Call":
                self.task_priority = self.task_priority + 1
                self.task_number = self.task_number + 1
                task_being_called = section.attributes['task'].source_string
                if section.attributes['alias']:
                    task_alias = section.attributes['alias'].source_string
                else:
                    task_alias = task_being_called
                job = self.parse_workflow_call(section)
                self.workflows_dictionary.setdefault((self.task_priority,
                                                      self.task_number,
                                                      task_being_called,
                                                      task_alias), {})[
                    'job_declarations'] = job

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
        var_map = {}
        tsv = False
        if isinstance(wf_declaration_subAST.attr("type"), wdl_parser.Terminal):
            var_type = wf_declaration_subAST.attr("type").source_string
        elif isinstance(wf_declaration_subAST.attr("type"), wdl_parser.Ast):
            var_type = wf_declaration_subAST.attr("type").attributes[
                "name"].source_string
        elif isinstance(wf_declaration_subAST.attr("type"), wdl_parser.AstList):
            raise NotImplementedError
        var_name = wf_declaration_subAST.attr("name").source_string

        # only read_tsv currently supported
        tsv_array = []
        if wf_declaration_subAST.attr("expression"):
            wdl_function_called = \
                wf_declaration_subAST.attr("expression").attributes[
                    'name'].source_string
            if wdl_function_called == 'read_tsv':
                # get all params for 'read_tsv'
                # expecting one file name pointing to a path in the JSON/YML secondary file
                for j in wf_declaration_subAST.attr("expression").attributes[
                    'params']:
                    filename = j.source_string
                    tsv_filepath = self.json_dict[filename]
                    tsv_array = self.create_tsv_array(tsv_filepath)
                    self.tsv_dict[var_name] = tsv_array
                    tsv = True

        if var_name in self.json_dict:
            var_value = self.json_dict[var_name]
        # deal with arrays other than tsv files
        elif var_type == 'Array':
            pass
        else:
            raise RuntimeError(
                'Variable in workflow declarations not found in secondary file.')

        if tsv:
            var_map['type'] = var_type
            var_map['value'] = tsv_array
        else:
            var_map['type'] = var_type
            var_map['value'] = var_value

        return var_name, var_map

    def parse_workflow_scatter(self, section, workflow_name):
        # name of iterator; e.g. 'sample'
        scatter_counter = section.attributes['item'].source_string

        # name of collection to iterate over
        scatter_collection = section.attributes['collection'].source_string

        self.workflows_dictionary.setdefault('scatter_calls', {})[
            scatter_collection] = scatter_counter

        if scatter_collection in self.workflows_dictionary[workflow_name][
            'wf_declarations']:
            if self.workflows_dictionary[workflow_name]['wf_declarations'][
                scatter_collection]['type'] == 'Array':
                scatter_array = \
                    self.workflows_dictionary[workflow_name]['wf_declarations'][
                        scatter_collection]['value']
                self.parse_workflow_scatter_array(section, scatter_array)
            else:
                raise RuntimeError(
                    'Scatter failed.  Scatter collection is not an array.')
        else:
            raise RuntimeError(
                'Scatter failed.  Scatter collection not found in workflows_dictionary.')

    def parse_workflow_scatter_array(self, section, scatter_array):
        scatter_num = 0
        for set_of_vars in scatter_array:
            for j in section.attributes['body']:
                self.task_number = self.task_number + 1
                task_being_called = j.attributes['task'].source_string
                if j.attributes['alias']:
                    task_alias = j.attributes['alias'].source_string
                else:
                    task_alias = task_being_called
                job = self.parse_workflow_call(j, scatter_num=str(scatter_num))
                self.workflows_dictionary.setdefault((self.task_priority,
                                                      self.task_number,
                                                      task_being_called,
                                                      task_alias), {})[
                    'job_declarations'] = job
            scatter_num = scatter_num + 1

    def parse_workflow_call(self, i, scatter_num=None):
        '''
        Parses a WDL workflow call AST subtree to give the variable mappings for
        that particular job/task "call".

        :param i: WDL workflow job object
        :return: python dictionary of io mappings for that job call
        '''
        io_map = {}

        if i.attributes['body']:
            if i.attributes['body'].attributes['io']:
                for g in i.attributes['body'].attributes['io']:
                    for k in g.attributes['map']:
                        if isinstance(k.attributes['key'], wdl_parser.Terminal):
                            key_name = k.attributes['key'].source_string
                        if isinstance(k.attributes['value'],
                                      wdl_parser.Terminal):
                            value_name = k.attributes['value'].source_string
                            value_type = k.attributes['value'].str
                        if isinstance(k.attributes['key'], wdl_parser.Ast):
                            raise NotImplementedError
                        if isinstance(k.attributes['value'], wdl_parser.Ast):
                            if k.attributes['value'].attributes[
                                'rhs'].str == 'integer':
                                output_variable = \
                                    k.attributes['value'].attributes[
                                        'rhs'].source_string
                                task = k.attributes['value'].attributes[
                                    'lhs'].source_string
                                if scatter_num:
                                    value_name = task + '[' + scatter_num + '][' + output_variable + ']'
                                else:
                                    value_name = task + '[' + output_variable + ']'
                                value_type = 'index_value'
                            elif k.attributes['value'].attributes[
                                'rhs'].str == 'identifier':
                                output_variable = \
                                    k.attributes['value'].attributes[
                                        'rhs'].source_string
                                task = k.attributes['value'].attributes[
                                    'lhs'].source_string
                                value_name = task + ' ' + output_variable
                                value_type = 'output'
                            else:
                                raise RuntimeError('Unsupported rhs type.')

                        io_map.setdefault(key_name, {})['name'] = value_name
                        io_map.setdefault(key_name, {})['type'] = value_type
        return (io_map)
