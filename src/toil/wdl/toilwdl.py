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

import fnmatch
import argparse
import json
import csv
import os
import collections
import subprocess
import logging
import textwrap

import toil.wdl.wdl_parser as wdl_parser

wdllogger = logging.getLogger(__name__)


class ToilWDL:
    '''
    A program to run WDL input files using native Toil scripts.

    Requires a WDL file, and a JSON file.  The WDL file contains ordered commands,
    and the JSON file contains input values for those commands.  To run in Toil,
    these two files must be parsed, restructured into python dictionaries, and
    then compiled into a Toil formatted python script.  This compiled Toil script
    is deleted after running unless the user specifies: "--dont_delete_compiled"
    as an option.

    The WDL parser was auto-generated from the Broad's current WDL grammar file:
    https://github.com/openwdl/wdl/blob/master/parsers/grammar.hgr
    using Scott Frazer's Hermes: https://github.com/scottfrazer/hermes
    Thank you Scott Frazer!

    Currently in alpha testing, and known to work with the Broad's GATK tutorial
    set for WDL on their main wdl site:
    software.broadinstitute.org/wdl/documentation/topic?name=wdl-tutorials

    And ENCODE's WDL workflow:
    github.com/ENCODE-DCC/pipeline-container/blob/master/local-workflows/encode_mapping_workflow.wdl

    Additional support to be broadened to include more features soon.
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

    def write_modules(self):
        # string used to write imports to the file
        module_string = heredoc_wdl('''
                    from toil.job import Job
                    from toil.common import Toil
                    from toil.lib.docker import apiDockerCall
                    from toil.wdl.toilwdl import generate_docker_bashscript_file
                    from toil.wdl.toilwdl import recursive_glob
                    import fnmatch
                    import subprocess
                    import os
                    import errno
                    import glob
                    import time
                    import shutil
                    import shlex
                    import uuid
                    import logging

                    logger = logging.getLogger(__name__)


                        ''')
        return module_string

    def write_main(self):
        '''
        Writes out a huge string representing the main section of the python
        compiled toil script.

        Currently looks at and writes 5 sections:
        1. JSON Variables (includes importing and preparing files as tuples)
        2. TSV Variables (includes importing and preparing files as tuples)
        3. CSV Variables (includes importing and preparing files as tuples)
        4. Wrapping each WDL "task" function as a toil job
        5. List out children and encapsulated jobs by priority, then start job0.

        This should create variable declarations necessary for function calls.
        Map file paths appropriately and store them in the toil fileStore so
        that they are persistent from job to job.  Create job wrappers for toil.
        And finally write out, and run the jobs in order of priority using the
        addChild and encapsulate commands provided by toil.

        :return: giant string containing the main def for the toil script.
        '''

        main_section = ''

        # write out the JSON/YML file declarations
        main_header = self.write_main_header()
        main_section = main_section + main_header

        # write out the JSON/YML file declarations
        main_section = main_section + '\n\n        # JSON Variables\n'
        scatter_vars_to_write = self.write_main_JSON()
        main_section = main_section + scatter_vars_to_write

        # write out TSV variable declarations
        main_section = main_section + '\n\n        # TSV Variables\n'
        scatter_vars_to_write = self.write_main_arrayarrayfile(self.tsv_dict)
        main_section = main_section + scatter_vars_to_write

        # write out CSV variable declarations
        main_section = main_section + '\n\n        # CSV Variables\n'
        scatter_vars_to_write = self.write_main_arrayarrayfile(self.csv_dict)
        main_section = main_section + scatter_vars_to_write

        # write toil job wrappers with input vars
        jobs_to_write = self.write_main_jobwrappers()
        main_section = main_section + jobs_to_write

        # write toil job calls
        jobs_to_write = self.write_main_jobcalls()
        main_section = main_section + jobs_to_write

        # write toil job stats
        jobs_to_write = self.write_main_stats()
        main_section = main_section + jobs_to_write

        return main_section

    def write_main_header(self):
        log_dir = os.path.join(self.output_directory, "wdl-stats.log")
        main_header_dict = {"log_dir": log_dir}
        main_header = heredoc_wdl('''

            if __name__=="__main__":
                options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
                with Toil(options) as toil:
                    start = time.time()
                    with open("{log_dir}", "a+") as f:
                        f.write("Starting WDL Job @ " + str(time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())) + "\\n\\n")

            ''', main_header_dict)
        return main_header

    def write_main_arrayarrayfile(self, aaf_dict):
        '''
        Writes a loop used to import files from Array[Array[File]] type
        objects (typically created by csv and tsv files).

        :return: A string representing this loop.
        '''
        main_section = ''
        for aaf in aaf_dict:
            if aaf in self.workflows_dictionary['scatter_calls']:
                iterator = self.workflows_dictionary['scatter_calls'][aaf]

                arrayarray_dict = {"aaf": aaf,
                                   "iterator": iterator,
                                   "aaf_value": str(self.tsv_dict[aaf])}
                arrayarray_loop = heredoc_wdl('''
                        {aaf} = []
                        {aaf}0 = {aaf_value}
                        for {iterator}0 in {aaf}0:
                            {iterator} = []
                            for i in {iterator}0:
                                if os.path.isfile(str(i)):
                                    {iterator}0 = toil.importFile("file://" + os.path.abspath(i))
                                    {iterator}0_preserveThisFilename = os.path.basename(i)
                                    {iterator}.append(({iterator}0, {iterator}0_preserveThisFilename))
                                else:
                                    {iterator}.append(i)
                            {aaf}.append({iterator})''', arrayarray_dict,
                                              indent='        ')
                main_section = main_section + arrayarray_loop
        # write for docker as well
        return main_section

    def write_main_JSON(self):
        '''
        Writes file imports and declared variables from the secondary JSON file.
        :return: A string representing these file imports and declared variables.
        '''
        main_section = ''

        input_dict = self.json_dict
        for dict_var in input_dict:
            v = input_dict[dict_var]
            # WDL sometimes supplies a list of file paths
            # later potentially implement a catch for a list of lists
            if type(v) is list:
                list_iterator = 0
                for item in v:
                    importFile_section = self.write_main_importFile(item,
                                                                    dict_var,
                                                                    list_iterator)
                    main_section = main_section + importFile_section
                    list_iterator = list_iterator + 1
                list_iterator = 0
                if os.path.isfile(v[0]):
                    main_section = main_section + '        ' + dict_var + ' = ['
                    for item in v:
                        main_section = main_section + '(' + dict_var + str(list_iterator) + ', ' + dict_var + str(list_iterator) + '_preserveThisFilename), '
                        list_iterator = list_iterator + 1
                    if main_section.endswith(', '):
                        main_section = main_section[:-2]
                    main_section = main_section + ']\n'
                else:
                    main_section = main_section + '        ' + dict_var + ' = ['
                    for item in v:
                        main_section = main_section + '(' + dict_var + str(list_iterator) + '), '
                    if main_section.endswith(', '):
                        main_section = main_section[:-2]
                    main_section = main_section + ']\n'
            else:
                main_section = main_section + self.write_main_importFile(v, dict_var)
        return main_section

    def write_main_importFile(self, item, input_var, list_iterator=None):
        '''
        Writes file imports and declared variables.

        :param item:
        :param input_var:
        :param list_iterator:
        :return: A string representing these file imports and declared variables.
        '''
        main_section = ''

        # if it's a file, then import and save the original filename
        if os.path.isfile(str(item)) or os.path.isfile(str(os.path.join(os.getcwd(), item))):
            filename = os.path.basename(item)
            if list_iterator is None:
                main_section = main_section + '        ' + input_var + '0 = toil.importFile("file://' + os.path.abspath(item) + '")\n'
                main_section = main_section + '        ' + input_var + '0_preserveThisFilename = "' + filename + '"\n'
                main_section = main_section + '        ' + input_var + ' = (' + input_var + '0, ' + input_var + '0_preserveThisFilename)\n'
            else:
                main_section = main_section + '        ' + input_var + str(
                    list_iterator) + ' = toil.importFile("file://' + os.path.abspath(
                    item) + '")\n'
                main_section = main_section + '        ' + input_var + str(
                    list_iterator) + '_preserveThisFilename = "' + filename + '"\n'
        # elif string, add quotes
        elif isinstance(item, (str, unicode)):
            if list_iterator is None:
                main_section = main_section + '        ' + input_var + ' = "' + item + '"\n'
            else:
                main_section = main_section + '        ' + input_var + str(
                    list_iterator) + ' = "' + item + '"\n'
        # otherwise, just simply declare the variable
        else:
            if list_iterator is None:
                main_section = main_section + '        ' + input_var + ' = ' + item + '\n'
            else:
                main_section = main_section + '        ' + input_var + str(list_iterator) + ' = ' + item + '\n'
        return main_section

    def write_main_jobwrappers(self):
        '''
        Writes out 'jobs' as wrapped toil objects in preparation for calling.

        :return: A string representing this.
        '''
        main_section = ''

        # toil cannot technically start with multiple jobs, so an empty
        # 'initialize_jobs' function is always called first to get around this
        main_section = main_section + '\n        job0 = Job.wrapJobFn(initialize_jobs)\n'

        # declare each job in main as a wrapped toil function in order of priority
        job_declaration_dict = self.mk_ordered_dict_of_all_job_input_params()
        for job_wrap in job_declaration_dict:
            main_section = main_section + '        ' + job_wrap + ' = Job.wrapJobFn('
            for var in job_declaration_dict[job_wrap]:
                main_section = main_section + var + ', '
            main_section = main_section[:-2]
            main_section = main_section + ')\n'
        main_section = main_section + '\n'

        return main_section

    def write_main_jobcalls(self):
        '''
        Writes out 'job' calls in order of priority.

        :return: A string representing this.
        '''
        main_section = ''
        skip_first = 1

        for priority in range(self.task_priority + 1):
            for job_declaration in self.workflows_dictionary:
                if isinstance(job_declaration, (list, tuple)):
                    if job_declaration[0] == priority:
                        main_section = main_section + '        job0.addChild(job' + str(
                            job_declaration[1]) + ')\n'
            if skip_first == 0:
                main_section = main_section + '\n        job0 = job0.encapsulate()\n'
            skip_first = 0
        if main_section.endswith('\n        job0 = job0.encapsulate()\n'):
            main_section = main_section[:-34]
        main_section = main_section + '        toil.start(job0)\n\n'
        return main_section

    def write_main_stats(self):
        '''
        Writes statements giving a runtime to output_directory/wdl-stats.log.

        :return: A string containing this.
        '''
        log_dir = os.path.join(self.output_directory, "wdl-stats.log")
        main_section_dict = {"log_dir": log_dir}
        main_section = heredoc_wdl('''
                end = time.time()
                with open("{log_dir}", "a+") as f:
                    f.write("Ending WDL Job @ " + str(time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())))
                    f.write("\\n")
                    f.write("Total runtime: %2.2f sec" % (end - start))
                    f.write("\\n\\n")
                    f.write("\\n" + "-"*80 + "\\n")''', main_section_dict,
                                   indent='        ')
        return main_section

    def write_functions(self):
        '''
        Writes out a python function for each WDL "task" object.

        :return: a giant string containing the meat of the job defs.
        '''

        # toil cannot technically start with multiple jobs, so an empty
        # 'initialize_jobs' function is always called first to get around this
        fn_section = "def initialize_jobs(job):\n" + \
                     "    job.fileStore.logToMaster('''initialize_jobs''')\n\n"

        list_of_jobs_to_write = self.return_one_job_per_priority()

        for job in list_of_jobs_to_write:
            needs_docker = self.determine_if_docker_job(job)
            if needs_docker:
                function_to_write = self.write_docker_function(job)
            else:
                function_to_write = self.write_nondocker_function(job)

            fn_section = fn_section + function_to_write

        return fn_section

    def write_nondocker_function(self, job):
        '''
        Writes out a python function for each WDL "task" object.

        Each python function is a unit of work written out as a string in
        preparation to being written out to a file.  In WDL, each "job" is
        called a "task".  Each WDL task is written out in multiple steps:

        1: Header and inputs (e.g. 'def mapping(self, input1, input2)')
        2: Log job name (e.g. 'job.fileStore.logToMaster('initialize_jobs')')
        3: Create temp dir (e.g. 'tempDir = job.fileStore.getLocalTempDir()')
        4: import filenames and use readGlobalFile() to get files from the
           jobStore
        5: Reformat commandline variables (like converting to ' '.join(files)).
        6: Commandline call using subprocess.Popen().
        7: Write the section returning the outputs.  Also logs stats.

        :return: a giant string containing the meat of the job defs for the toil script.
        '''

        fn_section = ''

        job_priority = job[0]
        job_number = job[1]
        job_task_reference = job[2]  # default name
        job_alias = job[
            3]  # reassigned name (optional; default if not assigned)

        # get all variable declarations for this particular job
        job_declaration_array = self.get_job_declarations(job)

        # write the function header
        function_header = self.write_function_header(job, job_declaration_array)
        fn_section = fn_section + function_header

        # log to toil which job is being run when this function is called
        fn_start_dict = {"job_alias": job_alias}
        fn_start = heredoc_wdl('''
                                 job.fileStore.logToMaster("{job_alias}")
                                 start = time.time()

                                 tempDir = job.fileStore.getLocalTempDir()

                                 ''', fn_start_dict, indent='    ')
        fn_section = fn_section + fn_start

        # import files into the job store using readGlobalFile()
        readglobalfiles_declarations = self.write_function_readglobalfiles(job,
                                                                           job_declaration_array)
        fn_section = fn_section + readglobalfiles_declarations

        # write out commandline keywords
        cmdline = self.write_function_cmdvarprep(job, docker=False)
        fn_section = fn_section + cmdline

        # write out commandline keywords
        cmdline = self.write_function_cmdline(job, docker=False)
        fn_section = fn_section + cmdline

        subprocesspopen = self.write_function_subprocesspopen(job)
        fn_section = fn_section + subprocesspopen

        # write the outputs for the definition to return
        return_outputs = self.write_function_outputreturn(job,
                                                          job_task_reference)
        fn_section = fn_section + return_outputs

        return fn_section

    def write_docker_function(self, job):
        '''
        Writes out a python function for each WDL "task" object.

        Each python function is a unit of work written out as a string in
        preparation to being written out to a file.  In WDL, each "job" is
        called a "task".  Each WDL task is written out in multiple steps:

        1: Header and inputs (e.g. 'def mapping(self, input1, input2)')
        2: Log job name (e.g. 'job.fileStore.logToMaster('initialize_jobs')')
        3: Create temp dir (e.g. 'tempDir = job.fileStore.getLocalTempDir()')
        4: Make a new folder for the execution to take place in
        5: import filenames and use readGlobalFile() to get files from the
           jobStore
        6: Write the line to create a bashscript file.
        6: Reformat commandline variables (like converting to '/data/' + file).
        7: apiDockerCall() to run docker.
        8: Write the section returning the outputs.  Also logs stats.

        :return: a giant string containing the meat of the job defs for the toil script.
        '''

        fn_section = ''

        job_priority = job[0]
        job_number = job[1]
        job_task_reference = job[2]  # default name
        job_alias = job[
            3]  # reassigned name (optional; default if not assigned)

        # get all variable declarations for this particular job
        job_declaration_array = self.get_job_declarations(job)

        # write the function header
        function_header = self.write_function_header(job, job_declaration_array)
        fn_section = fn_section + function_header

        # log to toil which job is being run when this function is called
        fn_start_dict = {"job_alias": job_alias}
        fn_start = heredoc_wdl('''
                                 job.fileStore.logToMaster("{job_alias}")
                                 start = time.time()

                                 tempDir = job.fileStore.getLocalTempDir()

                                 try:
                                     os.makedirs(tempDir + '/execution')
                                 except OSError as e:
                                     if e.errno != errno.EEXIST:
                                         raise''', fn_start_dict, indent='    ')
        fn_section = fn_section + fn_start

        # import files into the job store using readGlobalFile()
        readglobalfiles_declarations = self.write_function_readglobalfiles(job,
                                                                           job_declaration_array)
        fn_section = fn_section + readglobalfiles_declarations

        # prep Array[File] commandline keywords
        cmdline = self.write_function_cmdvarprep(job, docker=True)
        fn_section = fn_section + cmdline

        # write out commandline keywords
        cmdline = self.write_function_cmdline(job, docker=True)
        fn_section = fn_section + cmdline

        bashscriptline = self.write_function_bashscriptline(job_task_reference,
                                                            job_alias)
        fn_section = fn_section + bashscriptline

        docker_image = self.get_docker_image(job_task_reference)

        dockercall = self.write_function_dockercall(job_task_reference,
                                                    docker_image)
        fn_section = fn_section + dockercall

        # write the outputs for the definition to return
        return_outputs = self.write_function_outputreturn(job,
                                                          job_task_reference)
        fn_section = fn_section + return_outputs

        return fn_section

    def write_function_header(self, job, job_declaration_array):
        '''
        Writes the header that starts each function, for example, this function
        can write and return:

        'def write_function_header(self, job, job_declaration_array):'

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param job_declaration_array: A list of all inputs that job requires.
        :return: A string representing this.
        '''
        job_alias = job[3]

        fn_section = ''

        fn_section = fn_section + '\n\ndef ' + job_alias + '(job, '
        for job_declaration in job_declaration_array:
            job_declaration_name = job_declaration[0]
            fn_section = fn_section + job_declaration_name + ', '
        fn_section = fn_section[:-2]
        fn_section = fn_section + '):\n'

        return fn_section

    def write_function_readglobalfiles(self, job, job_declaration_array):
        '''
        Writes all job.fileStore.readGlobalFile() declarations needed to get
        files from the job store.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param job_declaration_array: A list of all inputs that job requires.
        :return: A string representing this.
        '''
        fn_section = ''
        for job_declaration in job_declaration_array:
            job_declaration_name = job_declaration[0]
            job_declaration_type = job_declaration[1]
            job_declaration_key = None
            if job_declaration_type == 'File':
                job_declaration_key, parent_job = self.if_output_mk_a_key(job,
                                                                          job_declaration_name)
                jobdecl_dict = {"job_declaration_name": job_declaration_name,
                                "job_declaration_key": job_declaration_key}
                if job_declaration_key:
                    jobdecl = heredoc_wdl('''
                        try:
                            {job_declaration_name}_fs = job.fileStore.readGlobalFile({job_declaration_name}["{job_declaration_key}"][0], userPath=os.path.join(tempDir, {job_declaration_name}["{job_declaration_key}"][1]))
                        except:
                            {job_declaration_name}_fs = os.path.join(tempDir, {job_declaration_name}["{job_declaration_key}"][1])

                    ''', jobdecl_dict, indent='    ')
                else:
                    jobdecl = heredoc_wdl('''
                        try:
                            {job_declaration_name}_fs = job.fileStore.readGlobalFile({job_declaration_name}[0], userPath=os.path.join(tempDir, {job_declaration_name}[1]))
                        except:
                            {job_declaration_name}_fs = os.path.join(tempDir, {job_declaration_name}[1])

                    ''', jobdecl_dict, indent='    ')
                fn_section = fn_section + jobdecl
            if job_declaration_type == 'ArrayFile':
                # these are handled in write_function_cmdvarprep()
                pass

        return fn_section

    def write_function_bashscriptline(self, job_task_reference, job_alias):
        '''
        Writes a function to create a bashscript for injection into the docker
        container.

        :param job_task_reference: The job referenced in WDL's Task section.
        :param job_alias: The actual job name to be written.
        :return: A string writing all of this.
        '''
        fn_section = "    generate_docker_bashscript_file(temp_dir=tempDir, docker_dir='/data', globs=["
        if self.tasks_dictionary[job_task_reference]['outputs']:
            for output in self.tasks_dictionary[job_task_reference]['outputs']:
                if output[1] == 'ArrayFile' or 'File':
                    output_filename = output[2][0]
                    fn_section = fn_section + "'" + output_filename + "', "
                else:
                    raise NotImplementedError
            if fn_section.endswith(', '):
                fn_section = fn_section[:-2]
        fn_section = fn_section + "], cmd=cmd, job_name='" + str(
            job_alias) + "')\n"
        fn_section = fn_section + '\n'

        return fn_section

    def write_function_dockercall(self, job_task_reference, docker_image):
        '''
        Writes a string containing the apiDockerCall() that will run the job.

        :param job_task_reference: The name of the job calling docker.
        :param docker_image: The corresponding name of the docker image.
                                                            e.g. "ubuntu:latest"
        :return: A string containing the apiDockerCall() that will run the job.
        '''
        docker_dict = {"docker_image": docker_image,
                       "job_task_reference": job_task_reference
                       }
        docker_template = heredoc_wdl('''
        apiDockerCall(job, 
                      image="{docker_image}", 
                      working_dir=tempDir, 
                      parameters=["/data/{job_task_reference}_script.sh"], 
                      entrypoint="/bin/bash", 
                      volumes={{tempDir: {{"bind": "/data"}}}})

            ''', docker_dict, indent='    ')

        return docker_template

    def write_function_cmdvarprep(self, job, docker=False):
        '''
        Finds ArrayFiles that need to be reformatted, as per sep=' '.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :return: A string containing this.
        '''

        fn_section = ''
        job_task_reference = job[2]
        for cmd_name, cmd_type, cmd_actions_dict in \
        self.tasks_dictionary[job_task_reference]['raw_commandline']:
            for input in self.tasks_dictionary[job_task_reference]['inputs']:
                input_var_name = input[0]
                input_var_type = input[1]
                if cmd_name == input_var_name:
                    if input_var_type == 'ArrayFile':
                        job_declaration_key, parent_job = self.if_output_mk_a_key(
                            job, input_var_name)
                        if job_declaration_key:
                            called_multiple_times = self.determine_if_called_multitimes(
                                parent_job)
                        else:
                            called_multiple_times = False
                        if 'sep' in cmd_actions_dict:
                            fn_section = fn_section + \
                                         self.write_function_cmdvarprep_arrayfile(
                                             sep=True,
                                             sep_var=cmd_actions_dict['sep'],
                                             docker=docker,
                                             job_declaration_key=job_declaration_key,
                                             cmd_name=cmd_name,
                                             called_multiple_times=called_multiple_times)
                        else:
                            fn_section = fn_section + \
                                         self.write_function_cmdvarprep_arrayfile(
                                             sep=False,
                                             sep_var=None,
                                             docker=docker,
                                             job_declaration_key=job_declaration_key,
                                             cmd_name=cmd_name,
                                             called_multiple_times=called_multiple_times)
        return fn_section

    def write_function_cmdvarprep_arrayfile(self,
                                            sep,
                                            sep_var,
                                            docker,
                                            job_declaration_key,
                                            cmd_name,
                                            called_multiple_times):
        '''
        For all Array[File] inputs, there are a couple of recipes to import each
        of the files inside into the toil jobStore properly, and this function
        handles writing those.

        :param sep: Whether an array should be concatenated into a large string
                    with some separator.  Equivalent to ' '.join(filearray).
        :param sep_var: The string separator used to join the array of strings.
                        i.e. ' -V ' for something like ' -V '.join(filearray)
        :param docker: If this is a docker function, in which case the filepaths
                       need to begin with the default of '/data'.
        :param job_declaration_key:
        :param cmd_name:
        :return: A string writing all of this.
        '''
        fn_section = ''

        if job_declaration_key:
            formatted_key = '["' + job_declaration_key + '"]'
        else:
            formatted_key = ''

        if called_multiple_times:
            formatted_key = ''
            multicall_key = '["' + job_declaration_key + '"]'
        else:
            multicall_key = ''

        if docker:
            path_appended = '"/data/" + i[1]'
        else:
            path_appended = 'j'

        if sep:
            fn_section = fn_section + '    ' + cmd_name + '_list = []\n'
        fn_section = fn_section + '    for i in ' + cmd_name + formatted_key + ':\n'
        fn_section = fn_section + '        try:\n'
        fn_section = fn_section + '            j = job.fileStore.readGlobalFile(i' + multicall_key + '[0], userPath=os.path.join(tempDir, i' + multicall_key + '[1]))\n'
        if sep:
            fn_section = fn_section + '            ' + cmd_name + '_list.append(' + path_appended + ')\n'
        fn_section = fn_section + '        except:\n'
        fn_section = fn_section + '            j = os.path.join(tempDir, i' + multicall_key + '[1])\n'
        if sep:
            fn_section = fn_section + '            ' + cmd_name + '_list.append(' + path_appended + ')\n'
            fn_section = fn_section + '    ' + cmd_name + '_sep = "' + str(
                sep_var) + '".join(' + cmd_name + '_list)\n\n'
        return fn_section

    def write_function_cmdline(self, job, docker):
        '''
        Write a series of commandline variables to be concatenated together
        eventually and either called with subprocess.Popen() or with
        apiDockerCall() if a docker image is called for.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :return: A string representing this.
        '''

        fn_section = ''
        command_var_decl_array = []
        job_task_reference = job[2]
        for cmd in self.tasks_dictionary[job_task_reference]['raw_commandline']:
            cmd_name = cmd[0]
            cmd_type = cmd[1]
            cmd_actions_dict = cmd[2]
            command_var_decl = 'command' + str(self.command_number)
            if cmd_type == 'variable':
                job_declaration_key, parent_job = self.if_output_mk_a_key(job,
                                                                          cmd_name)
                if job_declaration_key:
                    output_dict_key = '["' + job_declaration_key + '"]'
                else:
                    output_dict_key = ''
                for input in self.tasks_dictionary[job_task_reference][
                    'inputs']:
                    input_var_name = input[0]
                    input_var_type = input[1]
                    if cmd_name == input_var_name:

                        if input_var_type == 'File':
                            if docker:
                                fn_section = fn_section + '    ' + command_var_decl + ' = "/data/" + ' + cmd_name + output_dict_key + '[1]\n'
                            else:
                                fn_section = fn_section + '    ' + command_var_decl + ' = ' + cmd_name + '_fs\n'

                        elif input_var_type == 'ArrayFile':
                            if 'sep' in cmd_actions_dict:
                                fn_section = fn_section + '    ' + command_var_decl + ' = ' + cmd_name + '_sep\n'
                            else:
                                fn_section = fn_section + '    ' + command_var_decl + ' = ' + cmd_name + '\n'
                        else:
                            fn_section = fn_section + '    ' + command_var_decl + ' = ' + cmd_name + '\n'

            if cmd_type == 'normal_string':
                fn_section = fn_section + '    ' + command_var_decl + " = '''" + cmd_name + "'''\n"
            self.command_number = self.command_number + 1
            command_var_decl_array.append(command_var_decl)

        fn_section = fn_section + '\n    cmd = '
        for command in command_var_decl_array:
            fn_section = fn_section + command + ' + '
        if fn_section.endswith(' + '):
            fn_section = fn_section[:-3]
        fn_section = fn_section + '\n\n'

        return fn_section

    def write_function_subprocesspopen(self, job):
        '''
        Write a subprocess.Popen() call for this function and write it out as a
        string.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :return: A string representing this.
        '''
        fn_section = heredoc_wdl('''
                this_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                this_process.communicate()
            ''', indent='    ')

        return fn_section

    def write_function_outputreturn(self, job, job_task_reference):
        '''
        Find the output values that this function needs and write them out as a
        string.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param job_task_reference: The name of the job to look up values for.
        :return: A string representing this.
        '''

        fn_section = ''
        if 'outputs' in self.tasks_dictionary[job_task_reference]:
            files_to_return = []
            glob = False
            for output in self.tasks_dictionary[job_task_reference]['outputs']:
                output_name = output[0]
                output_type = output[1]
                output_value = output[2]
                output_action_dict = output[3]

                if output_value != '':
                    if 'index_lookup' in output_action_dict:
                        suffix = '_il'
                    else:
                        suffix = ''

                    if 'glob' in output_action_dict:

                        glob_dict = {
                            "output_name": output_name,
                            "suffix": suffix,
                            "out_value": output_value[0],
                            "out_dir": self.output_directory}
                        glob_template = heredoc_wdl('''
                            {output_name}{suffix} = []
                            for x in recursive_glob(job, directoryname=tempDir, glob_pattern="{out_value}"):
                                output_file = job.fileStore.writeGlobalFile(x)
                                output_filename = os.path.basename(x)
                                job.fileStore.exportFile(output_file, "file://{out_dir}/" + output_filename)
                                {output_name}{suffix}.append((output_file, output_filename))

                            ''', glob_dict, indent='    ')
                        fn_section = fn_section + glob_template

                        if 'index_lookup' in output_action_dict:
                            index_dict = {
                                "output_name": output_name,
                                "suffix": suffix,
                                "index_num": str(
                                    output_action_dict['index_lookup'])}
                            index_template = heredoc_wdl('''
                                {output_name} = {output_name}{suffix}[{index_num}]
                                ''', index_dict, indent='    ')
                            fn_section = fn_section + index_template.format(
                                **index_dict)

                        else:
                            fn_section = fn_section + '\n'
                        files_to_return.append(output_name)
                    else:
                        nonglob_dict = {
                            "formatted_output_filename": self.translate_wdl_string_to_python_string(
                                job, output_value),
                            "output_name": output_name,
                            "out_dir": self.output_directory}
                        nonglob_template = heredoc_wdl('''
                            output_filename = {formatted_output_filename}
                            output_file = job.fileStore.writeGlobalFile(output_filename)
                            job.fileStore.exportFile(output_file, "file://{out_dir}/" + output_filename)
                            {output_name} = (output_file, output_filename)

                        ''', nonglob_dict, indent='    ')
                        fn_section = fn_section + nonglob_template
                        files_to_return.append(output_name)

            if files_to_return:
                fn_section = fn_section + '    rvDict = {'
            for file in files_to_return:
                fn_section = fn_section + '"' + file + '": ' + file + ', '
            if fn_section.endswith(', '):
                fn_section = fn_section[:-2]
            if files_to_return:
                fn_section = fn_section + '}\n\n'

            # only for logging stats
            log_dir = os.path.join(self.output_directory, "wdl-stats.log")
            stats_dict = {"log_dir": log_dir,
                          "job_name": job[3]}
            stats_template = heredoc_wdl('''
                    end = time.time()
                    with open("{log_dir}", "a+") as f:
                        f.write(str("{job_name}") + " now being run.")
                        f.write("\\n\\n")
                        f.write("Outputs:\\n")
                        for rv in rvDict:
                            f.write(str(rv) + ": " + str(rvDict[rv]))
                            f.write("\\n")
                        f.write("Total runtime: %2.2f sec" % (end - start))
                        f.write("\\n\\n")
                ''', stats_dict, indent='    ')
            fn_section = fn_section + stats_template

            if files_to_return:
                fn_section = fn_section + '    return rvDict\n\n'

        return fn_section

    def if_output_mk_a_key(self, job, job_declaration_name):
        '''
        An input variable for a job may be called "GVCFs", but the output that
        generates it may have called it "gvcf" and this function fetches that
        output's old name.

        This is important because all outputs are packaged as a dictionary of
        outputs, where individual values are extracted using the original output
        name as a key.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param input_var_name: The name of the input to look up values for.
        :return dict_output_key, parent_job: The output key if it is different
        from the original name, otherwise it returns the same name.
        '''
        if job in self.workflows_dictionary:
            for input in self.workflows_dictionary[job]['job_declarations']:
                input_type = \
                self.workflows_dictionary[job]['job_declarations'][input][
                    'type']
                if input_type == 'output':
                    input_name = \
                    self.workflows_dictionary[job]['job_declarations'][input][
                        'name']
                    if input == job_declaration_name:
                        parent_job = input_name.split()[0]
                        dict_output_key = input_name.split()[-1]
                        return dict_output_key, parent_job
        return None, None

    def get_docker_image(self, job_task_reference):
        '''
        Find the corresponding docker image for writing this job's dockerCall in
        the self.tasks_dictionary's runtime.

        :param job_task_reference: Name of the job; used as a key to call the
        task's dictionary.
        :return: The corresponding name of the docker image, e.g. "ubuntu:latest"
        '''
        if self.tasks_dictionary[job_task_reference]['runtime']:
            for tuple in self.tasks_dictionary[job_task_reference]['runtime']:
                if tuple[0] == 'docker':
                    docker_image = tuple[1]
        else:
            raise RuntimeError(
                'Writing docker function, but no runtime section found.')
        return docker_image

    def translate_wdl_string_to_python_string(self, job, some_string):
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
                output_string = output_string + keyword + " + "

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

    def return_one_job_per_priority(self):
        '''
        Definitions only need to be declared once, even if they are run multiple
        times, this function returns a list of jobs with these redundant jobs
        removed for this purpose.

        :return: job_list_with_redundant_jobs_removed
        '''
        job_list_with_redundant_jobs_removed = []
        for i in range(len(self.workflows_dictionary)):
            for job in self.workflows_dictionary:
                if i == job[0]:
                    job_list_with_redundant_jobs_removed.append(job)
                    break
        return (job_list_with_redundant_jobs_removed)

    def determine_if_docker_job(self, job):
        '''
        Returns True if the job has a docker parameter specified in its Task's
        'runtime' section.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :return:
        '''
        docker = False
        job_task_reference = job[2]  # default name
        if 'runtime' in self.tasks_dictionary[job_task_reference]:
            for runtime_var in self.tasks_dictionary[job_task_reference][
                'runtime']:
                if runtime_var[0] == 'docker':
                    docker = True
        return docker

    def mk_ordered_dict_of_all_job_input_params(self):
        '''
        Gets all of the info necessary to write the the toil job wrapping
        declarations with all appropriate variables.

        :return: an ordered dictionary.  Example:

        OrderedDict(
          [('job1', ['mapping', 'files=fastqs', 'reference_file=reference']),
           ('job2', ['process', 'i=fastqs', 'r=reference', 'sai=job1.rv()'])])
        '''
        job_dict = {}

        sort_these_jobs = []
        for job_map in self.workflows_dictionary:
            if isinstance(job_map, (list, tuple)):
                sort_these_jobs.append(job_map)
        sorted_jobs = sorted(sort_these_jobs)

        i = 1
        for job in sorted_jobs:
            job_reference = job[2]
            job_alias = job[3]
            job_name = 'job' + str(i)
            declaration_array = [job_alias]
            for task_declaration in self.tasks_dictionary[job_reference][
                'inputs']:
                task_var_name = task_declaration[0]
                mapped_var = self.map_to_final_var(job, task_var_name)
                declaration_array.append(task_var_name + '=' + mapped_var)
            job_dict[job_name] = declaration_array
            i = i + 1
        ordered_job_dict = collections.OrderedDict(sorted(job_dict.items(),
                                                          key=lambda t: t[0]))
        return ordered_job_dict

    def map_to_final_var(self, job, task_var_name):
        '''
        Typically takes a task variable, and if it is assigned to a new variable
        in the workflow, it it will return the new workflow replacement,
        otherwise it just returns the same variable back.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param task_var_name: The variable name that needs to be mapped.
        :return mapped_var: The input needed for this job for the original
                            declared variable.
        '''
        mapped_var = ' '
        for wf_declaration in self.workflows_dictionary[job][
            'job_declarations']:
            if task_var_name == wf_declaration:
                wf_declaration_type = \
                self.workflows_dictionary[job]['job_declarations'][
                    wf_declaration]['type']
                wf_declaration_name = \
                self.workflows_dictionary[job]['job_declarations'][
                    wf_declaration]['name']
                mapped_var = self.map_to_final_var_type(wf_declaration_name,
                                                        wf_declaration_type)
        if mapped_var == ' ':
            return task_var_name
        else:
            return mapped_var

    def map_to_final_var_type(self, declaration_name, declaration_type):
        '''
        Identifies workflow variable type, and if anything other than another
        variable name, modifies the file based on its type accordingly.

        :param declaration_name: A variable name, like x.
        :param declaration_type: Example types are:

        'identifier':
            wv -->  wv
        'index_value':
            wv --> wv[0][1]
        'string':
            wv --> 'wv'
        'output':
            wv --> job1.rv()
          OR
            wv --> [job1.rv(), job2.rv(), job3.rv()]

        :return declaration_name: Modified by type as appropriate above.
        '''
        if declaration_type == 'identifier':
            return declaration_name
        elif declaration_type == 'index_value':
            potential_scatter_item = declaration_name.split('[')[0]
            for collection in self.workflows_dictionary['scatter_calls']:
                scatter_item = self.workflows_dictionary['scatter_calls'][
                    collection]
                if scatter_item == potential_scatter_item:
                    old_index = declaration_name[len(potential_scatter_item):]
                    return collection + old_index
        elif declaration_type == 'string':
            return "'" + declaration_name + "'"
        elif declaration_type == 'output':
            return_values = []
            job_alias_reference = declaration_name.split()[0]
            for wf in self.workflows_dictionary:
                if isinstance(wf, (list, tuple)):
                    wf_alias_reference = wf[3]
                    wf_job_num = wf[1]
                    if job_alias_reference == wf_alias_reference:
                        return_values.append('job' + str(wf_job_num) + '.rv()')
            if len(return_values) == 1:
                declaration_name = return_values[0]
            if len(return_values) > 1:
                declaration_name = '[' + ', '.join(return_values) + ']'
        else:
            raise NotImplementedError
        return declaration_name

    def determine_if_called_multitimes(self, parent_job):
        '''
        Returns True if the parent_job (alias) is called more than once during
        the run.

        This is helpful and used to determine the following:

        If a job is called once, it returns a single dictionary of outputs to be
        input into the next job:
                JobInputs(FnName, A=A, B=B, C=C, D=Job1.rv())
        Where an example of Job1.rv() is:
                Job1.rv() = {'value1': 1, 'value2': 2}

        If a job is called more than once though, it returns an array of
        dictionaries:
                JobInputs(FnName, A=A, B=B, C=C, D=[Job1.rv(), Job2.rv()])
        Where examples of Job1.rv() & Job2.rv() are:
                Job1.rv() = {'value1': 1, 'value2': 2}
                Job2.rv() = {'value3': 3, 'value4': 4}

        This basically determines if the input file is expected to be an array
        or a dictionary and write the appropriate function calls.

        :param parent_job: e.g. a function def name like "haplotypeCaller"
        :return: bool True if called multiple times; False if not.
        '''
        jobs_that_called_this_task = []
        for task in self.workflows_dictionary:
            if isinstance(task, (list, tuple)):
                if parent_job == task[3]:
                    jobs_that_called_this_task.append(task)

        if len(jobs_that_called_this_task) > 1:
            multiple_calls = True
        else:
            multiple_calls = False

        return multiple_calls

    def get_job_declarations(self, job):
        '''
        Get the default declaration variable list from the WDL "task" skeleton.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :return: declarations_list of variables from the Task 'input' section.
        '''
        job_task_reference = job[2]
        declarations_list = []
        inputs = self.tasks_dictionary[job_task_reference]['inputs']
        for task_input in inputs:
            declarations_list.append((task_input[0], task_input[1]))
        return declarations_list

    def write_python_file(self,
                          module_section,
                          fn_section,
                          main_section,
                          output_file):
        '''
        Just takes three strings and writes them to output_file.

        :param module_section: A string of 'import modules'.
        :param fn_section: A string of python 'def functions()'.
        :param main_section: A string declaring toil options and main's header.
        :param job_section: A string import files into toil and declaring jobs.
        :param output_file: The file to write the compiled toil script to.
        '''
        with open(output_file, 'w') as file:
            file.write(module_section)
            file.write(fn_section)
            file.write(main_section)

    def write_AST(self):
        '''
        Prints an AST to stdout.

        Does not work by default with toil since Toil actively suppresses stdout
        during the run.
        '''
        with open('AST.out', 'w') as f:
            with open(self.wdl_file, 'r') as wdl:
                wdl_string = wdl.read()
                ast = wdl_parser.parse(wdl_string).ast()
                f.write(ast.dumps(indent=2))

    def write_mappings(self, i):
        '''
        Intended to take a ToilWDL_instance (i) and prints the final task dict,
        workflow dict, csv dict, and tsv dict.

        Does not work by default with toil since Toil actively suppresses stdout
        during the run.

        :param i: A class object instance with the following dict variables:
                                self.tasks_dictionary
                                self.workflows_dictionary
                                self.tsv_dict
                                self.csv_dict
        '''
        with open('mappings.out', 'w') as f:
            f.write('\n\ntask_dict')
            f.write(str(i.tasks_dictionary))
            for each_task in i.tasks_dictionary:
                f.write(str(each_task))
                if i.tasks_dictionary[each_task]:
                    for each_section in i.tasks_dictionary[each_task]:
                        f.write('    ' + str(each_section))
                        if i.tasks_dictionary[each_task][each_section]:
                            for each_variable in i.tasks_dictionary[each_task][
                                each_section]:
                                f.write('        ' + str(each_variable))

                        f.write('\n\nworkflows_dict')
            f.write(str(i.workflows_dictionary))
            for each_task in i.workflows_dictionary:
                f.write(str(each_task))
                if 'wf_declarations' in i.workflows_dictionary[each_task]:
                    f.write('    wf_declarations')
                    for d in i.workflows_dictionary[each_task][
                        'wf_declarations']:
                        f.write('        ' + str(d))
                if 'job_declarations' in i.workflows_dictionary[each_task]:
                    f.write('    job_declarations')
                    for j in i.workflows_dictionary[each_task][
                        'job_declarations']:
                        f.write('        ' + str(j))
                        for g in \
                        i.workflows_dictionary[each_task]['job_declarations'][
                            j]:
                            f.write('            ' + g + ': ' +
                                    i.workflows_dictionary[each_task][
                                        'job_declarations'][j][g])

            f.write('\n\ntsv_dict')
            for var in i.tsv_dict:
                f.write(str(var))
                f.write(str(i.tsv_dict))

            f.write('\n\ncsv_dict')
            for var in i.csv_dict:
                f.write(str(var))
                f.write(str(i.csv_dict))


def recursive_glob(job, directoryname, glob_pattern):
    '''
    Walks through a directory and its subdirectories looking for files matching
    the glob_pattern and returns a list=[].

    :param job: A "job" object representing the current task node "job" being
                passed around by toil.  Toil's minimum unit of work.
    :param directoryname: Any accessible folder name on the filesystem.
    :param glob_pattern: A string like "*.txt", which would find all text files.
    :return: A list=[] of absolute filepaths matching the glob pattern.
    '''
    matches = []
    for root, dirnames, filenames in os.walk(directoryname):
        for filename in fnmatch.filter(filenames, glob_pattern):
            absolute_filepath = os.path.join(root, filename)
            matches.append(absolute_filepath)
    return matches


def heredoc_wdl(template, dictionary={}, indent=''):
    template = textwrap.dedent(template).format(**dictionary)
    return template.replace('\n', '\n' + indent) + '\n'

def generate_docker_bashscript_file(temp_dir, docker_dir, globs, cmd, job_name):
    '''
    Creates a bashscript to inject into a docker container for the job.

    This script wraps the job command(s) given in a bash script, hard links the
    outputs and returns an "rc" file containing the exit code.  All of this is
    done in an effort to parallel the Broad's cromwell engine, which is the
    native WDL runner.  As they've chosen to write and then run a bashscript for
    every command, so shall we.

    :param temp_dir: The current directory outside of docker to deposit the
                     bashscript into, which will be the bind mount that docker
                     loads files from into its own containerized filesystem.
                     This is usually the tempDir created by this individual job
                     using 'tempDir = job.fileStore.getLocalTempDir()'.
    :param docker_dir: The working directory inside of the docker container
                       which is bind mounted to 'temp_dir'.  By default this is
                       'data'.
    :param globs: A list of expected output files to retrieve as glob patterns
                  that will be returned as hard links to the current working
                  directory.
    :param cmd: A bash command to be written into the bash script and run.
    :param job_name: The job's name, only used to write in a file name
                     identifying the script as written for that job.
                     Will be used to call the script later.
    :return: Nothing, but it writes and deposits a bash script in temp_dir
             intended to be run inside of a docker container for this job.
    '''
    wdl_copyright = heredoc_wdl('''#!/bin/bash
        
        # Borrowed/rewritten from the Broad's Cromwell implementation.  As 
        # that is under a BSD-ish license, I include here the license off 
        # of their GitHub repo.  Thank you Broadies!
        
        # Copyright (c) 2015, Broad Institute, Inc.
        # All rights reserved.
        
        # Redistribution and use in source and binary forms, with or without
        # modification, are permitted provided that the following conditions are met:
        
        # * Redistributions of source code must retain the above copyright notice, this
        #   list of conditions and the following disclaimer.
        
        # * Redistributions in binary form must reproduce the above copyright notice,
        #   this list of conditions and the following disclaimer in the documentation
        #   and/or other materials provided with the distribution.
        
        # * Neither the name Broad Institute, Inc. nor the names of its
        #   contributors may be used to endorse or promote products derived from
        #   this software without specific prior written permission.
        
        # THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
        # AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
        # IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
        # DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
        # FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
        # DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
        # SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
        # CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
        # OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
        # OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE
        
        # make a temp directory w/identifier
        ''')
    prefix_dict = {"docker_dir": docker_dir,
                   "cmd": cmd}
    bashfile_prefix = heredoc_wdl('''
        tmpDir=$(mktemp -d /{docker_dir}/execution/tmp.XXXXXX)
        chmod 777 $tmpDir
        # set destination for java to deposit all of its files
        export _JAVA_OPTIONS=-Djava.io.tmpdir=$tmpDir
        export TMPDIR=$tmpDir
        
        (
        cd /{docker_dir}/execution
        {cmd}
        )
        
        # gather the input command return code
        echo $? > "$tmpDir/rc.tmp"
        
        ''', prefix_dict)

    bashfile_string = wdl_copyright + bashfile_prefix

    begin_globbing_string = heredoc_wdl('''
        (
        cd $tmpDir
        mkdir "$tmpDir/globs"
        ''')

    bashfile_string = bashfile_string + begin_globbing_string

    for glob_input in globs:
        add_this_glob = \
            '( ln -L ' + glob_input + \
            ' "$tmpDir/globs" 2> /dev/null ) || ( ln ' + glob_input + \
            ' "$tmpDir/globs" )\n'
        bashfile_string = bashfile_string + add_this_glob

    bashfile_suffix = heredoc_wdl('''
        )
        
        # flush RAM to disk
        sync
        
        mv "$tmpDir/rc.tmp" "$tmpDir/rc"
        ''')

    bashfile_string = bashfile_string + bashfile_suffix

    with open(os.path.join(temp_dir, job_name + '_script.sh'), 'w') as bashfile:
        bashfile.write(bashfile_string)

def main():
    parser = argparse.ArgumentParser(description='Runs WDL files with toil.')
    parser.add_argument('wdl_file', help='A WDL workflow file.')
    parser.add_argument('secondary_file', help='A secondary data file (json).')
    parser.add_argument('-o',
                        '--output_directory',
                        required=False,
                        default=os.getcwd(),
                        help='Optionally specify the directory that outputs '
                             'are written to.  Default is the current working dir.')
    parser.add_argument('--gen_parse_files', required=False, default=False,
                        help='Creates "AST.out", which holds the printed AST and'
                             '"mappings.out", which holds the printed task, workflow,'
                             'csv, and tsv dictionaries generated by the parser.')
    parser.add_argument('--dont_delete_compiled', required=False, default=False,
                        help='Saves the compiled toil script generated from the'
                             'wdl/json files from deletion.')

    # wdl_run_args is an array containing all of the unknown arguments not
    # specified by the parser in this main.  All of these will be passed down in
    # check_call later to run the compiled toil file.
    args, wdl_run_args = parser.parse_known_args()

    wdl_file_path = os.path.abspath(args.wdl_file)
    args.secondary_file = os.path.abspath(args.secondary_file)
    args.output_directory = os.path.abspath(args.output_directory)

    w = ToilWDL(wdl_file_path, args.secondary_file, args.output_directory)

    # read secondary file; create dictionary to hold variables
    if args.secondary_file.endswith('.json'):
        w.dict_from_JSON(args.secondary_file)
    elif args.secondary_file.endswith('.yml'):
        w.dict_from_YML(args.secondary_file)
    elif args.secondary_file.endswith('.yaml'):
        w.dict_from_YML(args.secondary_file)
    else:
        raise RuntimeError('Unsupported Secondary File Type.  Use json.')

    # parse the wdl AST into 2 dictionaries
    with open(wdl_file_path, 'r') as wdl:
        wdl_string = wdl.read()
        ast = wdl_parser.parse(wdl_string).ast()
        w.create_tasks_dict(ast)
        w.create_workflows_dict(ast)

    # use the AST dictionaries to write 4 strings
    # these are the future 4 sections of the compiled toil python file
    module_section = w.write_modules()
    fn_section = w.write_functions()
    main_section = w.write_main()

    # write 3 strings to a python output file
    w.write_python_file(module_section,
                        fn_section,
                        main_section,
                        w.output_file)

    wdllogger.debug('WDL file compiled to toil script.  Running now.')

    if args.gen_parse_files:
        w.write_mappings(w)
        w.write_AST()

    cmd = ['python', w.output_file]
    cmd.extend(wdl_run_args)
    subprocess.check_call(cmd)

    if not args.dont_delete_compiled:
        os.remove(w.output_file)

if __name__ == '__main__':
    main()