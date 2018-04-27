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
import os
import logging

import toil.wdl.wdl_parser as wdl_parser
from toil.wdl.wdl_functions import heredoc_wdl

wdllogger = logging.getLogger(__name__)


class SynthesizeWDL:
    '''
    SynthesizeWDL takes the "workflows_dictionary" and "tasks_dictionary" produced by
    wdl_analysis.py and uses them to write a native python script for use with Toil.

    A WDL "workflow" section roughly corresponds to the python "main()" function, where
    functions are wrapped as Toil "jobs", output dependencies specified, and called.

    A WDL "task" section corresponds to a unique python function, which will be wrapped
    as a Toil "job" and defined outside of the "main()" function that calls it.

    Generally this handles breaking sections into their corresponding Toil counterparts.

    For example: write the imports, then write all functions defining jobs (which have subsections
    like: write header, define variables, read "File" types into the jobstore, docker call, etc.),
    then write the main and all of its subsections.
    '''

    def __init__(self, tasks_dictionary, workflows_dictionary, output_directory, json_dict, tsv_dict, csv_dict):
        self.output_directory = output_directory
        self.output_file = os.path.join(self.output_directory, 'toilwdl_compiled.py')

        # only json is required; tsv/csv are optional
        self.json_dict = json_dict
        self.tsv_dict = tsv_dict
        self.csv_dict = csv_dict

        # holds task skeletons from WDL task objects
        self.tasks_dictionary = tasks_dictionary
        # holds workflow structure from WDL workflow objects
        self.workflows_dictionary = workflows_dictionary

        # unique iterator to add to cmd names
        self.cmd_num = 0
        # unique number for a job
        self.task_number = 0
        # a job's 'level' on the DAG
        self.task_priority = 0

    def write_modules(self):
        # string used to write imports to the file
        module_string = heredoc_wdl('''
                    from toil.job import Job
                    from toil.common import Toil
                    from toil.lib.docker import apiDockerCall
                    from toil.wdl.wdl_functions import generate_docker_bashscript_file
                    from toil.wdl.wdl_functions import select_first
                    from toil.wdl.wdl_functions import size
                    from toil.wdl.wdl_functions import glob
                    from toil.wdl.wdl_functions import process_and_read_file
                    from toil.wdl.wdl_functions import process_infile
                    from toil.wdl.wdl_functions import process_outfile
                    from toil.wdl.wdl_functions import abspath_file
                    from toil.wdl.wdl_functions import combine_dicts
                    from toil.wdl.wdl_functions import parse_memory
                    from toil.wdl.wdl_functions import parse_cores
                    from toil.wdl.wdl_functions import parse_disk
                    from toil.wdl.wdl_functions import read_string
                    from toil.wdl.wdl_functions import read_int
                    from toil.wdl.wdl_functions import read_float
                    from toil.wdl.wdl_functions import read_tsv
                    from toil.wdl.wdl_functions import read_csv
                    import fnmatch
                    import subprocess
                    import os
                    import errno
                    import time
                    import shutil
                    import shlex
                    import uuid
                    import logging
                    from re import sub # re.sub('[ES]', 'a', s)
                    asldijoiu23r8u34q89fho934t8u34fcurrentworkingdir = os.getcwd()

                    logger = logging.getLogger(__name__)


                        ''')[1:]
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

        # write out the main header
        main_header = self.write_main_header()
        main_section = main_section + main_header

        # write out the workflow declarations
        main_section = main_section + '        # WF Declarations\n'
        wf_declarations_to_write = self.write_main_wfdeclarations()
        main_section = main_section + wf_declarations_to_write

        # write toil job wrappers with input vars
        jobs_to_write = self.write_main_jobwrappers()
        main_section = main_section + jobs_to_write

        return main_section

    def write_main_header(self):
        main_header = heredoc_wdl('''
            if __name__=="__main__":
                options = Job.Runner.getDefaultOptions("./toilWorkflowRun")
                options.clean = 'always'
                with Toil(options) as toil:
            ''')
        return main_header

    def write_main_wfdeclarations(self):
        main_section = ''
        for wfname, wf in self.workflows_dictionary.iteritems():
            if 'wf_declarations' in wf:
                for var, var_expressn in wf['wf_declarations'].iteritems():

                    # check the json file for the expression's value
                    # this is a higher priority and overrides anything written in the .wdl
                    json_expressn = self.json_var(wf=wfname, var=var)
                    if json_expressn:
                        var_expressn['value'] = json_expressn

                    # empty string
                    if not var_expressn['value'] and (var_expressn['type'] in ['String', 'File']):
                        main_section += '        {} = ""\n'.format(var)
                    # None
                    elif not var_expressn['value'] and not (var_expressn['type'] in ['String', 'File']):
                        main_section += '        {} = None\n'.format(var)
                    # import filepath into jobstore
                    elif var_expressn['value'] and (var_expressn['type'] == 'File'):
                        main_section += '        {} = process_infile({}, toil)\n'.format(var, var_expressn['value'])
                    # normal declaration
                    else:
                        main_section += '        {} = {}\n'.format(var, var_expressn['value'])
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
        for wf in self.workflows_dictionary:
            for assignment in self.workflows_dictionary[wf]:
                if assignment.startswith('call'):
                    main_section += self.write_main_jobwrappers_call(self.workflows_dictionary[wf][assignment])
        main_section += '\n        toil.start(job0)\n'

        return main_section

    def write_main_jobwrappers_call(self, task):

        main_section = '        {} = job0.addChild({}Cls('.format(task['alias'], task['task'])
        for var in task['io']:
            main_section += var + '=' + task['io'][var] + ', '
        if main_section.endswith(', '):
            main_section = main_section[:-2]
        return main_section + ')).encapsulate()\n'

    def write_functions(self):
        '''
        Writes out a python function for each WDL "task" object.

        :return: a giant string containing the meat of the job defs.
        '''

        # toil cannot technically start with multiple jobs, so an empty
        # 'initialize_jobs' function is always called first to get around this
        fn_section = 'def initialize_jobs(job):\n' + \
                     '    job.fileStore.logToMaster("initialize_jobs")\n'

        for job in self.tasks_dictionary:
            fn_section += self.write_function(job)

        return fn_section

    def write_function(self, job):
        '''
        Writes out a python function for each WDL "task" object.

        Each python function is a unit of work written out as a string in
        preparation to being written out to a file.  In WDL, each "job" is
        called a "task".  Each WDL task is written out in multiple steps:

        1: Header and inputs (e.g. 'def mapping(self, input1, input2)')
        2: Log job name (e.g. 'job.fileStore.logToMaster('initialize_jobs')')
        3: Create temp dir (e.g. 'tempDir = fileStore.getLocalTempDir()')
        4: import filenames and use readGlobalFile() to get files from the
           jobStore
        5: Reformat commandline variables (like converting to ' '.join(files)).
        6: Commandline call using subprocess.Popen().
        7: Write the section returning the outputs.  Also logs stats.

        :return: a giant string containing the meat of the job defs for the toil script.
        '''

        # write the function header
        fn_section = self.write_function_header(job)

        # write out commandline keywords
        fn_section += self.write_function_cmdline(job)

        if self.needsdocker(job):
            # write a bash script to inject into the docker
            fn_section += self.write_function_bashscriptline(job)
            # write a call to the docker API
            fn_section += self.write_function_dockercall(job)
        else:
            # write a subprocess call
            fn_section += self.write_function_subprocesspopen()

        # write the outputs for the definition to return
        fn_section += self.write_function_outputreturn(job, docker=self.needsdocker(job))

        return fn_section

    def write_function_header(self, job):
        '''
        Writes the header that starts each function, for example, this function
        can write and return:

        'def write_function_header(self, job, job_declaration_array):'

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param job_declaration_array: A list of all inputs that job requires.
        :return: A string representing this.
        '''
        fn_section = '\n\nclass {jobname}Cls(Job):\n'.format(jobname=job)
        fn_section += '    def __init__(self, '
        if 'inputs' in self.tasks_dictionary[job]:
            for i in self.tasks_dictionary[job]['inputs']:
                var = i[0]
                vartype = i[1]
                if vartype == 'String':
                    fn_section += '{input}="", '.format(input=var)
                else:
                    fn_section += '{input}=None, '.format(input=var)
        fn_section += '*args, **kwargs):\n'

        if 'runtime' in self.tasks_dictionary[job]:
            runtime_resources = []
            if 'memory' in self.tasks_dictionary[job]['runtime']:
                runtime_resources.append('memory=memory')
                memory = self.tasks_dictionary[job]['runtime']['memory']
                fn_section += '        memory=parse_memory({})\n'.format(memory)
            if 'cpu' in self.tasks_dictionary[job]['runtime']:
                runtime_resources.append('cores=cores')
                cores = self.tasks_dictionary[job]['runtime']['cpu']
                fn_section += '        cores=parse_cores({})\n'.format(cores)
            if 'disks' in self.tasks_dictionary[job]['runtime']:
                runtime_resources.append('disk=disk')
                disk = self.tasks_dictionary[job]['runtime']['disks']
                fn_section += '        disk=parse_disk({})\n'.format(disk)
            runtime_resources = ['self'] + runtime_resources
            fn_section += '        Job.__init__({})\n\n'.format(', '.join(runtime_resources))

        if 'inputs' in self.tasks_dictionary[job]:
            for i in self.tasks_dictionary[job]['inputs']:
                var = i[0]
                var_expressn = i[2]
                json_expressn = self.json_var(task=job, var=var)

                # json declarations have priority and can overwrite
                # whatever is in the wdl file
                if json_expressn:
                    var_expressn = json_expressn
                if not var_expressn:
                    var_expressn = var

                fn_section += '        self.{} = {}\n'.format(var, var_expressn)

        fn_section += heredoc_wdl('''
                                 super({jobname}Cls, self).__init__(*args, **kwargs)

                             def run(self, fileStore):
                                 fileStore.logToMaster("{jobname}")
                                 tempDir = fileStore.getLocalTempDir()
                                 
                                 try:
                                     os.makedirs(os.path.join(tempDir, 'execution'))
                                 except OSError as e:
                                     if e.errno != errno.EEXIST:
                                         raise

                                 ''', {'jobname': job}, indent='    ')[1:]
        if 'inputs' in self.tasks_dictionary[job]:
            for i in self.tasks_dictionary[job]['inputs']:
                var = i[0]
                var_type = i[1]
                docker_bool = str(self.needsdocker(job))
                if var_type == 'File':
                    fn_section += '        {} = process_and_read_file(abspath_file(self.{}, asldijoiu23r8u34q89fho934t8u34fcurrentworkingdir), tempDir, fileStore, docker={})\n'.format(var, var, docker_bool)
                else:
                    fn_section += '        {} = self.{}\n'.format(var, var)

        return fn_section

    def json_var(self, var, task=None, wf=None):
        """

        :param var:
        :param task:
        :param wf:
        :return:
        """
        # default to the last workflow in the list
        if wf is None:
            for workflow in self.workflows_dictionary:
                wf = workflow

        for identifier in self.json_dict:
            # check task declarations
            if task:
                if identifier == '{}.{}.{}'.format(wf, task, var):
                    return self.json_dict[identifier]
            # else check workflow declarations
            else:
                if identifier == '{}.{}'.format(wf, var):
                    return self.json_dict[identifier]

        return None

    def write_function_bashscriptline(self, job):
        '''
        Writes a function to create a bashscript for injection into the docker
        container.

        :param job_task_reference: The job referenced in WDL's Task section.
        :param job_alias: The actual job name to be written.
        :return: A string writing all of this.
        '''
        fn_section = "        generate_docker_bashscript_file(temp_dir=tempDir, docker_dir='/root', globs=["
        # TODO: Add glob functionality.
        # if 'outputs' in self.tasks_dictionary[job]:
        #     for output in self.tasks_dictionary[job]['outputs']:
        #         fn_section += '({}), '.format(output[2])
        if fn_section.endswith(', '):
            fn_section = fn_section[:-2]
        fn_section += "], cmd=cmd, job_name='{}')\n\n".format(str(job))

        return fn_section

    def write_function_dockercall(self, job):
        '''
        Writes a string containing the apiDockerCall() that will run the job.

        :param job_task_reference: The name of the job calling docker.
        :param docker_image: The corresponding name of the docker image.
                                                            e.g. "ubuntu:latest"
        :return: A string containing the apiDockerCall() that will run the job.
        '''
        docker_dict = {"docker_image": self.tasks_dictionary[job]['runtime']['docker'],
                       "job_task_reference": job}
        docker_template = heredoc_wdl('''
        stdout = apiDockerCall(self, 
                               image={docker_image}, 
                               working_dir=tempDir, 
                               parameters=["/root/{job_task_reference}_script.sh"], 
                               entrypoint="/bin/bash", 
                               volumes={{tempDir: {{"bind": "/root"}}}})
            ''', docker_dict, indent='        ')[1:]

        return docker_template

    def write_function_cmdline(self, job):
        '''
        Write a series of commandline variables to be concatenated together
        eventually and either called with subprocess.Popen() or with
        apiDockerCall() if a docker image is called for.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :return: A string representing this.
        '''

        fn_section = '\n'
        cmd_array = []
        if 'raw_commandline' in self.tasks_dictionary[job]:
            for cmd in self.tasks_dictionary[job]['raw_commandline']:
                fn_section = fn_section + '        command{} = {}\n'.format(str(self.cmd_num), cmd)
                cmd_array.append('command' + str(self.cmd_num))
                self.cmd_num = self.cmd_num + 1

        if cmd_array:
            fn_section += '\n        cmd = '
            for command in cmd_array:
                fn_section += command + ' + '
            if fn_section.endswith(' + '):
                fn_section = fn_section[:-3]
            fn_section = fn_section + '\n'

        return fn_section

    def write_function_subprocesspopen(self):
        '''
        Write a subprocess.Popen() call for this function and write it out as a
        string.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :return: A string representing this.
        '''
        fn_section = heredoc_wdl('''
                this_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
                stdout = this_process.communicate()\n''', indent='        ')

        return fn_section

    def write_function_outputreturn(self, job, docker=False):
        '''
        Find the output values that this function needs and write them out as a
        string.

        :param job: A list such that:
                        (job priority #, job ID #, Job Skeleton Name, Job Alias)
        :param job_task_reference: The name of the job to look up values for.
        :return: A string representing this.
        '''

        fn_section = ''
        if 'outputs' in self.tasks_dictionary[job]:
            return_values = []
            for output in self.tasks_dictionary[job]['outputs']:
                output_name = output[0]
                output_type = output[1]
                output_value = output[2]

                if output_type == 'File':
                    nonglob_dict = {
                        "output_name": output_name,
                        "expression": output_value,
                        "out_dir": self.output_directory,
                        "output_type": output_type}

                    nonglob_template = heredoc_wdl('''
                        # output-type: {output_type}
                        output_filename = {expression}
                        {output_name} = process_outfile(output_filename, fileStore, tempDir, '{out_dir}')
                    ''', nonglob_dict, indent='        ')[1:]
                    fn_section += nonglob_template
                    return_values.append(output_name)
                else:
                    fn_section += '        {} = {}\n'.format(output_name, output_value)
                    return_values.append(output_name)

            if return_values:
                fn_section += '        rvDict = {'
            for return_value in return_values:
                fn_section += '"{rv}": {rv}, '.format(rv=return_value)
            if fn_section.endswith(', '):
                fn_section = fn_section[:-2]
            if return_values:
                fn_section = fn_section + '}\n'

            if return_values:
                fn_section += '        print(rvDict)\n'
                fn_section += '        return rvDict\n\n'

        return fn_section

    def needsdocker(self, job):
        """

        :param job:
        :return:
        """
        if 'runtime' in self.tasks_dictionary[job]:
            if 'docker' in self.tasks_dictionary[job]['runtime']:
                return True

        return False

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
        with open('/home/quokka/Desktop/test/toil/src/toil/test/wdl/astTemplates/compiled.py', 'w') as file:
            file.write(module_section)
            file.write(fn_section)
            file.write(main_section)

    def write_mappings(self, i):
        '''
        Intended to take a ToilWDL_instance (i) and prints the final task dict,
        and workflow dict.

        Does not work by default with toil since Toil actively suppresses stdout
        during the run.

        :param i: A class object instance with the following dict variables:
                                self.tasks_dictionary
                                self.workflows_dictionary
        '''
        from collections import OrderedDict

        class Formatter(object):
            def __init__(self):
                self.types = {}
                self.htchar = '\t'
                self.lfchar = '\n'
                self.indent = 0
                self.set_formater(object, self.__class__.format_object)
                self.set_formater(dict, self.__class__.format_dict)
                self.set_formater(list, self.__class__.format_list)
                self.set_formater(tuple, self.__class__.format_tuple)

            def set_formater(self, obj, callback):
                self.types[obj] = callback

            def __call__(self, value, **args):
                for key in args:
                    setattr(self, key, args[key])
                formater = self.types[type(value) if type(value) in self.types else object]
                return formater(self, value, self.indent)

            def format_object(self, value, indent):
                return repr(value)

            def format_dict(self, value, indent):
                items = [
                    self.lfchar + self.htchar * (indent + 1) + repr(key) + ': ' +
                    (self.types[type(value[key]) if type(value[key]) in self.types else object])(self, value[key], indent + 1)
                    for key in value]
                return '{%s}' % (','.join(items) + self.lfchar + self.htchar * indent)

            def format_list(self, value, indent):
                items = [
                    self.lfchar + self.htchar * (indent + 1) + (
                    self.types[type(item) if type(item) in self.types else object])(self, item, indent + 1)
                    for item in value]
                return '[%s]' % (','.join(items) + self.lfchar + self.htchar * indent)

            def format_tuple(self, value, indent):
                items = [
                    self.lfchar + self.htchar * (indent + 1) + (
                    self.types[type(item) if type(item) in self.types else object])(self, item, indent + 1)
                    for item in value]
                return '(%s)' % (','.join(items) + self.lfchar + self.htchar * indent)

        pretty = Formatter()

        def format_ordereddict(self, value, indent):
            items = [
                self.lfchar + self.htchar * (indent + 1) +
                "(" + repr(key) + ', ' + (self.types[
                    type(value[key]) if type(value[key]) in self.types else object
                ])(self, value[key], indent + 1) + ")"
                for key in value
            ]
            return 'OrderedDict([%s])' % (','.join(items) +
                                          self.lfchar + self.htchar * indent)

        pretty.set_formater(OrderedDict, format_ordereddict)

        with open('mappings.out', 'w') as f:
            f.write(pretty(i.tasks_dictionary))
            f.write('\n\n\n\n\n\n')
            f.write(pretty(i.workflows_dictionary))

def write_AST(wdl_file, outdir=None):
    '''
    Writes a file with the AST for a wdl file in the outdir.
    '''
    if outdir is None:
        outdir = os.getcwd()
    with open(os.path.join(outdir, 'AST.out'), 'w') as f:
        with open(wdl_file, 'r') as wdl:
            wdl_string = wdl.read()
            ast = wdl_parser.parse(wdl_string).ast()
            f.write(ast.dumps(indent=2))
