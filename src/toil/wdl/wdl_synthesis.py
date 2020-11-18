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
from six import iteritems

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

    def __init__(self,
                 tasks_dictionary,
                 workflows_dictionary,
                 output_directory,
                 json_dict,
                 docker_user,
                 jobstore=None,
                 destBucket=None):
        self.output_directory = output_directory
        self.output_file = os.path.join(self.output_directory, 'toilwdl_compiled.py')

        self.jobstore = jobstore if jobstore else './toilWorkflowRun'

        if docker_user != 'None':
            self.docker_user = "'" + docker_user + "'"
        else:
            self.docker_user = docker_user

        # only json is required; tsv/csv are optional
        self.json_dict = json_dict

        # holds task skeletons from WDL task objects
        self.tasks_dictionary = tasks_dictionary
        # holds workflow structure from WDL workflow objects
        self.workflows_dictionary = workflows_dictionary

        # unique iterator to add to cmd names
        self.cmd_num = 0

        # deposit WDL outputs into a cloud bucket; optional
        self.destBucket = destBucket

    def write_modules(self):
        # string used to write imports to the file
        module_string = heredoc_wdl('''
                    from toil.job import Job
                    from toil.common import Toil
                    from toil.lib.docker import apiDockerCall
                    from toil.wdl.wdl_types import WDLType
                    from toil.wdl.wdl_types import WDLPairType
                    from toil.wdl.wdl_types import WDLMapType
                    from toil.wdl.wdl_functions import generate_docker_bashscript_file
                    from toil.wdl.wdl_functions import parse_value_from_type
                    from toil.wdl.wdl_functions import generate_stdout_file
                    from toil.wdl.wdl_functions import select_first
                    from toil.wdl.wdl_functions import sub
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
                    from toil.wdl.wdl_functions import read_lines
                    from toil.wdl.wdl_functions import read_tsv
                    from toil.wdl.wdl_functions import read_csv
                    from toil.wdl.wdl_functions import read_json
                    from toil.wdl.wdl_functions import read_map
                    from toil.wdl.wdl_functions import read_int
                    from toil.wdl.wdl_functions import read_string
                    from toil.wdl.wdl_functions import read_float
                    from toil.wdl.wdl_functions import read_boolean
                    from toil.wdl.wdl_functions import write_lines
                    from toil.wdl.wdl_functions import write_tsv
                    from toil.wdl.wdl_functions import write_json
                    from toil.wdl.wdl_functions import write_map
                    from toil.wdl.wdl_functions import defined
                    from toil.wdl.wdl_functions import basename
                    from toil.wdl.wdl_functions import floor
                    from toil.wdl.wdl_functions import ceil
                    from toil.wdl.wdl_functions import wdl_range
                    from toil.wdl.wdl_functions import transpose
                    from toil.wdl.wdl_functions import length
                    import fnmatch
                    import textwrap
                    import subprocess
                    import os
                    import errno
                    import time
                    import shutil
                    import shlex
                    import uuid
                    import logging
                    
                    _toil_wdl_internal__current_working_dir = os.getcwd()

                    logger = logging.getLogger(__name__)


                        ''', {'jobstore': self.jobstore})[1:]
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

        # loop to export all outputs to a cloud bucket
        if self.destBucket:
            main_destbucket = self.write_main_destbucket()
            main_section = main_section + main_destbucket

        return main_section

    def write_main_header(self):
        main_header = heredoc_wdl('''
            if __name__=="__main__":
                options = Job.Runner.getDefaultOptions("{jobstore}")
                options.clean = 'always'
                with Toil(options) as fileStore:
            ''', {'jobstore': self.jobstore})
        return main_header

    def write_main_wfdeclarations(self):
        main_section = ''
        for wfname, wf in iteritems(self.workflows_dictionary):
            if 'wf_declarations' in wf:
                for var, var_expressn in iteritems(wf['wf_declarations']):
                    var_type = var_expressn['type']

                    # check the json file for the expression's value
                    # this is a higher priority and overrides anything written in the .wdl
                    json_expressn = self.json_var(wf=wfname, var=var)
                    if json_expressn is not None:
                        var_expressn['value'] = json_expressn

                    # empty string
                    if not var_expressn['value'] and (var_type in ('String', 'File')):
                        main_section += '        {} = ""\n'.format(var)
                    # None
                    elif var_expressn['value'] is None and not (var_type in ('String', 'File')):
                        main_section += '        {} = None\n'.format(var)
                    # file or compound types
                    elif var_type in ('File', 'Pair', 'Map'):
                        parsed_value = 'parse_value_from_type({}, var_type={}, file_store=fileStore)'.format(
                            var_expressn['value'], repr(var_type))
                        main_section += '        {} = {}\n'.format(var, parsed_value)
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
                    main_section += '        job0 = job0.encapsulate()\n'
                    main_section += self.write_main_jobwrappers_call(self.workflows_dictionary[wf][assignment])
                if assignment.startswith('scatter'):
                    main_section += '        job0 = job0.encapsulate()\n'
                    main_section += self.write_main_jobwrappers_scatter(self.workflows_dictionary[wf][assignment],
                                                                        assignment)
                if assignment.startswith('if'):
                    main_section += '        if {}:\n'.format(self.workflows_dictionary[wf][assignment]['expression'])
                    main_section += self.write_main_jobwrappers_if(self.workflows_dictionary[wf][assignment]['body'])

        main_section += '\n        fileStore.start(job0)\n'

        return main_section

    def write_main_destbucket(self):
        '''
        Writes out a loop for exporting outputs to a cloud bucket.

        :return: A string representing this.
        '''
        main_section = heredoc_wdl('''
            outdir = '{outdir}'
            onlyfiles = [os.path.join(outdir, f) for f in os.listdir(outdir) if os.path.isfile(os.path.join(outdir, f))]
            for output_f_path in onlyfiles:
                output_file = fileStore.writeGlobalFile(output_f_path)
                preserveThisFilename = os.path.basename(output_f_path)
                destUrl = '/'.join(s.strip('/') for s in [destBucket, preserveThisFilename])
                fileStore.exportFile(output_file, destUrl)
            ''', {'outdir': self.output_directory}, indent='    ')
        return main_section

    def fetch_ignoredifs(self, assignments, breaking_assignment):
        ignore_ifs = []
        for assignment in assignments:
            if assignment.startswith('call'):
                pass
            elif assignment.startswith('scatter'):
                pass
            elif assignment.startswith('if'):
                if not self.fetch_ignoredifs_chain(assignments[assignment]['body'], breaking_assignment):
                    ignore_ifs.append(assignment)
        return ignore_ifs

    def fetch_ignoredifs_chain(self, assignments, breaking_assignment):
        for assignment in assignments:
            if assignment.startswith('call'):
                if assignment == breaking_assignment:
                    return True
            if assignment.startswith('scatter'):
                if assignment == breaking_assignment:
                    return True
            if assignment.startswith('if'):
                return self.fetch_ignoredifs_chain(assignments[assignment]['body'], breaking_assignment)
        return False

    def write_main_jobwrappers_if(self, if_statement):
        main_section = ''
        for assignment in if_statement:
            if assignment.startswith('call'):
                main_section += '        job0 = job0.encapsulate()\n'
                main_section += self.write_main_jobwrappers_call(if_statement[assignment])
            if assignment.startswith('scatter'):
                main_section += '        job0 = job0.encapsulate()\n'
                main_section += self.write_main_jobwrappers_scatter(if_statement[assignment], assignment)
            if assignment.startswith('if'):
                main_section += '        if {}:\n'.format(if_statement[assignment]['expression'])
                main_section += self.write_main_jobwrappers_if(if_statement[assignment]['body'])
        main_section = self.indent_docstring(main_section)
        return main_section

    def write_main_jobwrappers_scatter(self, task, assignment):
        scatter_inputs = self.fetch_scatter_inputs(assignment)

        main_section = '        {scatter} = job0.addChild({scatter}Cls('.format(scatter=assignment)
        for var in scatter_inputs:
            main_section += var + '=' + var + ', '
        if main_section.endswith(', '):
            main_section = main_section[:-2]
        main_section += '))\n'

        scatter_outputs = self.fetch_scatter_outputs(task)
        for var in scatter_outputs:
            main_section += '        {var} = {scatter}.rv("{var}")\n'.format(var=var['task'] + '_' + var['output'], scatter=assignment)

        return main_section

    def fetch_scatter_outputs(self, task):
        scatteroutputs = []

        for var in task['body']:
            # TODO variable support
            if var.startswith('call'):
                if 'outputs' in self.tasks_dictionary[task['body'][var]['task']]:
                    for output in self.tasks_dictionary[task['body'][var]['task']]['outputs']:
                        scatteroutputs.append({'task': task['body'][var]['alias'], 'output': output[0]})
        return scatteroutputs

    def fetch_scatter_inputs(self, assigned):

        for wf in self.workflows_dictionary:
            ignored_ifs = self.fetch_ignoredifs(self.workflows_dictionary[wf], assigned)
            # TODO support additional wfs
            break

        scatternamespace = []

        for wfname, wf in iteritems(self.workflows_dictionary):
            if 'wf_declarations' in wf:
                for var in wf['wf_declarations']:
                    scatternamespace.append(var)

        for wf in self.workflows_dictionary:
            for assignment in self.workflows_dictionary[wf]:
                if assignment == assigned:
                    return scatternamespace
                elif assignment.startswith('call'):
                    if 'outputs' in self.tasks_dictionary[self.workflows_dictionary[wf][assignment]['task']]:
                        for output in self.tasks_dictionary[self.workflows_dictionary[wf][assignment]['task']]['outputs']:
                            scatternamespace.append(self.workflows_dictionary[wf][assignment]['alias'] + '_' + output[0])
                elif assignment.startswith('scatter'):
                    for var in self.fetch_scatter_outputs(self.workflows_dictionary[wf][assignment]):
                        scatternamespace.append(var['task'] + '_' + var['output'])
                elif assignment.startswith('if') and assignment not in ignored_ifs:
                    new_list, cont_or_break = self.fetch_scatter_inputs_chain(self.workflows_dictionary[wf][assignment]['body'],
                                                                        assigned,
                                                                        ignored_ifs,
                                                                        inputs_list=[])
                    scatternamespace += new_list
                    if not cont_or_break:
                        return scatternamespace
        return scatternamespace

    def fetch_scatter_inputs_chain(self, inputs, assigned, ignored_ifs, inputs_list):
        for i in inputs:
            if i == assigned:
                return inputs_list, False
            elif i.startswith('call'):
                if 'outputs' in self.tasks_dictionary[inputs[i]['task']]:
                    for output in self.tasks_dictionary[inputs[i]['task']]['outputs']:
                        inputs_list.append(inputs[i]['alias'] + '_' + output[0])
            elif i.startswith('scatter'):
                for var in self.fetch_scatter_outputs(inputs[i]):
                    inputs_list.append(var['task'] + '_' + var['output'])
            elif i.startswith('if') and i not in ignored_ifs:
                inputs_list, cont_or_break = self.fetch_scatter_inputs_chain(inputs[i]['body'], assigned, ignored_ifs, inputs_list)
                if not cont_or_break:
                    return inputs_list, False
        return inputs_list, True

    def write_main_jobwrappers_call(self, task):
        main_section = '        {} = job0.addChild({}Cls('.format(task['alias'], task['task'])
        for var in task['io']:
            main_section += var + '=' + task['io'][var] + ', '
        if main_section.endswith(', '):
            main_section = main_section[:-2]
        main_section += '))\n'

        call_outputs = self.fetch_call_outputs(task)
        for var in call_outputs:
            main_section += '        {var} = {task}.rv("{output}")\n'.format(var=var['task'] + '_' + var['output'],
                                                                             task=var['task'],
                                                                             output=var['output'])
        return main_section

    def fetch_call_outputs(self, task):
        calloutputs = []
        if 'outputs' in self.tasks_dictionary[task['task']]:
            for output in self.tasks_dictionary[task['task']]['outputs']:
                calloutputs.append({'task': task['alias'], 'output': output[0]})
        return calloutputs

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

        for wf in self.workflows_dictionary:
            for assignment in self.workflows_dictionary[wf]:
                if assignment.startswith('scatter'):
                    fn_section += self.write_scatterfunction(self.workflows_dictionary[wf][assignment], assignment)
                if assignment.startswith('if'):
                    fn_section += self.write_scatterfunctions_within_if(self.workflows_dictionary[wf][assignment]['body'])

        return fn_section

    def write_scatterfunctions_within_if(self, ifstatement):
        fn_section = ''
        for assignment in ifstatement:
            if assignment.startswith('scatter'):
                fn_section += self.write_scatterfunction(ifstatement[assignment], assignment)
            if assignment.startswith('if'):
                fn_section += self.write_scatterfunctions_within_if(ifstatement[assignment]['body'])
        return fn_section

    def write_scatterfunction(self, job, scattername):
        '''
        Writes out a python function for each WDL "scatter" object.
        '''

        scatter_outputs = self.fetch_scatter_outputs(job)

        # write the function header
        fn_section = self.write_scatterfunction_header(scattername)

        # write the scatter definitions
        fn_section += self.write_scatterfunction_lists(scatter_outputs)

        # write
        fn_section += self.write_scatterfunction_loop(job, scatter_outputs)

        # write the outputs for the task to return
        fn_section += self.write_scatterfunction_outputreturn(scatter_outputs)

        return fn_section

    def write_scatterfunction_header(self, scattername):
        """

        :return:
        """
        scatter_inputs = self.fetch_scatter_inputs(scattername)

        fn_section = '\n\nclass {jobname}Cls(Job):\n'.format(jobname=scattername)
        fn_section += '    def __init__(self, '
        for input in scatter_inputs:
            fn_section += '{input}=None, '.format(input=input)
        fn_section += '*args, **kwargs):\n'
        fn_section += '        Job.__init__(self)\n\n'

        for input in scatter_inputs:
            fn_section += '        self.id_{input} = {input}\n'.format(input=input)

        fn_section += heredoc_wdl('''

                             def run(self, fileStore):
                                 fileStore.logToMaster("{jobname}")
                                 tempDir = fileStore.getLocalTempDir()
                                 
                                 try:
                                     os.makedirs(os.path.join(tempDir, 'execution'))
                                 except OSError as e:
                                     if e.errno != errno.EEXIST:
                                         raise
                                 ''', {'jobname': scattername}, indent='    ')[1:]
        for input in scatter_inputs:
            fn_section += '        {input} = self.id_{input}\n'.format(input=input)
        return fn_section

    def write_scatterfunction_outputreturn(self, scatter_outputs):
        """

        :return:
        """
        fn_section = '\n        rvDict = {'
        for var in scatter_outputs:
            fn_section += '"{var}": {var}, '.format(var=var['task'] + '_' + var['output'])
        if fn_section.endswith(', '):
            fn_section = fn_section[:-2]
        fn_section += '}\n'
        fn_section += '        return rvDict\n\n'

        return fn_section[:-1]

    def write_scatterfunction_lists(self, scatter_outputs):
        """

        :return:
        """
        fn_section = '\n'
        for var in scatter_outputs:
            fn_section += '        {var} = []\n'.format(var=var['task'] + '_' + var['output'])

        return fn_section

    def write_scatterfunction_loop(self, job, scatter_outputs):
        """

        :return:
        """
        collection = job['collection']
        item = job['item']

        fn_section = '        for {item} in {collection}:\n'.format(item=item, collection=collection)

        previous_dependency = 'self'
        for statement in job['body']:
            if statement.startswith('call'):
                fn_section += self.write_scatter_callwrapper(job['body'][statement], previous_dependency)
                previous_dependency = 'job_' + job['body'][statement]['alias']
            elif statement.startswith('variable'):
                fn_section += '            {var} = {expr}\n'.format(var=job['body'][statement]['name'],
                                                                    expr=job['body'][statement]['value'])

        for var in scatter_outputs:
            fn_section += '            {var}.append({task}.rv("{output}"))\n'.format(var=var['task'] + '_' + var['output'],
                                                                                   task='job_' + var['task'],
                                                                                   output=var['output'])
        return fn_section

    def write_scatter_callwrapper(self, job, previous_dependency):
        fn_section = '            job_{alias} = {pd}.addFollowOn({task}Cls('.format(alias=job['alias'],
                                                                                    pd=previous_dependency,
                                                                                    task=job['task'])
        for var in job['io']:
            fn_section += var + '=' + job['io'][var] + ', '
        if fn_section.endswith(', '):
            fn_section = fn_section[:-2]
        fn_section += '))\n'
        return fn_section

    def fetch_scatter_varwrapper_inputs(self, job, scattername):
        varwrapper_inputs = self.fetch_scatter_inputs(scattername)

        for wf in self.workflows_dictionary:
            for assignment in self.workflows_dictionary[wf]:
                if assignment == scattername:
                    for scatterassignment in self.workflows_dictionary[wf][assignment]['body']:
                        if scatterassignment.startswith('variable'):
                            if job['name'] == self.workflows_dictionary[wf][assignment]['body'][scatterassignment]['name']:
                                varwrapper_inputs.append(self.workflows_dictionary[wf][assignment]['item'])
                                return varwrapper_inputs
                            else:
                                varwrapper_inputs.append(self.workflows_dictionary[wf][assignment]['body'][scatterassignment]['name'])
                        if scatterassignment.startswith('call'):
                            varwrapper_inputs.append(self.workflows_dictionary[wf][assignment]['body'][scatterassignment]['alias'])
                    varwrapper_inputs.append(self.workflows_dictionary[wf][assignment]['item'])
        return varwrapper_inputs

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
        fn_section += '        super({jobname}Cls, self).__init__(*args, **kwargs)\n'.format(jobname=job)

        # TODO: Resolve inherent problems resolving resource requirements
        # In WDL, "local-disk " + 500 + " HDD" cannot be directly converted to python.
        # This needs a special handler.
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
                if json_expressn is not None:
                    var_expressn = json_expressn
                if var_expressn is None:
                    var_expressn = var

                fn_section += '        self.id_{} = {}\n'.format(var, var_expressn)

        fn_section += heredoc_wdl('''

                             def run(self, fileStore):
                                 fileStore.logToMaster("{jobname}")
                                 tempDir = fileStore.getLocalTempDir()
                                 
                                 _toil_wdl_internal__stdout_file = os.path.join(tempDir, 'stdout')
                                 _toil_wdl_internal__stderr_file = os.path.join(tempDir, 'stderr')
                                 
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
                # file or compound types
                if var_type in ('File', 'Pair', 'Map'):
                    args = ', '.join([f'self.id_{var}',
                                      f'var_type={repr(var_type)}',
                                      'read_in_file=True',
                                      'file_store=fileStore',
                                      'cwd=_toil_wdl_internal__current_working_dir',
                                      'temp_dir=tempDir',
                                      f'docker={docker_bool}'])

                    # inputs may be parsed again if they are from the workflow, but this time we need
                    # to read the input files to our tempDir.
                    fn_section += '        {} = parse_value_from_type({})\n'.format(var, args)
                else:
                    fn_section += '        {} = self.id_{}\n'.format(var, var)

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
        fn_section = "        generate_docker_bashscript_file(temp_dir=tempDir, docker_dir=tempDir, globs=["
        # TODO: Add glob
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
                       "job_task_reference": job,
                       "docker_user": str(self.docker_user)}
        docker_template = heredoc_wdl('''
        # apiDockerCall() with demux=True returns a tuple of bytes objects (stdout, stderr).
        _toil_wdl_internal__stdout, _toil_wdl_internal__stderr = \\
            apiDockerCall(self, 
                          image={docker_image}, 
                          working_dir=tempDir, 
                          parameters=[os.path.join(tempDir, "{job_task_reference}_script.sh")], 
                          entrypoint="/bin/bash", 
                          user={docker_user}, 
                          stderr=True, 
                          demux=True, 
                          volumes={{tempDir: {{"bind": tempDir}}}})
        with open(os.path.join(_toil_wdl_internal__current_working_dir, '{job_task_reference}.log'), 'wb') as f:
            if _toil_wdl_internal__stdout:
                f.write(_toil_wdl_internal__stdout)
            if _toil_wdl_internal__stderr:
                f.write(_toil_wdl_internal__stderr)
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
                if not cmd.startswith("r'''"):
                    cmd = 'str({i} if not isinstance({i}, tuple) else process_and_read_file({i}, tempDir, fileStore)).strip("{nl}")'.format(i=cmd, nl=r"\n")
                fn_section = fn_section + heredoc_wdl('''
                        try:
                            # Intended to deal with "optional" inputs that may not exist
                            # TODO: handle this better
                            command{num} = {cmd}
                        except:
                            command{num} = ''\n''', {'cmd': cmd, 'num': self.cmd_num}, indent='        ')
                cmd_array.append('command' + str(self.cmd_num))
                self.cmd_num = self.cmd_num + 1

        if cmd_array:
            fn_section += '\n        cmd = '
            for command in cmd_array:
                fn_section += '{command} + '.format(command=command)
            if fn_section.endswith(' + '):
                fn_section = fn_section[:-3]
            fn_section += '\n        cmd = textwrap.dedent(cmd.strip("{nl}"))\n'.format(nl=r"\n")

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
                this_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                _toil_wdl_internal__stdout, _toil_wdl_internal__stderr = this_process.communicate()\n''', indent='        ')

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

        fn_section += heredoc_wdl('''
            _toil_wdl_internal__stdout_file = generate_stdout_file(_toil_wdl_internal__stdout, 
                                                                   tempDir, 
                                                                   fileStore=fileStore)
            _toil_wdl_internal__stderr_file = generate_stdout_file(_toil_wdl_internal__stderr, 
                                                                   tempDir, 
                                                                   fileStore=fileStore, 
                                                                   stderr=True)
        ''', indent='        ')[1:]

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
                fn_section += '        return rvDict\n\n'

        return fn_section

    def indent_docstring(self, string2indent):
        split_string = string2indent.split('\n')
        new_string = ''
        for line in split_string:
            new_string += '    {}\n'.format(line)
        return new_string

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
