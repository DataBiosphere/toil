from toil.job import Job
import wdl
import wdl.binding
from wdl.binding import WdlStandardLibraryFunctions, Call, Scatter
from wdl.values import *
import subprocess
import io
import re
import sys
import json
import os
import argparse
import traceback

# TODO
#
# 1) Change wrapped_wdl_task to take a Call parameter - pickle bug
# 2) Initial staging of files
# 3) Outputting files from tasks (perminently)

# Call like this:
#
# $ rm -rf toilWorkflow && wdltoil './toilWorkflow' src/toil/wdl/2.wdl src/toil/wdl/2.inputs --logDebug

###
# Call name -> WdlObject with the outputs of that call
###
symbol_store = {}

####
# TODO: replace all of the parameters below with Call object (can't now because of pickle bug)
#
# command - Abstract command that needs to be instantiated
# inputs - Dictionary of name -> WdlValue for user-specified inputs
# input_expressions - Dictionary of name -> WdlExpression (from the 'call' statement)
# declarations - List of Declarations on the wrapped Task object
# outputs - List of wdl.binding.Declaration for each item in the "output {}" section of a task
####

def wrapped_wdl_task(job, call_name, command, inputs, input_expressions, declarations, outputs, **kwargs):

    # TODO: use something like this for getting files into fileStore
    # file_id = job.fileStore.importUrl("s3://sdfsdf")
    # local_path = job.fileStore.readGlobalFile(file_id)

    def lookup_func(var):
        decl = [decl for decl in declarations if decl.name == var and decl.expression is not None]
    	if var in inputs:
	    val = inputs[var]
	elif var in symbol_store:
	    val = symbol_store[var]
	elif var in input_expressions:
	    val = input_expressions[var].eval(lookup_func)
	elif len(decl) == 1:
	    val = decl[0].expression.eval(lookup_func)
	else:
	    job.fileStore.logToMaster('lookup_func: unable to lookup {}'.format(var))
	    return None

	if isinstance(val, WdlFile):
	    try:
	        path = job.fileStore.readGlobalFile(val.value)
		val = WdlFile(path)
	    except NoSuchFileException as e:
	        pass

	return val

    command = command.instantiate(lookup_func)
    stdout_file = job.fileStore.getLocalTempFile()
    stderr_file = job.fileStore.getLocalTempFile()
    cwd_dir = job.fileStore.getLocalTempDir()

    job.fileStore.logToMaster('[COMMAND] --> {}'.format(command))
    job.fileStore.logToMaster("[STDOUT] --> {}".format(stdout_file))
    job.fileStore.logToMaster("[STDERR] --> {}".format(stderr_file))
    job.fileStore.logToMaster("[CWD] --> {}".format(cwd_dir))

    # TODO: handle Docker by wrapping command in a 'docker exec'
    proc = subprocess.Popen(
	command,
	shell=True,
	universal_newlines=True,
	stdout=open(stdout_file, 'w'),
	stderr=open(stderr_file, 'w'),
	close_fds=True,
	cwd=cwd_dir
    )

    proc.communicate()

    with open(stdout_file) as fp:
	job.fileStore.logToMaster('STDOUT: `{}`'.format(fp.read()))
    with open(stderr_file) as fp:
	job.fileStore.logToMaster('STDERR: `{}`'.format(fp.read()))

    # Implementation of all of the WDL standard library functions
    # like read_lines(), stdout(), write_map(), etc.
    #
    # Base class handles most work, all that needs to be implemented
    # are the three functions listed below
    class ToilWdlFunctions(WdlStandardLibraryFunctions):
        def read_file(self, path):
            if not os.path.isabs(path.value):
                path = os.path.join(cwd_dir, path.value)
            else:
                path = path.value
            with open(path) as fp:
                return fp.read()
        def stdout(self, parameters):
            if len(parameters) != 0:
                raise wdl.binding.EvalException('stdout() does not take any parameters')
            return WdlFile(stdout_file)
        def stderr(self, parameters):
            if len(parameters) != 0:
                raise wdl.binding.EvalException('stderr() does not take any parameters')
            return WdlFile(stderr_file)

    # Evaluate all output expressions, return a dictionary of output name -> WdlValue
    job_outputs = {'rc': proc.returncode}
    for output in outputs:
    	uncoerced_value = wdl.binding.eval(output.expression, lookup_func, ToilWdlFunctions())
	job_outputs[output.name] = wdl.binding.coerce(uncoerced_value, output.type)

	if isinstance(job_outputs[output.name], WdlFile):
	    path = os.path.join(cwd_dir, job_outputs[output.name].value)
	    file_id = job.fileStore.writeGlobalFile(path)
	    with open(path) as fp:
	        job.fileStore.logToMaster('CONTENTS: `{}`'.format(fp.read()))
	    job.fileStore.logToMaster('Added {} to file store [{}]'.format(path, file_id))
	    job_outputs[output.name] = WdlFile(file_id)

    	job.fileStore.logToMaster("[OUTPUT] --> {} = {}".format(output.name, job_outputs[output.name]))

    symbol_store[call_name] = WdlObject(job_outputs)

    # TODO: use fileStore.exportUrl(file_id) once it's available
    return job_outputs

def rootJob(job, coerced_inputs):
    # Upload all files to the file store.  Create a map of Toil FileID -> WdlFile
    for k, v in coerced_inputs.items():
        if isinstance(v, WdlFile) and False:
            # TODO: this won't work yet, because we can't call job.fileStore.writeGlobalFile()
            # TODO: support Array[File], Map[File, String], etc
            file_id = job.fileStore.writeGlobalFile(v.value)
            coerced_inputs[k] = WdlFile(file_id)
    return symbol_store

def main():
    parser = Job.Runner.getDefaultArgumentParser()
    parser.add_argument('wdl_file')
    parser.add_argument('wdl_inputs_json')
    cli = parser.parse_args()

    wdl_namespace = wdl.load(cli.wdl_file)
    if len(wdl_namespace.workflows) != 1:
        sys.exit('Current implementation requires exactly one workflow definition per WDL file')
    workflow = wdl_namespace.workflows[0]

    # Process the user's JSON inputs file:
    #
    # Resolve all keys to declarations and coerce all values into WdlValues.
    with open(cli.wdl_inputs_json) as fp:
        try:
            wdl_coerced_inputs = wdl.binding.coerce_inputs(wdl_namespace, json.loads(fp.read()))
        except Exception as e:
            sys.exit(traceback.format_exc(e))

    # TODO: detect missing inputs

    # Takes WDL Scope returns Toil Job.  For each scope downstream of
    # this one, it will be turned into a Toil Job via recursive call
    # and then add that as a child of the upstream scope.
    def wdl_scope_to_toil_job(scope):
        if isinstance(scope, Call):
            fqn = scope.fully_qualified_name
            task_inputs = {}
            for k, v in wdl_coerced_inputs.items():
                match = re.match(r'^{0}\.([a-zA-Z_]+)$'.format(fqn), k)
                if match is not None:
                    task_inputs[match.group(1)] = v
            toil_job = Job.wrapJobFn(wrapped_wdl_task, scope.name, scope.task.command, task_inputs, scope.inputs, scope.task.declarations, scope.task.outputs)
	    for downstream_scope in scope.downstream():
	    	downstream_job = wdl_scope_to_toil_job(downstream_scope)
		toil_job.addChild(downstream_job)
            return toil_job
        else:
            sys.exit('Currently unsupported node: {}'.format(scope.fully_qualified_name))

    root = Job.wrapJobFn(rootJob, wdl_coerced_inputs)
    for child in workflow.body:
        if child.upstream() == set():
	    root.addChild(wdl_scope_to_toil_job(child))

    # TODO: add a final step that will aggregate output files of the workflow to somewhere more persistent as a stop-gap

    print(Job.Runner.startToil(root, cli))
