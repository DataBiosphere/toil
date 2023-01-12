#!/usr/bin/env python3
# Copyright (C) 2018-2022 UCSC Computational Genomics Lab
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
import argparse
import collections
import copy
import glob
import itertools
import json
import logging
import os
import shlex
import subprocess
import sys
import tempfile
import uuid

from typing import cast, Any, Callable, Union, Dict, List, Optional, Set, Sequence, Tuple, TypeVar, Iterator

import WDL

from toil.common import Config, Toil, addOptions
from toil.job import Job, JobFunctionWrappingJob, Promise, Promised, unwrap, unwrap_all
from toil.fileStores import FileID
from toil.fileStores.abstractFileStore import AbstractFileStore

logger = logging.getLogger(__name__)

# Bindings have a long type name
WDLBindings = WDL.Env.Bindings[WDL.Value.Base]

def combine_bindings(all_bindings: Sequence[WDLBindings]) -> WDLBindings:
    """
    Combine variable bindings from multiple predecessor tasks into one set for
    the current task.
    """
    
    # Sort, largest last
    all_bindings = sorted(all_bindings, key=lambda x: len(x))
    
    # Merge them up
    return WDL.Env.merge(*all_bindings)
    
def log_bindings(all_bindings: Sequence[Promised[WDLBindings]]) -> None:
    """
    Log bindings to the console, even if some are still promises.
    """
    for bindings in all_bindings:
        if isinstance(bindings, WDL.Env.Bindings):
            for binding in bindings:
                logger.info("%s = %s", binding.name, binding.value)
        elif isinstance(bindings, Promise):
            logger.info("<Unfulfilled promise for bindings>")
            
def get_supertype(types: Sequence[Optional[WDL.Type.Base]]) -> WDL.Type.Base:
    """
    Get the supertype that can hold values of all the given types.
    """
    
    if None in types:
        # Need to allow optional values
        if len(types) == 1:
            # Only None is here
            return WDL.Type.Any(optional=True)
        if len(types) == 2:
            # None and something else
            for item in types:
                if item is not None:
                    # Return the type that's actually there, but make optional if not already.
                    return item.copy(optional=True)
            raise RuntimeError("Expected non-None in types could not be found")
        else:
            # Multiple types, and some nulls, so we need an optional Any.
            return WDL.Type.Any(optional=True)
    else:
        if len(types) == 1:
            # Only one type. It isn't None.
            the_type = types[0]
            assert the_type is not None
            return the_type
        else:
            # Multiple types (or none). Assume Any
            return WDL.Type.Any()
            

def for_each_node(root: WDL.Tree.WorkflowNode) -> Iterator[WDL.Tree.WorkflowNode]:
    """
    Iterate over all WDL workflow nodes in the given node.
    
    Only goes into one level of section-ing: will show conditional and scatter
    nodes, but not their contents.
    """
    
    logger.debug('WorkflowNode: %s: %s %s', type(root), root, root.workflow_node_id)
    yield root
    for child_node in root.children:
        logger.debug('Child %s of %s', child_node, root)
        if isinstance(child_node, WDL.Tree.WorkflowNode):
            if isinstance(child_node, WDL.Tree.WorkflowSection):
                # Stop at section boundaries
                yield child_node
            else:
                for result in for_each_node(child_node):
                    yield result
        else:
            if hasattr(child_node, 'workflow_node_id'):
                logger.debug('Non-WorkflowNode: %s: %s %s', type(child_node), child_node, child_node.workflow_node_id)
            else:
                logger.debug('Non-WorkflowNode: %s: %s !!!NO_ID!!!', type(child_node), child_node)
                
class ToilWDLStdLibBase(WDL.StdLib.Base):
    """
    Standard library implementation for WDL as run on Toil.
    """
    
    def __init__(self, file_store: AbstractFileStore):
        """
        Set up the standard library.
        """
        # TODO: Just always be the 1.2 standard library.
        wdl_version = "1.2"
        # Where should we be writing files that write_file() makes?
        write_dir = file_store.getLocalTempDir()
        # Set up miniwdl's implementation (which may be WDL.StdLib.TaskOutputs)
        super().__init__(wdl_version, write_dir)
        
        # Keep the file store around so we can access files.
        self._file_store = file_store
        
    # Both the WDL code itself **and** the commands that it runs will deal in
    # "virtualized" filenames by default, so when making commands we need to
    # make sure to devirtualize filenames.
    # We have to guarantee that "When a WDL author uses a File input in their
    # Command Section, the fully qualified, localized path to the file is
    # substituted when that declaration is referenced in the command template."
        
    def _devirtualize_filename(self, filename: str) -> str:
        """
        'devirtualize' filename passed to a read_* function: return a filename that can be open()ed
        on the local host. 
        """
        
        # TODO: Support people doing path operations (join, split, get parent directory) on the virtualized filenames.
        # TODO: For task inputs, we are supposed to make sure to put things in the same directory if they came from the same directory. See <https://github.com/openwdl/wdl/blob/main/versions/1.0/SPEC.md#task-input-localization>
        
        if filename.startswith('toilfile:'):
            # This is a reference to the Toil filestore.
            # Deserialize the FileID
            file_id = FileID.unpack(filename[len("toilfile:") :])
            # And get a local path to the file
            result = self._file_store.readGlobalFile(file_id)
        elif filename.startswith('http:') or filename.startswith('https:') or filename.startswith('s3:'):
            # This is a URL that we think Toil knows how to read.
            # Import into the job store from here and then download to the node.
            # TODO: Can we predict all the URLs that can be used up front and do them all on the leader, where imports are meant to happen?
            imported = self._file_store.import_file(filename)
            assert imported is not None
            # And get a local path to the file
            result = self._file_store.readGlobalFile(imported)
        else:
            # This is a local file
            result = filename
        
        logger.info('Devirtualized %s as openable file %s', filename, result)
        return result
        
    def _virtualize_filename(self, filename: str) -> str:
        """
        from a local path in write_dir, 'virtualize' into the filename as it should present in a
        File value
        """
        
        VIRTUAL_SCHEMES = ['http:', 'https:', 's3:', 'gs:', 'toilfile:']
        for scheme in VIRTUAL_SCHEMES:
            if filename.startswith(scheme):
                # Already virtual
                logger.info('Virtualized %s as WDL file %s', filename, filename)
                return filename
        
        # Otherwise this is a local file and we want to fake it as a Toil file store file
        file_id = self._file_store.writeGlobalFile(filename)
        result = 'toilfile:' + file_id.pack() 
        logger.info('Virtualized %s as WDL file %s', filename, result)
        return result
    
class ToilWDLStdLibTaskOutputs(ToilWDLStdLibBase, WDL.StdLib.TaskOutputs):
    """
    Standard library implementation for WDL as run on Toil, with additional
    functions only allowed in task output sections.
    """
    
    def __init__(self, file_store: AbstractFileStore, stdout_path: str, stderr_path: str):
        """
        Set up the standard library for a task output section. Needs to know
        where standard output and error from the task have been stored.
        """
        
        # Just set up as ToilWDLStdLibBase, but it will call into
        # WDL.StdLib.TaskOutputs next.
        super().__init__(file_store)
        
        # Remember task putput files
        self._stdout_path = stdout_path
        self._stderr_path = stderr_path
        
        # We need to attach implementations for WDL's stdout(), stderr(), and glob().
        # TODO: Can we use the fancy decorators instead of this wizardry?
        setattr(
            self,
            "stdout",
            WDL.StdLib.StaticFunction("stdout", [], WDL.Type.File(), self._stdout),
        )
        setattr(
            self,
            "stderr",
            WDL.StdLib.StaticFunction("stderr", [], WDL.Type.File(), self._stderr),
        )
        setattr(
            self,
            "glob",
            WDL.StdLib.StaticFunction("glob", [WDL.Type.String()], WDL.Type.Array(WDL.Type.File()), self._glob),
        )
    
    def _stdout(self) -> WDL.Value.File:
        """
        Get the standard output of the command that ran, as a WDL File.
        """
        return WDL.Value.File(self._stdout_path)
    
    def _stderr(self) -> WDL.Value.File:
        """
        Get the standard error of the command that ran, as a WDL File.
        """
        return WDL.Value.File(self._stderr_path)
    
    def _glob(self, pattern: WDL.Value.String) -> WDL.Value.Array:
        """
        Get a WDL Array of WDL Files left behind by the job that ran, matching the given glob pattern.
        """
        
        # Unwrap the pattern
        pattern_string = pattern.coerce(WDL.Type.String()).value
       
        # The spec says we really are supposed to invoke `bash` and pass it
        # `echo <the pattern>`, and that `bash` is allowed to be
        # "non-standard", so if you use a Docker image you could ship any code
        # you want as "bash" and we have to run it and then filter out the
        # directories.
        
        # Problem: `echo <the pattern>` just dumps space-delimited filenames which may themselves contain spaces, so we can't actually correctly recover them, if we need to allow for `echo <the pattern>` being able to do arbitrary things in the container's Bash other than interpreting the pattern
        # So we send a little Bash script that can delimit the files with something, and assume the Bash really is a Bash.
        
        # TODO: get this to run in the right container if there is one
        # Bash (now?) has a compgen builtin for shell completion that can evaluate a glob where the glob is in a quotes string that might have spaces in it. See <https://unix.stackexchange.com/a/616608>.
        # This will handle everything except newlines in the filenames.
        # TODO: Newlines in the filenames?
        # Since compgen will return 1 if nothing matches, we need to allow a failing exit code here.
        lines = subprocess.run(['bash', '-c', 'compgen -G ' + shlex.quote(pattern_string)], stdout=subprocess.PIPE).stdout.decode('utf-8')
        
        # Get each name that is a file
        results = []
        for line in lines.split('\n'):
            if not line:
                continue
            if not os.path.isfile(line):
                continue
            results.append(line)
            
        for filename in os.listdir('.'):
            logger.info('I see file: %s', filename)
            
        # Just turn them all into WDL File objects with virtualized names.
        return WDL.Value.Array(WDL.Type.File(), [WDL.Value.File(x) for x in results]) 
            
def evaluate_named_expression(context: Union[WDL.Error.SourceNode, WDL.Error.SourcePosition], name: str, expected_type: Optional[WDL.Type.Base], expression: Optional[WDL.Expr.Base], environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDL.Value.Base:
    """
    Evaluate an expression when we know the name of it.
    """
    
    if expression is None:
        if expected_type and expected_type.optional:
            # We can just leave the value as null
            value: WDL.Value.Base = WDL.Value.Null()
        else:
            raise WDL.Error.EvalError(context, "Cannot evaluate no expression for " + name)
    else:
        logger.debug("Evaluate expression for %s: %s", name, expression)
        logger.debug("In environment:")
        log_bindings([environment])
        
        if expected_type:
            # Make sure the types are allowed
            expression.typecheck(expected_type)
        
        # Do the actual evaluation
        value = expression.eval(environment, stdlib)
    
    if expected_type:
        # Coerce to the type it should be.
        value = value.coerce(expected_type)
    
    return value

def evaluate_decl(node: WDL.Tree.Decl, environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDL.Value.Base:
    """
    Evaluate the expression of a declaration node, or raise an error.
    """
    
    return evaluate_named_expression(node, node.name, node.type, node.expr, environment, stdlib)
    
def evaluate_call_inputs(context: Union[WDL.Error.SourceNode, WDL.Error.SourcePosition], expressions: Dict[str, WDL.Expr.Base], environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Evaluate a bunch of expressions with names, and make them into a fresh set of bindings.
    """
    
    new_bindings: WDLBindings = WDL.Env.Bindings()
    for k, v in expressions.items():
        # Add each binding in turn
        new_bindings = new_bindings.bind(k, evaluate_named_expression(context, k, None, v, environment, stdlib))
    return new_bindings
    
def evaluate_defaultable_decl(node: WDL.Tree.Decl, environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDL.Value.Base:
    """
    If the name of the declaration is already defined in the environment, return its value. Otherwise, return the evaluated expression.
    """
    
    if node.name in environment:
        logger.debug('Name %s is already defined, not using default', node.name)
        return environment[node.name]
    else:
        logger.info('Defaulting %s to %s', node.name, node.expr)
        return evaluate_decl(node, environment, stdlib)

# TODO: make these stdlib methods???
def devirtualize_files(environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that are actually available to command line commands.
    """
    
    return map_over_files_in_bindings(environment, stdlib._devirtualize_filename)
    
def virtualize_files(environment: WDLBindings, stdlib: WDL.StdLib.Base) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that are usable from other machines.
    """
    
    return map_over_files_in_bindings(environment, stdlib._virtualize_filename)
    
def drop_missing_files(environment: WDLBindings) -> WDLBindings:
    """
    Make sure all the File values embedded in the given bindings point to files
    that exist, or are null.
    
    Files must not be virtualized.
    """
    
    # TODO: Howe do we know all the missing files are actually `File?`?
    
    def drop_if_missing(filename: str) -> Optional[str]:
        """
        Return None if a file doesn't exist, or its path if it does.
        """
        if os.path.exists(filename):
            return filename
        return None
    
    return map_over_files_in_bindings(environment, drop_if_missing)
    
def map_over_files_in_bindings(environment: WDLBindings, transform: Callable[[str], Optional[str]]) -> WDLBindings:
    """
    Run all File values embedded in the given bindings through the given
    transformation function.
    """
    
    return environment.map(lambda b: map_over_files_in_binding(b, transform))
    

def map_over_files_in_binding(binding: WDL.Env.Binding[WDL.Value.Base], transform: Callable[[str], Optional[str]]) -> WDL.Env.Binding[WDL.Value.Base]:
    """
    Run all File values embedded in the given binding's value through the given
    transformation function.
    """
    
    return WDL.Env.Binding(binding.name, map_over_files_in_value(binding.value, transform), binding.info)

# TODO: We want to type this to say, for anything descended from a WDL type, we
# return something descended from the same WDL type or a null. But I can't
# quite do that with generics, since you could pass in some extended WDL value
# type we've never heard of and expect to get one of those out.
#
# For now we assume that any types extending the WDL value types will implement
# compatible constructors.
def map_over_files_in_value(value: WDL.Value.Base, transform: Callable[[str], Optional[str]]) -> WDL.Value.Base:
    """
    Run all File values embedded in the given value through the given
    transformation function.
    
    If the transform returns None, the file value is changed to Null.
    """
    
    if isinstance(value, WDL.Value.File):
        # This is a file so we need to process it
        new_path = transform(value.value)
        if new_path is None:
            return WDL.Value.Null()
        else:
            # Make whatever the value is around the new path.
            # TODO: why does this need casting?
            return WDL.Value.File(new_path, value.expr)
    elif isinstance(value, WDL.Value.Array):
        # This is an array, so recurse on the items
        return WDL.Value.Array(value.type.item_type, [map_over_files_in_value(v, transform) for v in value.value], value.expr)
    elif isinstance(value, WDL.Value.Map):
        # This is a map, so recurse on the members of the items, which are tuples (but not wrapped as WDL Pair objects)
        # TODO: Can we avoid a cast in a comprehension if we get MyPy to know that each pair is always a 2-element tuple?
        return WDL.Value.Map(value.type.item_type, [cast(Tuple[WDL.Value.Base, WDL.Value.Base], tuple((map_over_files_in_value(v, transform) for v in pair))) for pair in value.value], value.expr)
    elif isinstance(value, WDL.Value.Pair):
        # This is a pair, so recurse on the left and right items
        return WDL.Value.Pair(value.type.left_type, value.type.right_type, cast(Tuple[WDL.Value.Base, WDL.Value.Base], tuple((map_over_files_in_value(v, transform) for v in value.value))), value.expr)
    elif isinstance(value, WDL.Value.Struct):
        # This is a struct, so recurse on the values in the backing dict
        return WDL.Value.Struct(cast(Union[WDL.Type.StructInstance, WDL.Type.Object], value.type), {k: map_over_files_in_value(v, transform) for k, v in value.value.items()}, value.expr)
    else:
        # All other kinds of value can be passed through unmodified.
        return value
    
class WDLInputJob(Job):
    """
    Job that evaluates a WDL input, or sources it from the workflow inputs.
    """
    def __init__(self, node: WDL.Tree.Decl, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        super().__init__(unitName=node.workflow_node_id, displayName=node.workflow_node_id, **kwargs)
        
        self._node = node
        self._prev_node_results = prev_node_results
        
    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        logger.info("Running node %s", self._node.workflow_node_id)
        
        # Combine the bindings we get from previous jobs
        incoming_bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)
        
        return incoming_bindings.bind(self._node.name, evaluate_defaultable_decl(self._node, incoming_bindings, standard_library))
        
class WDLTaskJob(Job):
    """
    Job that runs a WDL task.
    
    Responsible for evaluating the input declarations for unspecified inputs,
    evaluating the runtime section, re-scheduling if resources are not
    available, running any command, and evaluating the outputs. 
    
    All bindings are in terms of task-internal names.
    """
    
    def __init__(self, task: WDL.Tree.Task, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        super().__init__(unitName=task.name, displayName=task.name, **kwargs)
        
        self._task = task
        self._prev_node_results = prev_node_results
        
    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        logger.info("Running task %s", self._task.name)
        
        # Combine the bindings we get from previous jobs.
        # For a task we are only passed the inside-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)
        
        if self._task.inputs:
            for input_decl in self._task.inputs:
                # Evaluate all the inputs that aren't pre-set
                bindings = bindings.bind(input_decl.name, evaluate_defaultable_decl(input_decl, bindings, standard_library))
        for postinput_decl in self._task.postinputs:
            # Evaluate all the postinput decls
            bindings = bindings.bind(postinput_decl.name, evaluate_defaultable_decl(postinput_decl, bindings, standard_library))
        
        # Evaluate the runtime section
        runtime_bindings = evaluate_call_inputs(self._task, self._task.runtime, bindings, standard_library)
        
        # TODO: Re-schedule if we need more resources
        
        # Make standard output and error files
        stdout_path = file_store.getLocalTempFile(prefix='stdout', suffix='.txt')
        stderr_path = file_store.getLocalTempFile(prefix='stderr', suffix='.txt')
        
        if self._task.command:
            # When the command string references a File, we need to get a path to the file on a local disk, which the commnad will be able to actually use, accounting for e.g. containers.
            # TODO: Figure out whan the command template actually uses File values and lazily download them.
            # For now we just grab all the File values in the inside-the-task environment, since any of them *might* be used.
            # TODO: MiniWDL can parallelize the fetch
            bindings = devirtualize_files(bindings, standard_library)
            
            # Work out the command string, and unwrap it
            command_string: str = evaluate_named_expression(self._task, "command", WDL.Type.String(), self._task.command, bindings, standard_library).coerce(WDL.Type.String()).value
            
            # And run it, capturing output to files
            logger.info('Executing command: %s', command_string)
            subprocess.check_call(command_string, shell=True, stdout=open(stdout_path, 'wb'), stderr=open(stderr_path, 'wb'))
            # TODO: Accept nonzero exit codes when requested.
            
            logger.info('Standard output: %s', open(stdout_path).read())
            logger.info('Standard error: %s', open(stderr_path).read())
            
        # Evaluate all the outputs in their special library context
        outputs_library = ToilWDLStdLibTaskOutputs(file_store, stdout_path, stderr_path)
        output_bindings: WDLBindings = WDL.Env.Bindings()
        for output_decl in self._task.outputs:
            output_bindings = output_bindings.bind(output_decl.name, evaluate_decl(output_decl, bindings, outputs_library))
            
        # Drop any files from the output which don't actually exist
        # TODO: Check if the type allows the file not to exist!
        output_bindings = drop_missing_files(output_bindings)
            
        # Upload any files in the outputs if not uploaded already
        output_bindings = virtualize_files(output_bindings, outputs_library)
        
        return output_bindings
        
class WDLWorkflowNodeJob(Job):
    """
    Job that evaluates a WDL workflow node.
    """
    
    def __init__(self, node: WDL.Tree.WorkflowNode, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        super().__init__(unitName=node.workflow_node_id, displayName=node.workflow_node_id, **kwargs)
        
        self._node = node
        self._prev_node_results = prev_node_results
        
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        logger.info("Running node %s", self._node.workflow_node_id)
        
        # Combine the bindings we get from previous jobs
        incoming_bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)
        
        if isinstance(self._node, WDL.Tree.Decl):
            # This is a variable assignment
            logger.info('Setting %s to %s', self._node.name, self._node.expr)
            value = evaluate_decl(self._node, incoming_bindings, standard_library)    
            return incoming_bindings.bind(self._node.name, value)
        elif isinstance(self._node, WDL.Tree.Call):
            # This is a call of a task or workflow
            
            # Fetch all the inputs we are passing and bind them.
            # The call is only allowed to use these.
            logger.info("Evaluate step inputs")
            input_bindings = evaluate_call_inputs(self._node, self._node.inputs, incoming_bindings, standard_library)
            
            # Bindings may also be added in from the enclosing workflow inputs
            # TODO: this is letting us also inject them from the workflow body.
            # TODO: Can this result in picking up non-namespaced values that
            # aren't meant to be inputs, by not changing their names?
            passed_down_bindings = incoming_bindings.enter_namespace(self._node.name)
            
            if isinstance(self._node.callee, WDL.Tree.Workflow):
                # This is a call of a workflow
                subjob: Job = WDLWorkflowJob(self._node.callee, [input_bindings, passed_down_bindings])
                self.addChild(subjob)
            elif isinstance(self._node.callee, WDL.Tree.Task):
                # This is a call of a task
                subjob = WDLTaskJob(self._node.callee, [input_bindings, passed_down_bindings])
                self.addChild(subjob)
            else:
                raise WDL.Error.InvalidType(self._node, "Cannot call a " + str(type(self._node.callee)))
                
            # We need to agregate outputs namespaced with our node name, and existing bindings
            namespace_job = WDLNamespaceBindingsJob(self._node.name, [subjob.rv()])
            subjob.addFollowOn(namespace_job)
            self.addChild(namespace_job)
            
            combine_job = WDLCombineBindingsJob([namespace_job.rv(), incoming_bindings])
            namespace_job.addFollowOn(combine_job)
            self.addChild(combine_job)
            
            return combine_job.rv()
        elif isinstance(self._node, WDL.Tree.Scatter):
            subjob = WDLScatterJob(self._node, [incoming_bindings])
            self.addChild(subjob)
            # Scatters don't really make a namespace, just kind of a scope?
            # TODO: Let stuff leave scope!
            return subjob.rv()
        elif isinstance(self._node, WDL.Tree.Conditional):
            subjob = WDLConditionalJob(self._node, [incoming_bindings])
            self.addChild(subjob)
            # Conditionals don't really make a namespace, just kind of a scope?
            # TODO: Let stuff leave scope!
            return subjob.rv()
        else:
            raise WDL.Error.InvalidType(self._node, "Unimplemented WorkflowNode: " + str(type(self._node)))
        
class WDLCombineBindingsJob(Job):
    """
    Job that collects the results from WDL workflow nodes and combines their
    environment changes.
    """
    
    def __init__(self, prev_node_results: Sequence[Promised[WDLBindings]], remove: Optional[Promised[WDLBindings]] = None, **kwargs: Any) -> None:
        """
        Make a new job to combine the results of previous jobs.
        
        If remove is set, bindings there will be subtracted out of the result.
        """
        super().__init__(**kwargs)
        
        self._prev_node_results = prev_node_results
        self._remove = remove
       
    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Aggregate incoming results.
        """
        combined = combine_bindings(unwrap_all(self._prev_node_results))
        if self._remove is not None:
            # We need to take stuff out of scope
            combined = combined.subtract(unwrap(self._remove))
        return combined
        
class WDLNamespaceBindingsJob(Job):
    """
    Job that puts a set of bindings into a namespace.
    """
    
    def __init__(self, namespace: str, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        """
        Make a new job to namespace results.
        """
        super().__init__(**kwargs)
        
        self._namespace = namespace
        self._prev_node_results = prev_node_results
       
    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Apply the namespace
        """
        return combine_bindings(unwrap_all(self._prev_node_results)).wrap_namespace(self._namespace)
        
class WDLSectionJob(Job):
    """
    Job that can create more graph for a section of the wrokflow.
    """
    
    def create_subgraph(self, nodes: Sequence[WDL.Tree.WorkflowNode], environment: WDLBindings, local_environment: Optional[WDLBindings] = None) -> Job:
        """
        Make a Toil job to evaluate a subgraph inside a workflow or workflow section.
        
        Returns a child Job that will return the aggregated environment after
        running all the things in the section.
        
        Bindings in environment will be passed through, but bindings in
        local_environment will go out of scope at the end of the section.
        """
        
        if local_environment is not None:
            # Bring local environment into scope
            environment = combine_bindings([environment, local_environment])
        
        # What nodes actually participate in dependencies?
        wdl_id_to_wdl_node: Dict[str, WDL.Tree.WorkflowNode] = {node.workflow_node_id: node for node in nodes if isinstance(node, WDL.Tree.WorkflowNode)}
        
        # To make Toil jobs, we need all the jobs they depend on made so we can
        # call .rv(). So we need to solve the workflow DAG ourselves to set it up
        # properly.
        
        # What are the dependencies of all the body nodes on things other than workflow inputs?
        wdl_id_to_dependency_ids = {node_id: [dep for dep in node.workflow_node_dependencies if dep in wdl_id_to_wdl_node] for node_id, node in wdl_id_to_wdl_node.items()}
        
        # Which of those are outstanding?
        wdl_id_to_outstanding_dependency_ids = copy.deepcopy(wdl_id_to_dependency_ids)
        
        # What nodes depend on each node?
        wdl_id_to_dependent_ids: Dict[str, Set[str]] = collections.defaultdict(set)
        for node_id, dependencies in wdl_id_to_dependency_ids.items():
            for dependency_id in dependencies:
                # Invert the dependency edges
                wdl_id_to_dependent_ids[dependency_id].add(node_id)
                
        # This will hold all the Toil jobs by WDL node ID
        wdl_id_to_toil_job: Dict[str, Job] = {}
        
        # And collect IDs of jobs with no successors to add a final sink job
        leaf_ids: Set[str] = set()
        
        # What nodes are ready?
        ready_node_ids = {node_id for node_id, dependencies in wdl_id_to_outstanding_dependency_ids.items() if len(dependencies) == 0}
        
        while len(wdl_id_to_outstanding_dependency_ids) > 0:
            logger.debug('Ready nodes: %s', ready_node_ids)
            logger.debug('Waiting nodes: %s', wdl_id_to_outstanding_dependency_ids)
        
            # Find a node that we can do now
            node_id = next(iter(ready_node_ids))
            
            # Say we are doing it
            ready_node_ids.remove(node_id)
            del wdl_id_to_outstanding_dependency_ids[node_id]
            logger.debug('Make Toil job for %s', node_id)
            
            # Collect the return values from previous jobs
            prev_jobs = [wdl_id_to_toil_job[prev_node_id] for prev_node_id in wdl_id_to_dependency_ids[node_id]]
            rvs: List[Union[WDLBindings, Promise]] = [prev_job.rv() for prev_job in prev_jobs]
            # We also need access to section-level bindings like inputs
            rvs.append(environment)
            
            # Use them to make a new job
            job = WDLWorkflowNodeJob(wdl_id_to_wdl_node[node_id], rvs)
            for prev_job in prev_jobs:
                # Connect up the happens-after relationships to make sure the
                # return values are available.
                # We have a graph that only needs one kind of happens-after
                # relationship, so we always use follow-ons.
                prev_job.addFollowOn(job)

            if len(prev_jobs) == 0:
                # Nothing came before this job, so connect it to the workflow.
                self.addChild(job)
                
            # Save the job
            wdl_id_to_toil_job[node_id] = job
                
            if len(wdl_id_to_dependent_ids[node_id]) == 0:
                # Nothing comes after this job, so connect it to sink
                leaf_ids.add(node_id)
            else:
                for dependent_id in wdl_id_to_dependent_ids[node_id]:
                    # For each job that waits on this job
                    wdl_id_to_outstanding_dependency_ids[dependent_id].remove(node_id)
                    logger.debug('Dependent %s no longer needs to wait on %s', dependent_id, node_id)
                    if len(wdl_id_to_outstanding_dependency_ids[dependent_id]) == 0:
                        # We were the last thing blocking them.
                        ready_node_ids.add(dependent_id)
                        logger.debug('Dependent %s is now ready', dependent_id)
                        
        # Make the sink job
        leaf_rvs: List[Union[WDLBindings, Promise]] = [wdl_id_to_toil_job[node_id].rv() for node_id in leaf_ids]
        # Make sure to also send the section-level bindings
        leaf_rvs.append(environment)
        # And to filter out stuff that should leave scope
        sink = WDLCombineBindingsJob(leaf_rvs, remove=local_environment)
        # It runs inside us
        self.addChild(sink)
        for node_id in leaf_ids:
            # And after all the leaf jobs.
            wdl_id_to_toil_job[node_id].addFollowOn(sink)
            
        return sink
            
class WDLScatterJob(WDLSectionJob):
    """
    Job that evaluates a scatter in a WDL workflow. Runs the body for each
    value in an array, and makes arrays of the new bindings created in each
    instance of the body. If an instance of the body doesn't create a binding,
    it gets a null value in the corresponding array.
    """
    def __init__(self, scatter: WDL.Tree.Scatter, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL scatter.
        """
        super().__init__(**kwargs)
        
        # Because we need to return the return value of the workflow, we need
        # to return a Toil promise for the last/sink job in the workflow's
        # graph. But we can't save either a job that takes promises, or a
        # promise, in ourselves, because of the way that Toil resolves promises
        # at deserialization. So we need to do the actual building-out of the
        # workflow in run().
        
        logger.info("Preparing to run scatter on %s with inputs:", scatter.variable)
        log_bindings(prev_node_results)
        
        self._scatter = scatter
        self._prev_node_results = prev_node_results
    
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Run the scatter.
        """
        
        logger.info("Running scatter on %s", self._scatter.variable)
        
        # Combine the bindings we get from previous jobs.
        # For a task we only see the insode-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)
        
        # Get what to scatter over
        scatter_value = evaluate_named_expression(self._scatter, self._scatter.variable, None, self._scatter.expr, bindings, standard_library)
        
        assert isinstance(scatter_value, WDL.Value.Array)
        
        scatter_jobs = []
        for item in scatter_value.value:
            # Make an instantiation of our subgraph for each possible value of
            # the variable. Make sure the variable is bound only for the
            # duration of the body.
            local_bindings: WDLBindings = WDL.Env.Bindings()
            local_bindings = local_bindings.bind(self._scatter.variable, item)
            scatter_jobs.append(self.create_subgraph(self._scatter.body, bindings, local_bindings))
            
        # Make a job at the end to aggregate
        # Turn all the bindings created inside the scatter bodies into arrays of maybe-optional values
        gather_job = WDLArrayBindingsJob([j.rv() for j in scatter_jobs], bindings)
        self.addChild(gather_job)
        for j in scatter_jobs:
            j.addFollowOn(gather_job)
        return gather_job.rv()
        
class WDLArrayBindingsJob(Job):
    """
    Job that takes all new bindings created in an array of input environments, relative to a base environment, and produces bindings where each new binding name is bound to an array of the values in all the input environments.
    
    Useful for producing the results of a scatter.
    """
    
    def __init__(self, input_bindings: Sequence[Promised[WDLBindings]], base_bindings: WDLBindings, **kwargs: Any) -> None:
        """
        Make a new job to array-ify the given input bindings against the given base bindings.
        """
        super().__init__(**kwargs)
        
        # Everything might still be promises so just save it.
        # TODO: Allow promises in type signature???
        
        self._input_bindings = input_bindings
        self._base_bindings = base_bindings
        
    def run(self, file_sore: AbstractFileStore) -> WDLBindings:
        """
        Actually produce the array-ified bindings now that promised values are available.
        """
        
        # Subtract base bindings to get just the new bindings created in each input
        new_bindings = [env.subtract(self._base_bindings) for env in unwrap_all(self._input_bindings)]
        # Make a set of all the new names.
        # TODO: They ought to maybe have types? Spec just says "any scalar
        # outputs of these tasks is now an array", with no hint on what to do
        # if some tasks output nothing, some tasks output things of
        # incompatible types, etc.
        new_names = {b.name for env in new_bindings for b in env}
        
        result = self._base_bindings
        for name in new_names:
            # Determine the set of all types bound to the name, or None if a result is null.
            # Problem: the WDL type types are not hashable, so we need to do bad N^2 deduplication
            observed_types = []
            for env in new_bindings:
                binding_type = env.resolve(name).type if env.has_binding(name) else None
                if binding_type not in observed_types:
                    observed_types.append(binding_type)
            # Get the supertype of those types
            supertype: WDL.Type.Base = get_supertype(observed_types)
            # Bind an array of the values
            result = result.bind(name, WDL.Value.Array(supertype, [env.resolve(name) if env.has_binding(name) else WDL.Value.Null() for env in new_bindings]))
       
        # Base bindings are already included so return the result
        return result
        
class WDLConditionalJob(WDLSectionJob):
    """
    Job that evaluates a conditional in a WDL workflow.
    """
    def __init__(self, conditional: WDL.Tree.Conditional, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL conditional.
        """
        super().__init__(**kwargs)
        
        # Once again we need to ship the whole body template to be instantiated
        # into Toil jobs only if it will actually run.
        
        logger.info("Preparing to run conditional on %s with inputs:", conditional.expr)
        log_bindings(prev_node_results)
        
        self._conditional = conditional
        self._prev_node_results = prev_node_results
    
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Run the scatter.
        """
        
        logger.info("Checking condition %s", self._conditional.expr)
        
        # Combine the bindings we get from previous jobs.
        # For a task we only see the insode-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)
        
        # Get the expression value. Fake a name.
        expr_value = evaluate_named_expression(self._conditional, "<conditional expression>", WDL.Type.Boolean(), self._conditional.expr, bindings, standard_library)
        
        if expr_value.value:
            # Evaluated to true!
            logger.info('Condition is true')
            # Run the body and return its effects
            body_job = self.create_subgraph(self._conditional.body, bindings)
            return body_job.rv()
        else:
            logger.info('Condition is false')
            # Return the unmodified (but combined) input bindings
            return bindings
        
class WDLWorkflowJob(WDLSectionJob):
    """
    Job that evaluates an entire WDL workflow.
    """
    
    def __init__(self, workflow: WDL.Tree.Workflow, prev_node_results: Sequence[Promised[WDLBindings]], **kwargs: Any) -> None:
        """
        Create a subtree that will run a WDL workflow. The job returns the
        return value of the workflow.
        """
        super().__init__(**kwargs)
        
        # Because we need to return the return value of the workflow, we need
        # to return a Toil promise for the last/sink job in the workflow's
        # graph. But we can't save either a job that takes promises, or a
        # promise, in ourselves, because of the way that Toil resolves promises
        # at deserialization. So we need to do the actual building-out of the
        # workflow in run().
        
        logger.info("Preparing to run workflow %s with inputs:", workflow.name)
        log_bindings(prev_node_results)
        
        self._workflow = workflow
        self._prev_node_results = prev_node_results
        
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Run the workflow. Return the result of the workflow.
        """
        
        logger.info("Running workflow %s", self._workflow.name)
        
        # Combine the bindings we get from previous jobs.
        # For a task we only see the insode-the-task namespace.
        bindings = combine_bindings(unwrap_all(self._prev_node_results))
        # Set up the WDL standard library
        standard_library = ToilWDLStdLibBase(file_store)
        
        if self._workflow.inputs:
            for input_decl in self._workflow.inputs:
                # Evaluate all the inputs that aren't pre-set
                bindings = bindings.bind(input_decl.name, evaluate_defaultable_decl(input_decl, bindings, standard_library))
           
        # Make jobs to run all the parts of the workflow
        sink = self.create_subgraph(self._workflow.body, bindings)
        
        if self._workflow.outputs:
            # Add evaluating the outputs after the sink
            outputs_job = WDLOutputsJob(self._workflow.outputs, sink.rv())
            sink.addFollowOn(outputs_job)
            # Caller takes care of namespacing the result 
            return outputs_job.rv()
        else:
            # No outputs from this workflow.
            return WDL.Env.Bindings()
        
class WDLOutputsJob(Job):
    """
    Job which evaluates an outputs section (such as for a workflow).
    
    Returns an environment with just the outputs bound, in no namespace.
    """
    
    def __init__(self, outputs: List[WDL.Tree.Decl], bindings: Promised[WDLBindings], **kwargs: Any):
        """
        Make a new WDLWorkflowOutputsJob for the given workflow, with the given set of bindings after its body runs.
        """
        super().__init__(**kwargs)
        
        self._outputs = outputs
        self._bindings = bindings
        
    def run(self, file_store: AbstractFileStore) -> WDLBindings:
        """
        Make bindings for the outputs.
        """
        
        # Evaluate all the outputs in the noirmal, non-task-outputs library context
        standard_library = ToilWDLStdLibBase(file_store)
        output_bindings: WDL.Env.Bindings[WDL.Value.Base] = WDL.Env.Bindings()
        for output_decl in self._outputs:
            output_bindings = output_bindings.bind(output_decl.name, evaluate_decl(output_decl, unwrap(self._bindings), standard_library))
        
        return output_bindings
        
class WDLRootJob(WDLSectionJob):
    """
    Job that evaluates an entire WDL workflow, and returns the workflow outputs
    namespaced with the workflow name. Inputs may or may not be namespaced with
    the workflow name; both forms are accepted.
    """
    
    def __init__(self, workflow: WDL.Tree.Workflow, inputs: WDLBindings, **kwargs: Any) -> None:
        """
        Create a subtree to run the workflow and namespace the outputs.
        """
        super().__init__(**kwargs)
        
        self._workflow = workflow
        self._inputs = inputs
        
    def run(self, file_store: AbstractFileStore) -> Promised[WDLBindings]:
        """
        Actually build the subgraph.
        """
        
        # Run the workflow. We rely in this to handle entering the input
        # namespace if needed, or handling free-floating inputs.
        workflow_job = WDLWorkflowJob(self._workflow, [self._inputs])
        self.addChild(workflow_job)
        
        # And namespace its outputs
        namespace_job = WDLNamespaceBindingsJob(self._workflow.name, [workflow_job.rv()])
        workflow_job.addFollowOn(namespace_job)
        
        return namespace_job.rv()
        
def main() -> None:
    """
    A Toil workflow to interpret WDL input files.
    """
    
    parser = argparse.ArgumentParser(description='Runs WDL files with toil.')
    addOptions(parser)
    
    parser.add_argument("wdl_uri", type=str, help="WDL document URI")
    parser.add_argument("inputs_uri", type=str, help="WDL input JSON URI")
    parser.add_argument("--outputDialect", dest="output_dialect", type=str, default='cromwell', choices=['cromwell', 'miniwdl'],
                        help=("JSON output format dialect. 'cromwell' just returns the workflow's output"
                              "values as JSON, while 'miniwld' nests that under an 'outputs' key, and "
                              "includes a 'dir' key where files are written."))
    parser.add_argument("--outputDirectory", dest="output_directory", type=str, default=None,
                        help=("Directory in which to save output files."))
    
    options = parser.parse_args(sys.argv[1:])
    
    # Make sure we have an output directory and we don't need to ever worry
    # about a None, and MyPy knows it.
    output_directory: str = options.output_directory if options.output_directory else tempfile.mkdtemp()
    if not os.path.isdir(output_directory):
        # Make sure it exists
        os.mkdir(output_directory)
    
    
    with Toil(options) as toil:
        if options.restart:
            output_bindings = toil.restart()
        else:
            # Load the WDL document
            document: WDL.Tree.Document = WDL.load(options.wdl_uri)
            
            if document.workflow is None:
                logger.critical("No workflow in document!")
                sys.exit(1)
                
            if document.workflow.inputs:
                # Load the inputs.
                # TODO: Implement URLs
                # TODO: Report good errors
                inputs = json.load(open(options.inputs_uri)) if options.inputs_uri else {}
                # Parse out the available and required inputs. Each key in the
                # JSON ought to start with the workflow's name and then a .
                # TODO: WDL's Bindings[] isn't variant in the right way, so we
                # have to cast from more specific to less specific ones here.
                # The miniwld values_from_json function can evaluate
                # expressions in the inputs or something.
                WDLTypeDeclBindings = WDL.Env.Bindings[Union[WDL.Tree.Decl, WDL.Type.Base]]
                input_bindings = WDL.values_from_json(
                    inputs,
                    cast(WDLTypeDeclBindings, document.workflow.available_inputs),
                    cast(Optional[WDLTypeDeclBindings], document.workflow.required_inputs), 
                    document.workflow.name
                )
            
            # Run the workflow and get its outputs namespaced with the workflow name.
            root_job = WDLRootJob(document.workflow, input_bindings)
            output_bindings = toil.start(root_job)
        assert isinstance(output_bindings, WDL.Env.Bindings)
        
        # Fetch all the output files
        # TODO: deduplicate with _devirtualize_filename
        def devirtualize_output(filename: str) -> str:
            """
            'devirtualize' a file using the "toil" object instead of a filestore.
            Returns its local path.
            """
            if filename.startswith('toilfile:'):
                # This is a reference to the Toil filestore.
                # Deserialize the FileID
                file_id = FileID.unpack(filename[len("toilfile:") :])
                # Figure out where it should go.
                # TODO: make the toilfile URI somehow encode the right file name
                dest_name = os.path.join(output_directory, str(uuid.uuid4()))
                # Export the file
                toil.exportFile(file_id, dest_name)
                # And return where we put it
                return dest_name
            elif filename.startswith('http:') or filename.startswith('https:') or filename.startswith('s3:'):
                # This is a URL that we think Toil knows how to read.
                imported = toil.import_file(filename)
                assert imported is not None
                # Do the same as we do for files we actually made.
                dest_name = os.path.join(output_directory, str(uuid.uuid4()))
                toil.exportFile(file_id, dest_name)
                return dest_name
            else:
                # Not a fancy file
                return filename
                
        # Make all the files local files
        output_bindings = map_over_files_in_bindings(output_bindings, devirtualize_output)
        
        # Report the result in the right format
        outputs = WDL.values_to_json(output_bindings)
        if options.output_dialect == 'miniwdl':
            outputs = {'dir': output_directory, 'outputs': outputs}
        print(json.dumps(outputs))
    
    
    
if __name__ == "__main__":
    main()
     
    



