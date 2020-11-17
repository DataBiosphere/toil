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
import fnmatch
import json
import os
import logging
import re
import textwrap
import csv
import math

import subprocess
import uuid
from typing import (Optional,
                    List,
                    Tuple,
                    Dict,
                    Union,
                    Any)

from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.wdl.wdl_types import WDLPair

wdllogger = logging.getLogger(__name__)


class WDLRuntimeError(Exception):
    """ WDL-related run-time error."""

    def __init__(self, message):
        super(WDLRuntimeError, self).__init__(message)


class WDLJSONEncoder(json.JSONEncoder):
    """
    Extended JSONEncoder to support WDL-specific JSON encoding.
    """

    def default(self, obj):
        if isinstance(obj, WDLPair):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)


def glob(glob_pattern, directoryname):
    '''
    Walks through a directory and its subdirectories looking for files matching
    the glob_pattern and returns a list=[].

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
    wdl_copyright = heredoc_wdl('''        \n
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

    bashfile_string = '#!/bin/bash' + wdl_copyright + bashfile_prefix

    begin_globbing_string = heredoc_wdl('''
        (
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
        chmod -R 777 $tmpDir
        ''')

    bashfile_string = bashfile_string + bashfile_suffix

    with open(os.path.join(temp_dir, job_name + '_script.sh'), 'w') as bashfile:
        bashfile.write(bashfile_string)


def process_single_infile(f, fileStore):
    wdllogger.info('Importing {f} into the jobstore.'.format(f=f))
    if f.startswith('http://') or f.startswith('https://') or \
            f.startswith('file://') or f.startswith('wasb://'):
        filepath = fileStore.importFile(f)
        preserveThisFilename = os.path.basename(f)
    elif f.startswith('s3://'):
        try:
            filepath = fileStore.importFile(f)
            preserveThisFilename = os.path.basename(f)
        except:
            from toil.lib.ec2nodes import EC2Regions
            success = False
            for region in EC2Regions:
                try:
                    html_path = 'http://s3.{}.amazonaws.com/'.format(region) + f[5:]
                    filepath = fileStore.importFile(html_path)
                    preserveThisFilename = os.path.basename(f)
                    success = True
                except:
                    pass
            if not success:
                raise RuntimeError('Unable to import: ' + f)
    elif f.startswith('gs://'):
        f = 'https://storage.googleapis.com/' + f[5:]
        filepath = fileStore.importFile(f)
        preserveThisFilename = os.path.basename(f)
    else:
        filepath = fileStore.importFile("file://" + os.path.abspath(f))
        preserveThisFilename = os.path.basename(f)
    return filepath, preserveThisFilename


def process_array_infile(af, fileStore):
    processed_array = []
    for f in af:
        processed_array.append(process_infile(f, fileStore))
    return processed_array


def process_infile(f, fileStore):
    """
    Takes an array of files or a single file and imports into the jobstore.

    This returns a tuple or an array of tuples replacing all previous path
    strings.  Toil does not preserve a file's original name upon import and
    so the tuple keeps track of this with the format: '(filepath, preserveThisFilename)'

    :param f: String or an Array.  The smallest element must be a string,
              so: an array of strings, an array of arrays of strings... etc.
    :param fileStore: The filestore object that is called to load files into the filestore.
    :return: A tuple or an array of tuples.
    """
    # check if this has already been processed
    if isinstance(f, tuple):
        return f
    elif isinstance(f, list):
        return process_array_infile(f, fileStore)
    elif isinstance(f, str):
        return process_single_infile(f, fileStore)
    else:
        raise RuntimeError('Error processing file: '.format(str(f)))


def parse_value_from_type(in_data: Any,
                          var_type: str,
                          read_in_file: bool = False,
                          file_store: Optional[AbstractFileStore] = None,
                          cwd: Optional[str] = None,
                          temp_dir: Optional[str] = None,
                          docker: Optional[bool] = None
                          ):
    """
    Calls at runtime. This function parses and validates input from its type. File
    import is also handled.

    For values set in a task block, set `read_in_file` to True to process and read
    all encountered files. This requires `cwd`, `temp_dir`, and `docker` to be
    passed into this function.
    """

    def validate(val: bool, msg: str):
        if not val:
            raise WDLRuntimeError(f'Invalid input: {msg}')

    if not in_data:  # optional type?
        # TODO: check if type is in fact an optional.
        return in_data

    if var_type == 'File':
        # in_data can be an array of files
        if read_in_file:
            return process_and_read_file(f=abspath_file(f=in_data, cwd=cwd),
                                         tempDir=temp_dir,
                                         fileStore=file_store,
                                         docker=docker)

        return process_infile(in_data, file_store)

    elif isinstance(in_data, list):
        # if in_data is not an array of files, then handle elements one by one.
        return [parse_value_from_type(i, var_type, read_in_file, file_store, cwd, temp_dir, docker) for i in in_data]

    elif var_type == 'Pair':
        if isinstance(in_data, WDLPair):
            left = in_data.left
            right = in_data.right

        elif isinstance(in_data, dict):
            validate('left' in in_data and 'right' in in_data, f'Pair needs \'left\' and \'right\' keys')
            left = in_data.get('left')
            right = in_data.get('right')
        else:
            validate(isinstance(in_data, tuple) and len(in_data) == 2, 'Only support Pair len == 2')
            left, right = in_data

        return WDLPair(parse_value_from_type(left, var_type.left, read_in_file, file_store, cwd, temp_dir, docker),
                       parse_value_from_type(right, var_type.right, read_in_file, file_store, cwd, temp_dir, docker))

    elif var_type == 'Map':
        validate(isinstance(in_data, dict), f'Expected dict, but got {type(in_data)}')
        return {k:
                parse_value_from_type(v, var_type.value, read_in_file, file_store, cwd, temp_dir, docker)
                for k, v in in_data.items()}

    else:
        # everything else is fine as is.
        return in_data


def sub(input_str: str, pattern: str, replace: str) -> str:
    """
    Given 3 String parameters `input`, `pattern`, `replace`, this function will
    replace any occurrence matching `pattern` in `input` by `replace`.
    `pattern` is expected to be a regular expression. Details of regex evaluation
    will depend on the execution engine running the WDL.

    WDL syntax: String sub(String, String, String)
    """

    if isinstance(input_str, tuple):
        input_str = input_str[1]
    if isinstance(pattern, tuple):
        pattern = pattern[1]
    if isinstance(replace, tuple):
        replace = replace[1]

    return re.sub(pattern=str(pattern), repl=str(replace), string=str(input_str))


def defined(i):
    if i:
        return True
    return False


def process_single_outfile(f, fileStore, workDir, outDir):
    if os.path.exists(f):
        output_f_path = f
    elif os.path.exists(os.path.abspath(f)):
        output_f_path = os.path.abspath(f)
    elif os.path.exists(os.path.join(workDir, 'execution', f)):
        output_f_path = os.path.join(workDir, 'execution', f)
    elif os.path.exists(os.path.join('execution', f)):
        output_f_path = os.path.join('execution', f)
    elif os.path.exists(os.path.join(workDir, f)):
        output_f_path = os.path.join(workDir, f)
    else:
        tmp = subprocess.check_output(['ls', '-lha', workDir]).decode('utf-8')
        exe = subprocess.check_output(['ls', '-lha', os.path.join(workDir, 'execution')]).decode('utf-8')
        raise RuntimeError('OUTPUT FILE: {} was not found!\n'
                           '{}\n\n'
                           '{}\n'.format(f, tmp, exe))
    output_file = fileStore.writeGlobalFile(output_f_path)
    preserveThisFilename = os.path.basename(output_f_path)
    fileStore.exportFile(output_file, "file://" + os.path.join(os.path.abspath(outDir), preserveThisFilename))
    return output_file, preserveThisFilename


def process_array_outfile(af, fileStore, workDir, outDir):
    processed_array = []
    for f in af:
        processed_array.append(process_outfile(f, fileStore, workDir, outDir))
    return processed_array


def process_outfile(f, fileStore, workDir, outDir):
    if isinstance(f, list):
        return process_array_outfile(f, fileStore, workDir, outDir)
    elif isinstance(f, str):
        return process_single_outfile(f, fileStore, workDir, outDir)
    else:
        raise RuntimeError('Error processing file: '.format(str(f)))


def abspath_single_file(f, cwd):
    if f == os.path.abspath(f):
        return f
    else:
        return os.path.join(cwd, f)


def abspath_array_file(af, cwd):
    processed_array = []
    for f in af:
        processed_array.append(abspath_file(f, cwd))
    return processed_array


def abspath_file(f, cwd):
    if not f:
        # in the case of "optional" files (same treatment in 'process_and_read_file()')
        # TODO: handle this at compile time, not here
        return ''
    # check if this has already been processed
    if isinstance(f, tuple):
        return f
    if isinstance(f, list):
        return abspath_array_file(f, cwd)
    elif isinstance(f, str):
        if f.startswith('s3://') or f.startswith('http://') or f.startswith('https://') or \
                f.startswith('file://') or f.startswith('wasb://') or f.startswith('gs://'):
            return f
        return abspath_single_file(f, cwd)
    else:
        raise RuntimeError('Error processing file: ({}) of type: ({}).'.format(str(f), str(type(f))))


def read_single_file(f, tempDir, fileStore, docker=False):
    import os
    try:
        fpath = fileStore.readGlobalFile(f[0], userPath=os.path.join(tempDir, f[1]))
    except:
        fpath = os.path.join(tempDir, f[1])
    return fpath


def read_array_file(af, tempDir, fileStore, docker=False):
    processed_array = []
    for f in af:
        processed_array.append(read_file(f, tempDir, fileStore, docker=docker))
    return processed_array


def read_file(f, tempDir, fileStore, docker=False):
    # check if this has already been processed
    if isinstance(f, tuple):
        return read_single_file(f, tempDir, fileStore, docker=docker)
    elif isinstance(f, list):
        return read_array_file(f, tempDir, fileStore, docker=docker)
    else:
        raise RuntimeError('Error processing file: {}'.format(str(f)))


def process_and_read_file(f, tempDir, fileStore, docker=False):
    if not f:
        # in the case of "optional" files (same treatment in 'abspath_file()')
        # TODO: handle this at compile time, not here and change to the empty string
        return None
    processed_file = process_infile(f, fileStore)
    return read_file(processed_file, tempDir, fileStore, docker=docker)


def generate_stdout_file(output, tempDir, fileStore, stderr=False):
    """
    Create a stdout (or stderr) file from a string or bytes object.

    :param str|bytes output: A str or bytes object that holds the stdout/stderr text.
    :param str tempDir: The directory to write the stdout file.
    :param fileStore: A fileStore object.
    :param bool stderr: If True, a stderr instead of a stdout file is generated.
    :return: The file path to the generated file.
    """
    if output is None:
        # write an empty file if there's no stdout/stderr.
        output = b''
    elif isinstance(output, str):
        output = bytes(output, encoding='utf-8')

    # TODO: we need a way to differentiate the stdout/stderr files in the workflow after execution.
    # Cromwell generates a folder for each task so the file is simply named stdout and lives in
    # the task execution folder. This is not the case with Toil. Though, this would not be a
    # problem with intermediate stdout files as each task has its own temp folder.
    name = 'stderr' if stderr else 'stdout'
    local_path = os.path.join(tempDir, 'execution', name)

    # import to fileStore then read to local temp file
    with fileStore.writeGlobalFileStream(cleanup=True, basename=name) as (stream, file_id):
        stream.write(output)

    assert file_id is not None
    return fileStore.readGlobalFile(fileStoreID=file_id, userPath=local_path)


def return_bytes(unit='B'):
    num_bytes = 1
    if unit.lower() in ['ki', 'kib']:
        num_bytes = 1 << 10
    if unit.lower() in ['mi', 'mib']:
        num_bytes = 1 << 20
    if unit.lower() in ['gi', 'gib']:
        num_bytes = 1 << 30
    if unit.lower() in ['ti', 'tib']:
        num_bytes = 1 << 40

    if unit.lower() in ['k', 'kb']:
        num_bytes = 1000
    if unit.lower() in ['m', 'mb']:
        num_bytes = 1000 ** 2
    if unit.lower() in ['g', 'gb']:
        num_bytes = 1000 ** 3
    if unit.lower() in ['t', 'tb']:
        num_bytes = 1000 ** 4
    return num_bytes


def parse_memory(memory):
    """
    Parses a string representing memory and returns
    an integer # of bytes.

    :param memory:
    :return:
    """
    memory = str(memory)
    if 'None' in memory:
        return 2147483648  # toil's default
    try:
        import re
        raw_mem_split = re.split('([a-zA-Z]+)', memory)
        mem_split = []

        for r in raw_mem_split:
            if r:
                mem_split.append(r.replace(' ', ''))

        if len(mem_split) == 1:
            return int(memory)

        if len(mem_split) == 2:
            num = mem_split[0]
            unit = mem_split[1]
            return int(float(num) * return_bytes(unit))
        else:
            raise RuntimeError('Memory parsing failed: {}'.format(memory))
    except:
        return 2147483648  # toil's default


def parse_cores(cores):
    cores = str(cores)
    if 'None' in cores:
        return 1  # toil's default
    if cores:
        return float(cores)
    else:
        return 1


def parse_disk(disk):
    disk = str(disk)
    if 'None' in disk:
        return 2147483648  # toil's default
    try:
        total_disk = 0
        disks = disk.split(',')
        for d in disks:
            d = d.strip().split(' ')
            if len(d) > 1:
                for part in d:
                    if is_number(part):
                        total_disk += parse_memory('{} GB'.format(part))
            else:
                return parse_memory(d[0]) if parse_memory(d[0]) > 2147483648 else 2147483648
        return total_disk if total_disk > 2147483648 else 2147483648
    except:
        return 2147483648  # toil's default


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def size(f: Optional[Union[tuple, List[tuple]]] = None,
         unit: Optional[str] = 'B',
         fileStore: Optional[AbstractFileStore] = None) -> float:
    """
    Given a `File` and a `String` (optional), returns the size of the file in Bytes
    or in the unit specified by the second argument.

    Supported units are KiloByte ("K", "KB"), MegaByte ("M", "MB"), GigaByte
    ("G", "GB"), TeraByte ("T", "TB") (powers of 1000) as well as their binary version
    (https://en.wikipedia.org/wiki/Binary_prefix) "Ki" ("KiB"), "Mi" ("MiB"),
    "Gi" ("GiB"), "Ti" ("TiB") (powers of 1024). Default unit is Bytes ("B").

    WDL syntax: Float size(File, [String])
    Varieties:  Float size(File?, [String])
                Float size(Array[File], [String])
                Float size(Array[File?], [String])
    """

    if f is None:
        return 0

    assert isinstance(f, (str, tuple, list)), f'size() excepts a "File" or "File?" argument!  Not: {type(f)}'

    # validate the input. fileStore is only required if the input is not processed.
    f = process_infile(f, fileStore)

    divisor = return_bytes(unit)

    if isinstance(f, list):
        total_size = sum(file[0].size for file in f)
        return total_size / divisor

    fileID = f[0]
    return fileID.size / divisor


def select_first(values):
    for var in values:
        if var:
            return var
    raise ValueError('No defined variables found for select_first array: {}'.format(str(values)))


def combine_dicts(dict1, dict2):
    from six import iteritems
    combineddict= {}
    for k, v in iteritems(dict1):
        counter1 = 0
        while isinstance(v, list):
            counter1 += 1
            v = v[0]
        break

    for k, v in iteritems(dict2):
        counter2 = 0
        while isinstance(v, list):
            counter2 += 1
            v = v[0]
        break

    for k in dict1:
        if counter1 > counter2:
            combineddict[k] = dict1[k]
            combineddict[k].append(dict2[k])
        elif counter1 < counter2:
            combineddict[k] = dict2[k]
            combineddict[k].append(dict1[k])
        else:
            combineddict[k] = [dict1[k], dict2[k]]
    return combineddict


def basename(path, suffix=None):
    """https://software.broadinstitute.org/wdl/documentation/article?id=10554"""
    path = path.strip()
    if suffix:
        suffix = suffix.strip()
        if path.endswith(suffix):
            path = path[:-len(suffix)]
    return os.path.basename(path)


def heredoc_wdl(template, dictionary={}, indent=''):
    template = textwrap.dedent(template).format(**dictionary)
    return template.replace('\n', '\n' + indent) + '\n'


def floor(i: Union[int, float]) -> int:
    """
    Converts a Float value into an Int by rounding down to the next lower integer.
    """
    return math.floor(i)


def ceil(i: Union[int, float]) -> int:
    """
    Converts a Float value into an Int by rounding up to the next higher integer.
    """
    return math.ceil(i)


def read_lines(path: str) -> List[str]:
    """
    Given a file-like object (`String`, `File`) as a parameter, this will read each
    line as a string and return an `Array[String]` representation of the lines in
    the file.

    WDL syntax: Array[String] read_lines(String|File)
    """
    # file should already be imported locally via `process_and_read_file`
    with open(path, "r") as f:
        return f.read().rstrip('\n').split('\n')


def read_tsv(path: str, delimiter: str = '\t') -> List[List[str]]:
    """
    Take a tsv filepath and return an array; e.g. [[],[],[]].

    For example, a file containing:

    1   2   3
    4   5   6
    7   8   9

    would return the array: [['1','2','3'], ['4','5','6'], ['7','8','9']]

    WDL syntax: Array[Array[String]] read_tsv(String|File)
    """
    tsv_array = []
    with open(path, 'r') as f:
        data_file = csv.reader(f, delimiter=delimiter)
        for line in data_file:
            tsv_array.append(line)
    return tsv_array


def read_csv(path: str) -> List[List[str]]:
    """
    Take a csv filepath and return an array; e.g. [[],[],[]].

    For example, a file containing:

    1,2,3
    4,5,6
    7,8,9

    would return the array: [['1','2','3'], ['4','5','6'], ['7','8','9']]
    """
    return read_tsv(path, delimiter=",")


def read_json(path: str) -> Any:
    """
    The `read_json()` function takes one parameter, which is a file-like object
     (`String`, `File`) and returns a data type which matches the data
     structure in the JSON file. See
    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#mixed-read_jsonstringfile

    WDL syntax: mixed read_json(String|File)
    """
    with open(path, "r") as f:
        return json.load(f)


def read_map(path: str) -> Dict[str, str]:
    """
    Given a file-like object (`String`, `File`) as a parameter, this will read each
    line from a file and expect the line to have the format `col1\tcol2`. In other
    words, the file-like object must be a two-column TSV file.

    WDL syntax: Map[String, String] read_map(String|File)
    """
    d = dict()
    with open(path, "r") as f:
        for line in f:
            line = line.rstrip()
            if not line:
                # remove extra lines
                continue
            key, value = line.split('\t', 1)
            d[key] = value.strip()
    return d


def read_int(path: Union[str, tuple]) -> int:
    """
    The `read_int()` function takes a file path which is expected to contain 1
    line with 1 integer on it. This function returns that integer.

    WDL syntax: Int read_int(String|File)
    """
    if isinstance(path, tuple):
        path = path[0]

    with open(path, 'r') as f:
        return int(f.read().strip())


def read_string(path: Union[str, tuple]) -> str:
    """
    The `read_string()` function takes a file path which is expected to contain 1
    line with 1 string on it. This function returns that string.

    WDL syntax: String read_string(String|File)
    """
    if isinstance(path, tuple):
        path = path[0]

    with open(path, 'r') as f:
        return str(f.read().strip())


def read_float(path: Union[str, tuple]) -> float:
    """
    The `read_float()` function takes a file path which is expected to contain 1
    line with 1 floating point number on it. This function returns that float.

    WDL syntax: Float read_float(String|File)
    """
    if isinstance(path, tuple):
        path = path[0]

    with open(path, 'r') as f:
        return float(f.read().strip())


def read_boolean(path: Union[str, tuple]) -> bool:
    """
    The `read_boolean()` function takes a file path which is expected to contain 1
    line with 1 Boolean value (either "true" or "false" on it). This function
    returns that Boolean value.

    WDL syntax: Boolean read_boolean(String|File)
    """
    if isinstance(path, tuple):
        path = path[0]

    with open(path, 'r') as f:
        return f.read().strip().lower() == 'true'


def _get_temp_file_path(function_name: str, temp_dir: Optional[str] = None) -> str:
    """
    Get a unique path with basename in the format of "{function_name}_{UUID}.tmp".
    """

    if not temp_dir:
        temp_dir = os.getcwd()

    # Cromwell uses the MD5 checksum of the content as part of the file name. We use a UUID instead
    # for now, since we're writing line by line via a context manager.
    # md5sum = hashlib.md5(content).hexdigest()
    # name = f'{function_name}_{md5sum}.tmp'

    name = f'{function_name}_{uuid.uuid4()}.tmp'
    return os.path.join(temp_dir, 'execution', name)


def write_lines(in_lines: List[str],
                temp_dir: Optional[str] = None,
                file_store: Optional[AbstractFileStore] = None) -> str:
    """
    Given something that's compatible with `Array[String]`, this writes each element
    to it's own line on a file.  with newline `\n` characters as line separators.

    WDL syntax: File write_lines(Array[String])
    """
    assert isinstance(in_lines, list), f'write_lines() requires "{in_lines}" to be a list!  Not: {type(in_lines)}'

    path = _get_temp_file_path('write_lines', temp_dir)

    with open(path, 'w') as file:
        for line in in_lines:
            file.write(f'{line}\n')

    if file_store:
        file_store.writeGlobalFile(path, cleanup=True)

    return path


def write_tsv(in_tsv: List[List[str]],
              delimiter: str = '\t',
              temp_dir: Optional[str] = None,
              file_store: Optional[AbstractFileStore] = None) -> str:
    """
    Given something that's compatible with `Array[Array[String]]`, this writes a TSV
    file of the data structure.

    WDL syntax: File write_tsv(Array[Array[String]])
    """
    assert isinstance(in_tsv, list), f'write_tsv() requires "{in_tsv}" to be a list!  Not: {type(in_tsv)}'

    path = _get_temp_file_path('write_tsv', temp_dir)

    with open(path, 'w') as file:
        tsv_writer = csv.writer(file, delimiter=delimiter)
        for row in in_tsv:
            tsv_writer.writerow(row)

    if file_store:
        file_store.writeGlobalFile(path, cleanup=True)

    return path


def write_json(in_json: Any,
               indent: Union[None, int, str] = None,
               separators: Optional[Tuple[str, str]] = (',', ':'),
               temp_dir: Optional[str] = None,
               file_store: Optional[AbstractFileStore] = None) -> str:
    """
    Given something with any type, this writes the JSON equivalent to a file. See
    the table in the definition of
    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#mixed-read_jsonstringfile

    WDL syntax: File write_json(mixed)
    """

    path = _get_temp_file_path('write_json', temp_dir)

    with open(path, 'w') as file:
        file.write(json.dumps(in_json, indent=indent, separators=separators, cls=WDLJSONEncoder))

    if file_store:
        file_store.writeGlobalFile(path, cleanup=True)

    return path


def write_map(in_map: Dict[str, str],
              temp_dir: Optional[str] = None,
              file_store: Optional[AbstractFileStore] = None) -> str:
    """
    Given something that's compatible with `Map[String, String]`, this writes a TSV
     file of the data structure.

    WDL syntax: File write_map(Map[String, String])
    """
    assert isinstance(in_map, dict), f'write_map() requires "{in_map}" to be a dict!  Not: {type(in_map)}'

    path = _get_temp_file_path('write_map', temp_dir)

    with open(path, 'w') as file:
        for key, val in in_map.items():
            file.write(f'{key}\t{val}\n')

    if file_store:
        file_store.writeGlobalFile(path, cleanup=True)

    return path


def wdl_range(num: int) -> List[int]:
    """
    Given an integer argument, the range function creates an array of integers of
    length equal to the given argument.

    WDL syntax: Array[Int] range(Int)
    """
    if not (isinstance(num, int) and num >= 0):
        raise WDLRuntimeError(f'range() requires an integer greater than or equal to 0 (but got {num})')

    return list(range(num))


def transpose(in_array: List[List[Any]]) -> List[List[Any]]:
    """
    Given a two dimensional array argument, the transpose function transposes the
    two dimensional array according to the standard matrix transpose rules.

    WDL syntax: Array[Array[X]] transpose(Array[Array[X]])
    """
    assert isinstance(in_array, list), f'transpose() requires "{in_array}" to be a list!  Not: {type(in_array)}'

    for arr in in_array:
        assert isinstance(arr, list), f'transpose() requires all collections to be a list!  Not: {type(arr)}'
        # zip() can handle this but Cromwell can not.
        assert len(arr) == len(in_array[0]), 'transpose() requires all collections have the same size!'

    return [list(i) for i in zip(*in_array)]


def length(in_array: List[Any]) -> int:
    """
    Given an Array, the `length` function returns the number of elements in the Array
    as an Int.
    """
    if not isinstance(in_array, list):
        # Cromwell throws an exception for anything other than a WDL Array
        raise WDLRuntimeError(f'length() requires ${in_array} to be a list!  Not: {type(in_array)}')

    return len(in_array)
