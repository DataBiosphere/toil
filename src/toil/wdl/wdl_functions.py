# Copyright (C) 2015-2021 Regents of the University of California
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
import csv
import json
import logging
import math
import os
import re
import subprocess
import textwrap
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

from toil.fileStores.abstractFileStore import AbstractFileStore
from toil.lib.conversions import bytes_in_unit
from toil.lib.resources import glob  # type: ignore
from toil.wdl.wdl_types import WDLFile, WDLPair

logger = logging.getLogger(__name__)


class WDLRuntimeError(Exception):
    """ WDL-related run-time error."""

    def __init__(self, message):
        super().__init__(message)


class WDLJSONEncoder(json.JSONEncoder):
    """
    Extended JSONEncoder to support WDL-specific JSON encoding.
    """

    def default(self, obj):
        if isinstance(obj, WDLPair):
            return obj.to_dict()
        return json.JSONEncoder.default(self, obj)


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


def process_single_infile(wdl_file: WDLFile, fileStore: AbstractFileStore) -> WDLFile:
    f = wdl_file.file_path
    logger.info(f'Importing {f} into the jobstore.')
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
                    html_path = f'http://s3.{region}.amazonaws.com/' + f[5:]
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
    return WDLFile(file_path=filepath, file_name=preserveThisFilename, imported=True)


def process_infile(f: Any, fileStore: AbstractFileStore):
    """
    Takes any input and imports the WDLFile into the fileStore.

    This returns the input importing all WDLFile instances to the fileStore.  Toil
    does not preserve a file's original name upon import and so the WDLFile also keeps
    track of this.

    :param f: A primitive, WDLFile, or a container. A file needs to be a WDLFile instance
              to be imported.
    :param fileStore: The fileStore object that is called to load files into the fileStore.
    """
    if isinstance(f, WDLFile):
        # check if this has already been imported into the fileStore
        if f.imported:
            return f
        else:
            return process_single_infile(f, fileStore)
    elif isinstance(f, list):
        # recursively call process_infile() to handle cases like Array[Map[String, File]]
        return [process_infile(sf, fileStore) for sf in f]
    elif isinstance(f, WDLPair):
        f.left = process_infile(f.left, fileStore)
        f.right = process_infile(f.right, fileStore)
        return f
    elif isinstance(f, dict):
        return {process_infile(k, fileStore): process_infile(v, fileStore) for k, v in f.items()}
    elif isinstance(f, (int, str, bool, float)):
        return f
    else:
        raise WDLRuntimeError(f'Error processing file: {str(f)}')


def sub(input_str: str, pattern: str, replace: str) -> str:
    """
    Given 3 String parameters `input`, `pattern`, `replace`, this function will
    replace any occurrence matching `pattern` in `input` by `replace`.
    `pattern` is expected to be a regular expression. Details of regex evaluation
    will depend on the execution engine running the WDL.

    WDL syntax: String sub(String, String, String)
    """

    if isinstance(input_str, WDLFile):
        input_str = input_str.file_name
    if isinstance(pattern, WDLFile):
        pattern = pattern.file_name
    if isinstance(replace, WDLFile):
        replace = replace.file_name

    return re.sub(pattern=str(pattern), repl=str(replace), string=str(input_str))


def defined(i):
    if i:
        return True
    return False


def process_single_outfile(wdl_file: WDLFile, fileStore, workDir, outDir) -> WDLFile:
    f = wdl_file.file_path
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
    elif os.path.exists(os.path.join(outDir, f)):
        output_f_path = os.path.join(outDir, f)
    else:
        tmp = subprocess.check_output(['ls', '-lha', workDir]).decode('utf-8')
        exe = subprocess.check_output(['ls', '-lha', os.path.join(workDir, 'execution')]).decode('utf-8')
        for std_file in ('stdout', 'stderr'):
            std_file = os.path.join(workDir, 'execution', std_file)
            if os.path.exists(std_file):
                with open(std_file, 'rb') as f:
                    logger.info(f.read())

        raise RuntimeError('OUTPUT FILE: {} was not found in {}!\n'
                           '{}\n\n'
                           '{}\n'.format(f, os.getcwd(), tmp, exe))
    output_file = fileStore.writeGlobalFile(output_f_path)
    preserveThisFilename = os.path.basename(output_f_path)
    fileStore.export_file(output_file, "file://" + os.path.join(os.path.abspath(outDir), preserveThisFilename))
    return WDLFile(file_path=output_file, file_name=preserveThisFilename, imported=True)


def process_outfile(f, fileStore, workDir, outDir):
    if isinstance(f, WDLFile):
        return process_single_outfile(f, fileStore, workDir, outDir)
    elif isinstance(f, list):
        # recursively call process_outfile() to handle cases like Array[Map[String, File]]
        return [process_outfile(sf, fileStore, workDir, outDir) for sf in f]
    elif isinstance(f, WDLPair):
        f.left = process_outfile(f.left, fileStore, workDir, outDir)
        f.right = process_outfile(f.right, fileStore, workDir, outDir)
        return f
    elif isinstance(f, dict):
        return {process_outfile(k, fileStore, workDir, outDir):
                process_outfile(v, fileStore, workDir, outDir) for k, v in f.items()}
    elif isinstance(f, (int, str, bool, float)):
        return f
    else:
        raise WDLRuntimeError(f'Error processing file: {str(f)}')


def abspath_single_file(f: WDLFile, cwd: str) -> WDLFile:
    path = f.file_path
    if path != os.path.abspath(path):
        f.file_path = os.path.join(cwd, path)
    return f


def abspath_file(f: Any, cwd: str):
    if not f:
        # in the case of "optional" files (same treatment in 'process_and_read_file()')
        # TODO: handle this at compile time, not here
        return ''
    if isinstance(f, WDLFile):
        # check if this has already been imported into the fileStore
        if f.imported:
            return f
        path = f.file_path
        if path.startswith('s3://') or path.startswith('http://') or path.startswith('https://') or \
                path.startswith('file://') or path.startswith('wasb://') or path.startswith('gs://'):
            return f
        return abspath_single_file(f, cwd)
    elif isinstance(f, list):
        # recursively call abspath_file() to handle cases like Array[Map[String, File]]
        return [abspath_file(sf, cwd) for sf in f]
    elif isinstance(f, WDLPair):
        f.left = abspath_file(f.left, cwd)
        f.right = abspath_file(f.right, cwd)
        return f
    elif isinstance(f, dict):
        return {abspath_file(k, cwd): abspath_file(v, cwd) for k, v in f.items()}
    elif isinstance(f, (int, str, bool, float)):
        return f
    else:
        raise WDLRuntimeError(f'Error processing file: ({str(f)}) of type: ({str(type(f))}).')


def read_single_file(f: WDLFile, tempDir, fileStore, docker=False) -> str:
    import os
    try:
        fpath = fileStore.readGlobalFile(f.file_path, userPath=os.path.join(tempDir, f.file_name))
    except:
        fpath = os.path.join(tempDir, f.file_name)
    return fpath


def read_file(f: Any, tempDir: str, fileStore: AbstractFileStore, docker: bool = False):
    if isinstance(f, WDLFile):
        return read_single_file(f, tempDir, fileStore, docker=docker)
    elif isinstance(f, list):
        # recursively call read_file() to handle cases like Array[Map[String, File]]
        return [read_file(sf, tempDir, fileStore, docker=docker) for sf in f]
    elif isinstance(f, WDLPair):
        f.left = read_file(f.left, tempDir, fileStore, docker=docker)
        f.right = read_file(f.right, tempDir, fileStore, docker=docker)
        return f
    elif isinstance(f, dict):
        return {read_file(k, tempDir, fileStore, docker=docker):
                read_file(v, tempDir, fileStore, docker=docker) for k, v in f.items()}
    elif isinstance(f, (int, str, bool, float)):
        return f
    else:
        raise WDLRuntimeError(f'Error processing file: {str(f)}')


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
            return int(float(num) * bytes_in_unit(unit))
        else:
            raise RuntimeError(f'Memory parsing failed: {memory}')
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
                        total_disk += parse_memory(f'{part} GB')
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


def size(f: Optional[Union[str, WDLFile, List[Union[str, WDLFile]]]] = None,
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

    # it is possible that size() is called directly (e.g.: size('file')) and so it is not treated as a file.
    if isinstance(f, str):
        f = WDLFile(file_path=f)
    elif isinstance(f, list):
        f = [WDLFile(file_path=sf) if isinstance(sf, str) else sf for sf in f]

    assert isinstance(f, (WDLFile, list)), f'size() excepts a "File" or "File?" argument!  Not: {type(f)}'

    # validate the input. fileStore is only required if the input is not processed.
    f = process_infile(f, fileStore)

    divisor = bytes_in_unit(unit)

    if isinstance(f, list):
        total_size = sum(file.file_path.size for file in f)
        return total_size / divisor

    fileID = f.file_path
    return fileID.size / divisor


def select_first(values):
    for var in values:
        if var:
            return var
    raise ValueError(f'No defined variables found for select_first array: {str(values)}')


def combine_dicts(dict1, dict2):
    combineddict= {}
    for k, v in dict1.items():
        counter1 = 0
        while isinstance(v, list):
            counter1 += 1
            v = v[0]
        break

    for k, v in dict2.items():
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
    with open(path) as f:
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
    with open(path) as f:
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
    with open(path) as f:
        return json.load(f)


def read_map(path: str) -> Dict[str, str]:
    """
    Given a file-like object (`String`, `File`) as a parameter, this will read each
    line from a file and expect the line to have the format `col1\tcol2`. In other
    words, the file-like object must be a two-column TSV file.

    WDL syntax: Map[String, String] read_map(String|File)
    """
    d = dict()
    with open(path) as f:
        for line in f:
            line = line.rstrip()
            if not line:
                # remove extra lines
                continue
            key, value = line.split('\t', 1)
            d[key] = value.strip()
    return d


def read_int(path: Union[str, WDLFile]) -> int:
    """
    The `read_int()` function takes a file path which is expected to contain 1
    line with 1 integer on it. This function returns that integer.

    WDL syntax: Int read_int(String|File)
    """
    if isinstance(path, WDLFile):
        path = path.file_path

    with open(path) as f:
        return int(f.read().strip())


def read_string(path: Union[str, WDLFile]) -> str:
    """
    The `read_string()` function takes a file path which is expected to contain 1
    line with 1 string on it. This function returns that string.

    WDL syntax: String read_string(String|File)
    """
    if isinstance(path, WDLFile):
        path = path.file_path

    with open(path) as f:
        return str(f.read().strip())


def read_float(path: Union[str, WDLFile]) -> float:
    """
    The `read_float()` function takes a file path which is expected to contain 1
    line with 1 floating point number on it. This function returns that float.

    WDL syntax: Float read_float(String|File)
    """
    if isinstance(path, WDLFile):
        path = path.file_path

    with open(path) as f:
        return float(f.read().strip())


def read_boolean(path: Union[str, WDLFile]) -> bool:
    """
    The `read_boolean()` function takes a file path which is expected to contain 1
    line with 1 Boolean value (either "true" or "false" on it). This function
    returns that Boolean value.

    WDL syntax: Boolean read_boolean(String|File)
    """
    if isinstance(path, WDLFile):
        path = path.file_path

    with open(path) as f:
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


def wdl_zip(left: List[Any], right: List[Any]) -> List[WDLPair]:
    """
    Return the dot product of the two arrays. If the arrays have different lengths
    it is an error.

    WDL syntax: Array[Pair[X,Y]] zip(Array[X], Array[Y])
    """
    if not isinstance(left, list) or not isinstance(right, list):
        raise WDLRuntimeError(f'zip() requires both inputs to be lists!  Not: {type(left)} and {type(right)}')

    if len(left) != len(right):
        raise WDLRuntimeError('zip() requires that input values have the same size!')

    return list(WDLPair(left=left_val, right=right_val) for left_val, right_val in zip(left, right))


def cross(left: List[Any], right: List[Any]) -> List[WDLPair]:
    """
    Return the cross product of the two arrays. Array[Y][1] appears before
    Array[X][1] in the output.

    WDL syntax: Array[Pair[X,Y]] cross(Array[X], Array[Y])
    """
    if not isinstance(left, list) or not isinstance(right, list):
        raise WDLRuntimeError(f'cross() requires both inputs to be Array[]!  Not: {type(left)} and {type(right)}')

    return list(WDLPair(left=left_val, right=right_val) for left_val in left for right_val in right)


def as_pairs(in_map: dict) -> List[WDLPair]:
    """
    Given a Map, the `as_pairs` function returns an Array containing each element
    in the form of a Pair. The key will be the left element of the Pair and the
    value the right element. The order of the the Pairs in the resulting Array
    is the same as the order of the key/value pairs in the Map.

    WDL syntax: Array[Pair[X,Y]] as_pairs(Map[X,Y])
    """
    if not isinstance(in_map, dict):
        raise WDLRuntimeError(f'as_pairs() requires "{in_map}" to be Map[]!  Not: {type(in_map)}')

    return list(WDLPair(left=k, right=v) for k, v in in_map.items())


def as_map(in_array: List[WDLPair]) -> dict:
    """
    Given an Array consisting of Pairs, the `as_map` function returns a Map in
    which the left elements of the Pairs are the keys and the right elements the
    values.

    WDL syntax: Map[X,Y] as_map(Array[Pair[X,Y]])
    """
    if not isinstance(in_array, list):
        raise WDLRuntimeError(f'as_map() requires "{in_array}" to be a list!  Not: {type(in_array)}')

    map = {}

    for pair in in_array:
        if map.get(pair.left):
            raise WDLRuntimeError('Cannot evaluate "as_map()" with duplicated keys.')

        map[pair.left] = pair.right

    return map


def keys(in_map: dict) -> list:
    """
    Given a Map, the `keys` function returns an Array consisting of the keys in
    the Map. The order of the keys in the resulting Array is the same as the
    order of the Pairs in the Map.

    WDL syntax: Array[X] keys(Map[X,Y])
    """

    return list(in_map.keys())


def collect_by_key(in_array: List[WDLPair]) -> dict:
    """
    Given an Array consisting of Pairs, the `collect_by_key` function returns a Map
    in which the left elements of the Pairs are the keys and the right elements the
    values.

    WDL syntax: Map[X,Array[Y]] collect_by_key(Array[Pair[X,Y]])
    """
    if not isinstance(in_array, list):
        raise WDLRuntimeError(f'as_map() requires "{in_array}" to be a list!  Not: {type(in_array)}')

    map = {}

    for pair in in_array:
        map.setdefault(pair.left, []).append(pair.right)

    return map


def flatten(in_array: List[list]) -> list:
    """
    Given an array of arrays, the `flatten` function concatenates all the member
    arrays in the order to appearance to give the result. It does not deduplicate
    the elements.

    WDL syntax: Array[X] flatten(Array[Array[X]])
    """
    assert isinstance(in_array, list), f'flatten() requires "{in_array}" to be a list!  Not: {type(in_array)}'

    arr = []

    for element in in_array:
        assert isinstance(element, list), f'flatten() requires all collections to be a list!  Not: {type(element)}'
        arr.extend(element)

    return arr
