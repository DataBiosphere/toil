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
from past.builtins import basestring

import fnmatch
import os
import logging
import textwrap
import csv

import subprocess

wdllogger = logging.getLogger(__name__)



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
    return (filepath, preserveThisFilename)


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
    elif isinstance(f, basestring):
        return process_single_infile(f, fileStore)
    else:
        raise RuntimeError('Error processing file: '.format(str(f)))


def sub(a, b, c):
    if isinstance(a, tuple):
        a = a[1]
    if isinstance(a, tuple):
        b = b[1]
    if isinstance(a, tuple):
        c = c[1]
    import re
    return re.sub(str(a), str(b), str(c))


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
    return (output_file, preserveThisFilename)


def process_array_outfile(af, fileStore, workDir, outDir):
    processed_array = []
    for f in af:
        processed_array.append(process_outfile(f, fileStore, workDir, outDir))
    return processed_array


def process_outfile(f, fileStore, workDir, outDir):
    if isinstance(f, list):
        return process_array_outfile(f, fileStore, workDir, outDir)
    elif isinstance(f, basestring):
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
    elif isinstance(f, basestring):
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
        raise RuntimeError('Error processing file: '.format(str(f)))


def process_and_read_file(f, tempDir, fileStore, docker=False):
    if not f:
        # in the case of "optional" files (same treatment in 'abspath_file()')
        # TODO: handle this at compile time, not here and change to the empty string
        return None
    processed_file = process_infile(f, fileStore)
    return read_file(processed_file, tempDir, fileStore, docker=docker)


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
        return 2147483648 # toil's default
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
        return 2147483648 # toil's default


def parse_cores(cores):
    cores = str(cores)
    if 'None' in cores:
        return 1 # toil's default
    if cores:
        return float(cores)
    else:
        return 1


def parse_disk(disk):
    disk = str(disk)
    if 'None' in disk:
        return 2147483648 # toil's default
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


def size(f, unit='B', fileStore=None):
    """
    Returns the size of a file in bytes.

    :param f: Filename
    :param unit: Return the byte size in these units (gigabytes, etc.).
    :return:
    """
    divisor = return_bytes(unit)
    fileID = process_infile(f, fileStore)[0]
    return fileID.size / divisor


def select_first(values):
    for var in values:
        if var:
            return var
    raise ValueError('No defined variables found for select_first array: {}'.format(str(values)))


def read_string(inputstring):
    if isinstance(inputstring, tuple):
        inputstring = inputstring[0]
    return str(inputstring)


def read_float(inputstring):
    if isinstance(inputstring, tuple):
        inputstring = inputstring[0]
    return float(inputstring)


def read_int(inputstring):
    if isinstance(inputstring, tuple):
        inputstring = inputstring[0]
    return int(inputstring)


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


def read_tsv(f, delimiter="\t"):
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
    with open(f, "r") as f:
        data_file = csv.reader(f, delimiter=delimiter)
        for line in data_file:
            tsv_array.append(line)
    return (tsv_array)


def read_csv(f):
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
    return read_tsv(f, delimiter=",")

