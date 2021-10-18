# Copyright (C) 2018-2021 UCSC Computational Genomics Lab
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
import logging
import os
import subprocess
import sys

from toil.wdl.utils import dict_from_JSON, get_analyzer, write_mappings
from toil.wdl.wdl_synthesis import SynthesizeWDL

logger = logging.getLogger(__name__)


def main():
    """
    A program to run WDL input files using native Toil scripts.

    Calls two files, described below, wdl_analysis.py and wdl_synthesis.py:

    wdl_analysis reads the wdl and restructures them into 2 intermediate data
    structures before writing (python dictionaries):
        "wf_dictionary": containing the parsed workflow information.
        "tasks_dictionary": containing the parsed task information.

    wdl_synthesis takes the "wf_dictionary", "tasks_dictionary", and the JSON file
    and uses them to write a native python script for use with Toil.

    Requires a WDL file, and a JSON file.  The WDL file contains ordered commands,
    and the JSON file contains input values for those commands.  To run in Toil,
    these two files must be parsed, restructured into python dictionaries, and
    then compiled into a Toil formatted python script.  This compiled Toil script
    is deleted unless the user specifies: "--dev_mode" as an option.

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
    """
    parser = argparse.ArgumentParser(description='Runs WDL files with toil.')
    parser.add_argument('wdl_file', help='A WDL workflow file.')
    parser.add_argument('secondary_file', help='A secondary data file (json).')
    parser.add_argument("--jobStore", type=str, required=False, default=None)
    parser.add_argument('-o',
                        '--outdir',
                        required=False,
                        default=os.getcwd(),
                        help='Optionally specify the directory that outputs '
                             'are written to.  Default is the current working dir.')
    parser.add_argument('--dev_mode', required=False, default=False,
                        help='1. Creates "AST.out", which holds the printed AST and '
                             '"mappings.out", which holds the parsed task, workflow '
                             'dictionaries that were generated.  '
                             '2. Saves the compiled toil script generated from the '
                             'wdl/json files from deletion.  '
                             '3. Skips autorunning the compiled python file.')
    parser.add_argument('--docker_user', required=False, default='root',
                        help='The user permissions that the docker containers will be run '
                             'with (and the permissions set on any output files produced).  '
                             'Default is "root".  Setting this to None will set this to '
                             'the current user.')
    parser.add_argument("--destBucket", type=str, required=False, default=False,
                        help="Specify a cloud bucket endpoint for output files.")

    # wdl_run_args is an array containing all of the unknown arguments not
    # specified by the parser in this main.  All of these will be passed down in
    # check_call later to run the compiled toil file.
    args, wdl_run_args = parser.parse_known_args()

    wdl_file = os.path.abspath(args.wdl_file)
    args.secondary_file = os.path.abspath(args.secondary_file)
    args.outdir = os.path.abspath(args.outdir)

    aWDL = get_analyzer(wdl_file=wdl_file)

    if args.dev_mode:
        aWDL.write_AST(out_dir=args.outdir)

    # read secondary file; create dictionary to hold variables
    if args.secondary_file.endswith('.json'):
        json_dict = dict_from_JSON(args.secondary_file)
    else:
        raise RuntimeError('Unsupported Secondary File Type.  Use json.')

    aWDL.analyze()

    sWDL = SynthesizeWDL(aWDL.version,
                         aWDL.tasks_dictionary,
                         aWDL.workflows_dictionary,
                         args.outdir,
                         json_dict,
                         args.docker_user,
                         args.jobStore,
                         args.destBucket)

    # use the AST dictionaries to write 4 strings
    # these are the future 4 sections of the compiled toil python file
    module_section = sWDL.write_modules()
    fn_section = sWDL.write_functions()
    main_section = sWDL.write_main()

    # write 3 strings to a python output file
    sWDL.write_python_file(module_section,
                           fn_section,
                           main_section,
                           sWDL.output_file)

    if args.dev_mode:
        logger.debug('WDL file compiled to toil script.')
        write_mappings(aWDL)
    else:
        logger.debug('WDL file compiled to toil script.  Running now.')
        exe = sys.executable if sys.executable else 'python'
        cmd = [exe, sWDL.output_file]
        cmd.extend(wdl_run_args)
        subprocess.check_call(cmd)
        os.remove(sWDL.output_file)


if __name__ == '__main__':
    main()
