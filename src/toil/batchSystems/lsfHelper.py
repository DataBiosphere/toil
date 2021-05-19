#!/usr/bin/env python
# Adapted from https://github.com/roryk/ipython-cluster-helper
# ipython-cluster-helper is licensed under the MIT license
#
# Copyright 2013-2017 "Rory Kirchne" <rory.kirchner@gmail.com> and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import fnmatch
import os
import re
import subprocess

from packaging import version

from toil.lib.conversions import bytes_in_unit, convert_units

LSB_PARAMS_FILENAME = "lsb.params"
LSF_CONF_FILENAME = "lsf.conf"
LSF_CONF_ENV = ["LSF_CONFDIR", "LSF_ENVDIR"]
DEFAULT_LSF_UNITS = "KB"
DEFAULT_RESOURCE_UNITS = "MB"
LSF_JSON_OUTPUT_MIN_VERSION = "10.1.0.2"


def find(basedir, string):
    """
    walk basedir and return all files matching string
    """
    matches = []
    for root, dirnames, filenames in os.walk(basedir):
        for filename in fnmatch.filter(filenames, string):
            matches.append(os.path.join(root, filename))
    return matches


def find_first_match(basedir, string):
    """
    return the first file that matches string starting from basedir
    """
    matches = find(basedir, string)
    return matches[0] if matches else matches


def get_conf_file(filename, env):
    conf_path = os.environ.get(env)
    if not conf_path:
        return None
    conf_file = find_first_match(conf_path, filename)
    return conf_file


def apply_conf_file(fn, conf_filename):
    for env in LSF_CONF_ENV:
        conf_file = get_conf_file(conf_filename, env)
        if conf_file:
            with open(conf_file, encoding='utf-8') as conf_handle:
                value = fn(conf_handle)
            if value:
                return value
    return None


def per_core_reserve_from_stream(stream):
    for k, v in tokenize_conf_stream(stream):
        if k in {"RESOURCE_RESERVE_PER_SLOT", "RESOURCE_RESERVE_PER_TASK"}:
            return v.upper()
    return None


def get_lsf_units_from_stream(stream):
    for k, v in tokenize_conf_stream(stream):
        if k == "LSF_UNIT_FOR_LIMITS":
            return v
    return None


def tokenize_conf_stream(conf_handle):
    """
    convert the key=val pairs in a LSF config stream to tuples of tokens
    """
    for line in conf_handle:
        if line.startswith("#"):
            continue
        tokens = line.split("=")
        if len(tokens) != 2:
            continue
        yield (tokens[0].strip(), tokens[1].strip())


def apply_bparams(fn):
    """
    apply fn to each line of bparams, returning the result
    """
    cmd = ["bparams", "-a"]
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
    except:
        return None
    return fn(output.split("\n"))


def apply_lsadmin(fn):
    """
    apply fn to each line of lsadmin, returning the result
    """
    cmd = ["lsadmin", "showconf", "lim"]
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
    except:
        return None
    return fn(output.split("\n"))


def get_lsf_units(resource: bool = False) -> str:
    """
    check if we can find LSF_UNITS_FOR_LIMITS in lsadmin and lsf.conf
    files, preferring the value in bparams, then lsadmin, then the lsf.conf file
    """
    lsf_units = apply_bparams(get_lsf_units_from_stream)
    if lsf_units:
        return lsf_units

    lsf_units = apply_lsadmin(get_lsf_units_from_stream)
    if lsf_units:
        return lsf_units

    lsf_units = apply_conf_file(get_lsf_units_from_stream, LSF_CONF_FILENAME)
    if lsf_units:
        return lsf_units

    # -R usage units are in MB, not KB by default
    if resource:
        return DEFAULT_RESOURCE_UNITS
    else:
        return DEFAULT_LSF_UNITS


def parse_mem_and_cmd_from_output(output: str):
    """Use regex to find "MAX MEM" and "Command" inside of an output."""
    # Handle hard wrapping in the middle of words and arbitrary
    # indents. May drop spaces at the starts of lines that aren't
    # meant to be part of the indent.
    cleaned_up_output = ' '.join(re.sub(r"\n\s*", "", output).split(','))
    max_mem = re.search(r"MAX ?MEM: ?(.*?);", cleaned_up_output)
    command = re.search(r"Command ?<(.*?)>", cleaned_up_output)
    return max_mem, command


def get_lsf_version():
    """
    Get current LSF version
    """
    cmd = ["lsid"]
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
    except:
        return None
    bjobs_search = re.search('IBM Spectrum LSF Standard (.*),', output)
    if bjobs_search:
        lsf_version = bjobs_search.group(1)
        return lsf_version
    else:
        return None


def check_lsf_json_output_supported():
    """Check if the current LSF system supports bjobs json output."""
    try:
        lsf_version = get_lsf_version()
        if lsf_version and (version.parse(lsf_version) >= version.parse(LSF_JSON_OUTPUT_MIN_VERSION)):
            return True
    except:
        return False
    return False


def parse_memory_resource(mem: float) -> str:
    """Parse memory parameter for -R."""
    return parse_memory(mem, True)


def parse_memory_limit(mem: float) -> str:
    """Parse memory parameter for -M."""
    return parse_memory(mem, False)


def parse_memory(mem: float, resource: bool) -> str:
    """Parse memory parameter."""
    lsf_unit = get_lsf_units(resource=resource)
    megabytes_of_mem = convert_units(float(mem), src_unit=lsf_unit, dst_unit='MB')
    if megabytes_of_mem < 1:
        megabytes_of_mem = 1.0
    # round as a string here to avoid returning something like 1.231e+12
    return f'{megabytes_of_mem:.0f}MB'


def per_core_reservation():
    """
    returns True if the cluster is configured for reservations to be per core,
    False if it is per job
    """
    per_core = apply_bparams(per_core_reserve_from_stream)
    if per_core:
        if per_core.upper() == "Y":
            return True
        else:
            return False

    per_core = apply_lsadmin(per_core_reserve_from_stream)
    if per_core:
        if per_core.upper() == "Y":
            return True
        else:
            return False

    per_core = apply_conf_file(per_core_reserve_from_stream, LSB_PARAMS_FILENAME)
    if per_core and per_core.upper() == "Y":
        return True
    return False


if __name__ == "__main__":
    print(get_lsf_units())
    print(per_core_reservation())
