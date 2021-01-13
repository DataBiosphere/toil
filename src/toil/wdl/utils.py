# Copyright (C) 2020-2021 UCSC Computational Genomics Lab
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
import json

from toil.wdl.wdl_analysis import AnalyzeWDL


def get_version(iterable) -> str:
    """
    Get the version of the WDL document.

    :param iterable: An iterable that contains the lines of a WDL document.
    :return: The WDL version used in the workflow.
    """
    if isinstance(iterable, str):
        iterable = iterable.split('\n')

    for line in iterable:
        line = line.strip()
        # check if the first non-empty, non-comment line is the version statement
        if line and not line.startswith('#'):
            if line.startswith('version '):
                return line[8:].strip()
            break
    # only draft-2 doesn't contain the version declaration
    return 'draft-2'


def get_analyzer(wdl_file: str) -> AnalyzeWDL:
    """
    Creates an instance of an AnalyzeWDL implementation based on the version.

    :param wdl_file: The path to the WDL file.
    """
    with open(wdl_file, 'r') as f:
        version = get_version(f)

    if version == 'draft-2':
        from toil.wdl.versions.draft2 import AnalyzeDraft2WDL
        return AnalyzeDraft2WDL(wdl_file)
    elif version == '1.0':
        from toil.wdl.versions.v1 import AnalyzeV1WDL
        return AnalyzeV1WDL(wdl_file)
    elif version == 'development':
        from toil.wdl.versions.dev import AnalyzeDevelopmentWDL
        return AnalyzeDevelopmentWDL(wdl_file)
    else:
        raise RuntimeError(f"Unsupported WDL version: '{version}'.")


def dict_from_JSON(JSON_file: str) -> dict:
    """
    Takes a WDL-mapped json file and creates a dict containing the bindings.

    :param JSON_file: A required JSON file containing WDL variable bindings.
    """
    json_dict = {}

    # TODO: Add context support for variables within multiple wdl files

    with open(JSON_file) as data_file:
        data = json.load(data_file)
    for d in data:
        if isinstance(data[d], str):
            json_dict[d] = f'"{data[d]}"'
        else:
            json_dict[d] = data[d]
    return json_dict


def write_mappings(parser: AnalyzeWDL, filename: str = 'mappings.out') -> None:
    """
    Takes an AnalyzeWDL instance and writes the final task dict and workflow
    dict to the given file.

    :param parser: An AnalyzeWDL instance.
    :param filename: The name of a file to write to.
    """
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
                (self.types[type(value[key]) if type(value[key]) in self.types else object])(self, value[key],
                                                                                             indent + 1)
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

    with open(filename, 'w') as f:
        f.write(pretty(parser.tasks_dictionary))
        f.write('\n\n\n\n\n\n')
        f.write(pretty(parser.workflows_dictionary))
