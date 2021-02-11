#!/usr/bin/env python3
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa


EXTENSIONS_TO_PROCESS = {'.csv',
                         '.cwl',
                         '.input',
                         '.json',
                         '.md',
                         '.mtail',
                         '.py',
                         '.rst',
                         '.sh',
                         '.test-pr',
                         '.tsv',
                         '.txt',
                         '.wdl',
                         '.yaml',
                         '.yml',
                         'Dockerfile',
                         'Makefile'}


def strip_whitespace_from_file(filename: str):
    pristine_lines = []
    with open(filename, 'r') as r:
        for line in r:
            pristine_lines.append(line.rstrip())

    with open(filename, 'w') as w:
        for pristine_line in pristine_lines:
            w.write(pristine_line + '\n')


def strip_whitespace_from_all_files_in_dir(dirname: str):
    for dirpath, dirnames, filenames in os.walk(dirname):
        for f in filenames:
            for ext in EXTENSIONS_TO_PROCESS:
                if f.endswith(ext):
                    strip_whitespace_from_file(os.path.abspath(os.path.join(dirpath, f)))
                    break
        for d in dirnames:
            strip_whitespace_from_all_files_in_dir(os.path.abspath(os.path.join(dirpath, d)))


def main():
    for directory in ['src', 'docs', 'docker', 'dashboard', 'attic']:
        strip_whitespace_from_all_files_in_dir(os.path.join(pkg_root, directory))


if __name__ == '__main__':
    main()
