#!/usr/bin/env python3
"""
A script to remove all trailing whitespace from the Toil codebase's text files
(mainly targeting .py files, but also things like .sh and .txt files).

Run with:
    remove_trailing_whitespace.py

Note: This script preserves file permissions (and thus, executability) and that should be maintained.
"""
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa


# changing either of these variables will change the targets of this script
TOIL_DIRS_TO_PROCESS = {'attic', 'dashboard', 'docker', 'docs', 'src'}
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


def strip_trailing_whitespace_from_file(filename: str):
    """Strips trailing whitespace from a file, in-place."""
    pristine_lines = []
    with open(filename, 'r') as r:
        for line in r:
            pristine_lines.append(line.rstrip())

    with open(filename, 'w') as w:
        for pristine_line in pristine_lines:
            w.write(pristine_line + '\n')


def strip_trailing_whitespace_from_all_files_in_dir(dirname: str):
    """
    Strips trailing whitespace from all files in a directory, recursively.

    Only strips files ending in one of the white-listed extensions in EXTENSIONS_TO_PROCESS.
    Note: This includes things like "Dockerfile" that end in "Dockerfile".
    """
    for dirpath, dirnames, filenames in os.walk(dirname):
        for f in filenames:
            for ext in EXTENSIONS_TO_PROCESS:
                if f.endswith(ext):
                    strip_trailing_whitespace_from_file(os.path.abspath(os.path.join(dirpath, f)))
                    break
        for d in dirnames:
            strip_trailing_whitespace_from_all_files_in_dir(os.path.abspath(os.path.join(dirpath, d)))


def main():
    for directory in TOIL_DIRS_TO_PROCESS:
        strip_trailing_whitespace_from_all_files_in_dir(os.path.join(pkg_root, directory))


if __name__ == '__main__':
    main()
