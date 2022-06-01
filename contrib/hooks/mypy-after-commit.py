#!/usr/bin/env python3

"""
mypy-after-commit.py: Git post-commit script to type-check commits in the background.

Install with:

ln -rs ./contrib/hooks/mypy-after-commit.py .git/hooks/post-commit
"""

import sys
import subprocess
import os

from lib import announce, complain, file_link, in_acceptable_environment, check_to_cache, get_current_commit, is_rebase

def main(argc, argv):
    # No input; we want to run in the background
    if in_acceptable_environment() and not is_rebase():
        announce('Type-checking commit')
        commit = get_current_commit()
        result, log, filename = check_to_cache(commit, timeout=10)
        if result is None:
            # Type checking timed out
            announce('This is taking a while. Type-checking in the background.')
            if os.fork() == 0:
                check_to_cache(commit)
        elif result:
            announce('Commit OK')
        else:
            complain('Commit did not type-check! ' + file_link(filename, 'See the log for details.'))
    return 0

if __name__ == "__main__":
    sys.exit(main(len(sys.argv), sys.argv))
