#!/usr/bin/env python3

"""
mypy-before-push.py: Git pre-push script to make sure that you know if something you push is going to fail MyPy linting.

Install with:

ln -rs ./contrib/hooks/mypy-before-push.py .git/hooks/pre-push
"""

import sys
import subprocess
import os
from typing import Tuple, Optional

from lib import complain, announce, in_acceptable_environment, write_cache, read_cache, check_to_cache, get_current_commit

def check_can_run(local_object) -> bool:
    """
    Make sure we would be able to run mypy on the given commit.
    """

    if not in_acceptable_environment():
        announce('Environment not set up for type checking.')
        return False
    try:
        current_object = get_current_commit()
    except:
        announce('Currently checked-out commit cannot be determined.')
        return False
    if current_object != local_object:
        announce(f'Commit being pushed is not currently checked out')
        return False
    return True

def check_checked_out_commit(local_object) -> bool:
    """
    If the checked-out commit does not type-check, return false. Else, return true.
    """

    status, log = read_cache(local_object)
    if status is None:
        announce('Commit has not been type-checked. Checking now.')
        status, log = check_to_cache(local_object)
        from_cache = False
    else:
        from_cache = True
    if status == False:
        complain('Commit failed type-checking:')
        sys.stderr.write(log)
        # Type-check again in the background in case something has actually
        # changed about the environment.
        if from_cache:
            if os.fork() == 0:
                check_to_cache(local_object)
                sys.exit(0)
            else:
                announce('Re-checking in the background in case something changed.')
        return False
    elif status == True:
        announce('Commit passed type-checking.')
        return True

def main(argc, argv):
    for line in sys.stdin:
        line = line.strip()
        if line == '':
            continue
        parts = line.split()
        # Pushes will come from standard input as:
        # <local ref> SP <local object name> SP <remote ref> SP <remote object name> LF
        # See <https://www.git-scm.com/docs/githooks#_pre_push>
        local_ref = parts[0]
        local_object = parts[1]
        if local_ref == '(delete)':
            # Deleting a branch. Nothing to do
            continue
        if not check_can_run(local_object):
            announce('Cannot check the commit being pushed.')
            return 0
        if not check_checked_out_commit(local_object):
            complain('You should not push this. CI would fail!')
            return 1
    return 0

if __name__ == "__main__":
    sys.exit(main(len(sys.argv), sys.argv))
