"""
Utility functions for hook scripts.
"""

import sys
import os
import subprocess
import socket

from subprocess import CalledProcessError, TimeoutExpired
from typing import Tuple, Optional

def complain(message):
    sys.stderr.flush()
    sys.stderr.write(message)
    sys.stderr.write('\n')
    sys.stderr.flush()

def announce(message):
    sys.stderr.flush()
    sys.stderr.write(message)
    sys.stderr.write('\n')
    sys.stderr.flush()

cached_hostname = None
def get_hostname() -> str:
    global cached_hostname
    if cached_hostname is None:
        try:
            cached_hostname = socket.getfqdn()
        except:
            cached_hostname = 'localhost'
    return cached_hostname

def file_link(path: str, text: Optional[str] = None) -> str:
    
    # Determine link protocol to use
    if 'SSH_TTY' in os.environ:
        protocol = 'sftp'
    else:
        protocol = 'file'
    
    realpath = os.path.realpath(path)
    if not text:
        text = path    
        
    return f'\033]8;;{protocol}://{get_hostname()}{realpath}\033\\{text}\033]8;;\033\\'

def in_acceptable_environment() -> bool:
    try:
        # We need to be able to get at Toil, and we need to be in a virtual
        # environment.
        from toil import inVirtualEnv
        return inVirtualEnv()
    except:
        # If we can't do that, either we're not in a Toil dev environment or
        # Toil is Very Broken and that will be caught other ways.
        return False

def get_current_commit() -> str:
    """
    Get the currently checked-out commit.
    """
    return subprocess.check_output(['git', 'rev-parse', 'HEAD']).decode('utf-8').strip()
    
def is_rebase():
    """
    Return true if we think we are currently rebasing.
    """
    git_dir = os.getenv('GIT_DIR', '.git')
    return os.path.exists(os.path.join(git_dir, 'rebase-merge')) or os.path.exists(os.path.join(git_dir, 'rebase-apply'))

# We have a cache for mypy results so we can compute them in advance.
CACHE_DIR = '.mypy_toil_result_cache'
# But we don't want it to last too long.
USER_DIR = f'/var/run/user/{os.getuid()}'
if os.path.isdir(USER_DIR):
    CACHE_DIR = os.path.join(USER_DIR, CACHE_DIR)

def write_cache(commit: str, result: bool, log: str) -> str:
    """
    Save the given status and log to the cache for the given commit.
    Returns cache filename for log text.
    """

    os.makedirs(CACHE_DIR, exist_ok=True)
    basename = os.path.join(CACHE_DIR, commit)
    if os.path.exists(basename + '.fail.txt'):
        os.unlink(basename + '.fail.txt')
    if os.path.exists(basename + '.success.txt'):
        os.unlink(basename + '.success.txt')
    fullname = basename + ('.success.txt' if result else '.fail.txt')
    with open(fullname, 'w') as f:
        f.write(log)
    return fullname

def read_cache(commit: str) -> Tuple[Optional[bool], Optional[str]]:
    """
    Read the status and log from the cache for the given commit.
    """

    status = None
    log = None

    basename = os.path.join(CACHE_DIR, commit)
    fullname = None
    if os.path.exists(basename + '.fail.txt'):
        # We have a cached failure.
        fullname = basename + '.fail.txt'
        status = False
    elif os.path.exists(basename + '.success.txt'):
        # We have a cached success
        fullname = basename + '.success.txt'
        status = True
    if fullname:
        log = open(fullname).read()
    return status, log

def check_to_cache(local_object, timeout: float = None) -> Tuple[Optional[bool], Optional[str], Optional[str]]:
    """
    Type-check current commit and save result to cache. Return status, log, and log filename, or None, None, None if a timeout is hit.
    """

    try:
        # As a hook we know we're in the project root when running.
        mypy_output = subprocess.check_output(['make', 'mypy'], stderr=subprocess.STDOUT, timeout=timeout)
        log = mypy_output.decode('utf-8')
        # If we get here it passed
        filename = write_cache(local_object, True, log)
        return True, log, filename
    except CalledProcessError as e:
        # It did not work.
        log = e.output.decode('utf-8')
        # Save this in a cache
        filename = write_cache(local_object, False, log)
        return False, log, filename
    except TimeoutExpired:
        return None, None, None
        
