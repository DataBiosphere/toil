import datetime
import logging
import os
import random
import socket
import subprocess
import sys
import typing
from contextlib import closing
from typing import Iterator, List, Optional, Union

import pytz

logger = logging.getLogger(__name__)


def get_public_ip() -> str:
    """Get the IP that this machine uses to contact the internet.

    If behind a NAT, this will still be this computer's IP, and not the router's."""
    try:
        # Try to get the internet-facing IP by attempting a connection
        # to a non-existent server and reading what IP was used.
        ip = '127.0.0.1'
        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as sock:
            # 203.0.113.0/24 is reserved as TEST-NET-3 by RFC 5737, so
            # there is guaranteed to be no one listening on the other
            # end (and we won't accidentally DOS anyone).
            sock.connect(('203.0.113.1', 1))
            ip = sock.getsockname()[0]
        return ip
    except:
        # Something went terribly wrong. Just give loopback rather
        # than killing everything, because this is often called just
        # to provide a default argument
        return '127.0.0.1'

def utc_now() -> datetime.datetime:
    """Return a datetime in the UTC timezone corresponding to right now."""
    return datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)

def slow_down(seconds: float) -> float:
    """
    Toil jobs that have completed are not allowed to have taken 0 seconds, but
    Kubernetes timestamps round things to the nearest second. It is possible in
    some batch systems for a pod to have identical start and end timestamps.

    This function takes a possibly 0 job length in seconds and enforces a
    minimum length to satisfy Toil.

    :param float seconds: Timestamp difference

    :return: seconds, or a small positive number if seconds is 0
    :rtype: float
    """

    return max(seconds, sys.float_info.epsilon)

def printq(msg: str, quiet: bool) -> None:
    if not quiet:
        print(msg)


def truncExpBackoff() -> Iterator[float]:
    # as recommended here https://forums.aws.amazon.com/thread.jspa?messageID=406788#406788
    # and here https://cloud.google.com/storage/docs/xml-api/reference-status
    yield 0
    t = 1
    while t < 1024:
        # google suggests this dither
        yield t + random.random()
        t *= 2
    while True:
        yield t


class CalledProcessErrorStderr(subprocess.CalledProcessError):
    """Version of CalledProcessError that include stderr in the error message if it is set"""

    def __str__(self) -> str:
        if (self.returncode < 0) or (self.stderr is None):
            return str(super())
        else:
            err = self.stderr if isinstance(self.stderr, str) else self.stderr.decode("ascii", errors="replace")
            return "Command '%s' exit status %d: %s" % (self.cmd, self.returncode, err)


def call_command(cmd: List[str], *args: str, input: Optional[str] = None, timeout: Optional[float] = None,
                useCLocale: bool = True, env: Optional[typing.Dict[str, str]] = None) -> Union[str, bytes]:
    """Simplified calling of external commands.  This always returns
    stdout and uses utf- encode8.  If process fails, CalledProcessErrorStderr
    is raised.  The captured stderr is always printed, regardless of
    if an expect occurs, so it can be logged.  At the debug log level, the
    command and captured out are always used.  With useCLocale, C locale
    is force to prevent failures that occurred in some batch systems
    with UTF-8 locale.
    """

    # using non-C locales can cause GridEngine commands, maybe other to
    # generate errors
    if useCLocale:
        env = dict(os.environ) if env is None else dict(env)  # copy since modifying
        env["LANGUAGE"] = env["LC_ALL"] = "C"

    logger.debug("run command: {}".format(" ".join(cmd)))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            encoding='utf-8', errors="replace", env=env)
    stdout, stderr = proc.communicate(input=input, timeout=timeout)
    sys.stderr.write(stderr)
    if proc.returncode != 0:
        logger.debug("command failed: {}: {}".format(" ".join(cmd), stderr.rstrip()))
        raise CalledProcessErrorStderr(proc.returncode, cmd, output=stdout, stderr=stderr)
    logger.debug("command succeeded: {}: {}".format(" ".join(cmd), stdout.rstrip()))
    return stdout
