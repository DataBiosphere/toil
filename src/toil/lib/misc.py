import logging
import os
import random
import shutil
import subprocess
import sys
import typing

from typing import Iterator, Union, List, Optional

logger = logging.getLogger(__name__)


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
