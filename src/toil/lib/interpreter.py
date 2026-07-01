"""
Runtime code injection system

When a workflow steps runs inside a container like Docker, where the contained
process is not a descendant of Toil, the Toil worker process on the host cannot
see how much CPU and RAM that step actually used.

This system allows Toil to inject code into the container that will colklect
and export resource usage information back to Toil.

It also allows Toil to do other checks on the container system.
"""

# Copyright (C) 2026 Regents of the University of California
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

import glob
import logging
import os
import platform
import shlex
import textwrap
from typing import Iterable

from toil.lib.resources import ResourceMonitor

logger = logging.getLogger(__name__)

# Code injected into the container communicates back to the rest of Toil
# through files in this directory.
INJECTED_MESSAGE_DIR = ".toil_runtime"


# We mostly want to work with shell script strings, but CWL works with command
# argument lists, so we use these functions to convert back and forth.

def command_line_to_shell_script(command_line: list[str]) -> str:
    """
    Extract or synthesize the inner shell script from a cwltool argv list.

    cwltool uses ``["/bin/sh", "-c", script]`` in some cases, so that pattern
    is handled specially.
    """
    if (
        len(command_line) == 3
        and command_line[0] in ("/bin/sh", "/bin/bash")
        and command_line[1] == "-c"
    ):
        return command_line[2]
    return " ".join(shlex.quote(arg) for arg in command_line) # this is the shell script string


def shell_script_to_command_line(script: str) -> list[str]:
    """
    Wrap a shell script as a list of arguments for launchign a process.

    The resulting command required Bash to be available.
    """
    return ["/bin/bash", "-c", script]

# Main function

def add_injections(
    command_string: str,
    file_mounts: Iterable[tuple[str, str]],
    message_dir: str = INJECTED_MESSAGE_DIR,
) -> str:
    """
    Add resource usage monitoring and file mount checking code to a command.

    The command is expected to be about to run in a container, under a
    container system that does not itself attribute resource usage to the
    calling Toil process (such as Docker, which uses a daemon).

    :param command_string: shell command or script to modify
    :param file_mounts: collection of (host path, container path) tuples for
        files mounted into the container. Code will be added to require that
        the container sees the complete file that the host sees.
    :param message_dir: directory relative to the working directory that the
        command should record resource usage to

    :returns: shell command string (possibly containing multiple commands) that
        runs the original command and reports resource usage.

    """

    parts = []
    # We're running on Docker or another platform where Toil isn't an ancestor
    # process, so we need to monitor CPU usage and so on from inside the
    # container, since it won't be attributed to Toil child processes in the
    # leader's self-monitoring.

    # TODO: Mount this script from a file Toil installs instead or something
    # instead of injecting it in every command line, which makes it show up in
    # logs.
    script = textwrap.dedent(
        """\
        function _toil_resource_monitor () {
            # Turn off error checking and echo in here
            set +ex
            MESSAGE_DIR="${1}"
            mkdir -p "${MESSAGE_DIR}"

            function sample_cpu_usec() {
                if [[ -f  /sys/fs/cgroup/cpu.stat ]] ; then
                    awk '{ if ($1 == "usage_usec") {print $2} }' /sys/fs/cgroup/cpu.stat
                elif [[ -f /sys/fs/cgroup/cpuacct/cpuacct.stat ]] ; then
                    echo $(( $(head -n 1 /sys/fs/cgroup/cpuacct/cpuacct.stat | cut -f2 -d' ') * 10000 ))
                fi
            }

            function sample_memory_bytes() {
                if [[ -f /sys/fs/cgroup/memory.stat ]] ; then
                    awk '{ if ($1 == "anon") { print $2 } }' /sys/fs/cgroup/memory.stat
                elif [[ -f /sys/fs/cgroup/memory/memory.stat ]] ; then
                    awk '{ if ($1 == "total_rss") { print $2 } }' /sys/fs/cgroup/memory/memory.stat
                fi
            }

            while true ; do
                printf "CPU\\t" >> ${MESSAGE_DIR}/resources.tsv
                sample_cpu_usec >> ${MESSAGE_DIR}/resources.tsv
                printf "Memory\\t" >> ${MESSAGE_DIR}/resources.tsv
                sample_memory_bytes >> ${MESSAGE_DIR}/resources.tsv
                sleep 1
            done
        }
        """
    )
    parts.append(script)
    # Launch in a subshell so that it doesn't interfere with Bash "wait" in the main shell
    parts.append(f"(_toil_resource_monitor {message_dir} &)")

    if platform.system() == "Darwin":
        # With gRPC FUSE file sharing, files immediately downloaded before
        # being mounted may appear as size 0 in the container due to a race
        # condition. Check for this and produce an approperiate error.

        script = textwrap.dedent(
            """\
            function _toil_check_size () {
                TARGET_FILE="${1}"
                GOT_SIZE="$(stat -c %s "${TARGET_FILE}")"
                EXPECTED_SIZE="${2}"
                if [[ "${GOT_SIZE}" != "${EXPECTED_SIZE}" ]] ; then
                    echo >&2 "Toil Error:"
                    echo >&2 "File size visible in container for ${TARGET_FILE} is size ${GOT_SIZE} but should be size ${EXPECTED_SIZE}"
                    echo >&2 "Are you using gRPC FUSE file sharing in Docker Desktop?"
                    echo >&2 "It doesn't work: see <https://github.com/DataBiosphere/toil/issues/4542>."
                    exit 1
                fi
            }
        """
        )
        parts.append(script)
        for host_path, job_path in file_mounts:
            expected_size = os.path.getsize(host_path)
            if expected_size != 0:
                parts.append(f'_toil_check_size "{job_path}" {expected_size}')

    parts.append(command_string)

    return "\n".join(parts)

# Helper functions to parse resource usage output

def handle_message_file(file_path: str) -> None:
    """
    Handle a message file received from injected code from :meth:`add_injections()`.

    Records the described usage as if it had occurred within a child process,
    by talking to the :class:`toil.lib.resources.ResourceMonitor` system.

    :param file_path: the host-side path of the message file.
    """
    if os.path.basename(file_path) == "resources.tsv":
        # This is a TSV of resource usage info.
        first_cpu_usec: int | None = None
        last_cpu_usec: int | None = None
        max_memory_bytes: int | None = None

        for line in open(file_path):
            if not line.endswith("\n"):
                # Skip partial lines
                continue
            # For each full line we got
            parts = line.strip().split("\t")
            if len(parts) != 2:
                # Skip odd-shaped lines
                continue
            if parts[0] == "CPU":
                # Parse CPU usage
                cpu_usec = int(parts[1])
                # Update summary stats
                if first_cpu_usec is None:
                    first_cpu_usec = cpu_usec
                last_cpu_usec = cpu_usec
            elif parts[0] == "Memory":
                # Parse memory usage
                memory_bytes = int(parts[1])
                # Update summary stats
                if max_memory_bytes is None or max_memory_bytes < memory_bytes:
                    max_memory_bytes = memory_bytes

        if max_memory_bytes is not None:
            logger.info(
                "Container used at about %s bytes of memory at peak",
                max_memory_bytes,
            )
            # Treat it as if used by a child process
            ResourceMonitor.record_extra_memory(max_memory_bytes // 1024)
        if last_cpu_usec is not None:
            assert first_cpu_usec is not None
            cpu_seconds = (last_cpu_usec - first_cpu_usec) / 1000000
            logger.info("Container used about %s seconds of CPU time", cpu_seconds)
            # Treat it as if used by a child process
            ResourceMonitor.record_extra_cpu(cpu_seconds)

def handle_injection_messages_from_outdir(outdir: str) -> None:
    """
    Handle any message files in the job outdir.

    Files would have been left by injected code from :meth:`add_injections()`.
    """
    message_dir = os.path.join(outdir, INJECTED_MESSAGE_DIR)
    for file_path in glob.glob(os.path.join(message_dir, "*")):
        if os.path.isfile(file_path):
            handle_message_file(file_path)
