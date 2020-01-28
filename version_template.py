# Copyright (C) 2015-2018 Regents of the University of California
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

"""This script is a template for src/toil/version.py. Running it without arguments echoes all
globals, i.e. module attributes. Constant assignments will be echoed verbatim while callables
will be invoked and their result echoed as an assignment using the function name as the left-hand
side and the return value of the function as right-hand side. To prevent a module attribute from
being echoed, start or end the attribute name with an underscore. To print the value of a single
symbol, pass the name of that attribute to the script as a command line argument. You can also
import the expand_ function and invoke it directly with either no or exactly one argument."""

# Note to maintainers:
#
#  - don't import at module level unless you want the imported value to be included in the output
#  - only import from the Python standard run-time library (you can't have any dependencies)


baseVersion = '3.23.0a1'
cgcloudVersion = '1.6.0a1.dev393'
dockerName = 'toil'


def version():
    """
    A version identifier that includes the full-legth commit SHA1 and an optional suffix to
    indicate that the working copy is dirty.
    """
    return _version()


def shortVersion():
    """
    A version identifier that includes the abbreviated commit SHA1 and an optional suffix to
    indicate that the working copy is dirty.
    """
    return _version(shorten=True)


def _version(shorten=False):
    return '-'.join(filter(None, [distVersion(),
                                  currentCommit()[:7 if shorten else None],
                                  ('dirty' if dirty() else None)]))


def distVersion():
    """The distribution version identifying a published release on PyPI."""
    from pkg_resources import parse_version
    if isinstance(parse_version(baseVersion), tuple):
        raise RuntimeError("Setuptools version 8.0 or newer required. Update by running "
                           "'pip install setuptools --upgrade'")
    return baseVersion


def dockerTag():
    """The primary tag of the Docker image for the appliance. This uniquely identifies the appliance image."""
    return version()


def dockerShortTag():
    """A secondary, shortened form of :func:`dockerTag` with which to tag the appliance image for convenience."""
    return shortVersion()


def dockerMinimalTag():
    """
    A minimal tag with which to tag the appliance image for convenience. Does not include
    information about the git commit or working copy dirtyness.
    """
    return distVersion()


def currentCommit():
    from subprocess import check_output
    try:
        output = check_output('git log --pretty=oneline -n 1 -- $(pwd)', shell=True).decode('utf-8').split()[0]
    except:
        # Return this we are not in a git environment.
        return '000'
    if isinstance(output, bytes):
        return output.decode('utf-8')
    return str(output)


def dockerRegistry():
    import os
    return os.getenv('TOIL_DOCKER_REGISTRY', 'quay.io/ucsc_cgl')


def dirty():
    from subprocess import call
    try:
        return 0 != call('(git diff --exit-code && git diff --cached --exit-code) > /dev/null', shell=True)
    except:
        return False  # In case the git call fails.


def expand_(name=None):
    variables = {k: v for k, v in globals().items()
                 if not k.startswith('_') and not k.endswith('_')}

    def resolve(k):
        v = variables[k]
        if callable(v):
            v = v()
        return v

    if name is None:
        return ''.join("%s = %s\n" % (k, repr(resolve(k))) for k, v in variables.items())
    else:
        return resolve(name)


def _main():
    import sys
    sys.stdout.write(expand_(*sys.argv[1:]))


if __name__ == '__main__':
    _main()
