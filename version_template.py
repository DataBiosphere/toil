# Copyright (C) 2015-2021 Regents of the University of California
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
#  - don't import at module level unless you want the imported value to be
#    included in the output
#  - only import from the Python standard run-time library (you can't have any
#    dependencies)
#  - don't import even standard modules at global scope without renaming them
#    to have leading/trailing underscores

baseVersion = '5.5.0a1'
cgcloudVersion = '1.6.0a1.dev393'


def version():
    """
    A version identifier that includes the full-legth commit SHA1 and an optional suffix to
    indicate that the working copy is dirty.
    """
    return '-'.join(filter(None, [distVersion(), currentCommit(), ('dirty' if dirty() else None)]))


def distVersion():
    """The distribution version identifying a published release on PyPI."""
    from pkg_resources import parse_version
    if isinstance(parse_version(baseVersion), tuple):
        raise RuntimeError("Setuptools version 8.0 or newer required. Update by running "
                           "'pip install setuptools --upgrade'")
    return baseVersion


def exactPython():
    """
    Returns the Python command for the exact version of Python we are installed
    for. Something like 'python2.7' or 'python3.6'.
    """
    import sys
    return 'python{}.{}'.format(sys.version_info[0], sys.version_info[1])


def python():
    """
    Returns the Python command for the general version of Python we are
    installed for.  Something like 'python2.7' or 'python3'.

    We assume all Python 3s are sufficiently intercompatible that we can just
    use 'python3' here for all of them. This is useful because the Toil Docker
    appliance is only built for particular Python versions, and we would like
    workflows to work with a variety of leader Python versions.
    """
    return exactPython()


def _pythonVersionSuffix():
    """
    Returns a short string identifying the running version of Python. Toil
    appliances running the same Toil version but on different versions of
    Python as returned by this function are not compatible.
    """
    import sys

    # For now, we assume all Python 3 releases are intercompatible.
    # We also only tag the Python 2 releases specially, since Python 2 is old and busted.
    return '-py{}.{}'.format(sys.version_info[0], sys.version_info[1])


def dockerTag():
    """The primary tag of the Docker image for the appliance. This uniquely identifies the appliance image."""
    return version() + _pythonVersionSuffix()


def currentCommit():
    import os
    from subprocess import check_output
    try:
        git_root_dir = os.path.dirname(os.path.abspath(__file__))
        output = check_output('git log --pretty=oneline -n 1 -- {}'.format(git_root_dir),
                              shell=True,
                              cwd=git_root_dir).decode('utf-8').split()[0]
    except:
        # Return this if we are not in a git environment.
        return '000'
    if isinstance(output, bytes):
        return output.decode('utf-8')
    return str(output)


def dockerRegistry():
    import os
    return os.getenv('TOIL_DOCKER_REGISTRY', 'quay.io/ucsc_cgl')


def dockerName():
    import os
    return os.getenv('TOIL_DOCKER_NAME', 'toil')


def dirty():
    import os
    from subprocess import call
    try:
        git_root_dir = os.path.dirname(os.path.abspath(__file__))
        return 0 != call('(git diff --exit-code && git diff --cached --exit-code) > /dev/null',
                         shell=True,
                         cwd=git_root_dir)
    except:
        return False  # In case the git call fails.


def expand_(name=None, others=None):
    """
    Returns a string of all the globals and additional variables passed as the
    others keyword argument.

    :param str name: If set, only the value of the given symbol is returned.
    :param dict others: A dictionary of additional variables to be included in
                        the return value.
    """
    variables = {k: v for k, v in globals().items()
                 if not k.startswith('_') and not k.endswith('_')}

    if others is not None:
        variables.update(others)

    def resolve(k):
        v = variables.get(k, None)
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
