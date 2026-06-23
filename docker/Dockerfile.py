# Copyright (C) 2015-2020 Regents of the University of California
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

import os
import sys
import textwrap

applianceSelf = os.environ['TOIL_APPLIANCE_SELF']
sdistName = os.environ['_TOIL_SDIST_NAME']

# Make sure to install packages into the pip for the version of Python we are
# building for.
python = f'python{sys.version_info[0]}.{sys.version_info[1]}'
pip = f'{python} -m pip'

# Debian and Ubuntu don't package ensurepip by default so the python-venv package must be installed
python_packages = {'python3.10': ['python3.10-distutils', 'python3.10-venv'],
                   'python3.11': ['python3.11-distutils', 'python3.11-venv'],
                   'python3.12': ['python3.12-venv'],
                   'python3.13': ['python3.13-venv'],
                   'python3.14': ['python3.14-venv'],
                   }  # python3.13 removed distutils

dependencies = ' '.join(python_packages[python] +
                        ['libffi-dev',  # For client side encryption for extras with PyNACL
                         python,
                         f'{python}-dev',
                         'python3-pip',
                         'libssl-dev',
                         'wget',
                         'curl',
                         'openssh-server',
                         "nodejs",  # CWL support for javascript expressions
                         'rsync',
                         'screen',
                         'libarchive13',
                         'libc6',
                         'libseccomp2',
                         'e2fsprogs',
                         'uuid-dev',
                         'libgpgme11-dev',
                         'libseccomp-dev',
                         'pkg-config',
                         'squashfs-tools',
                         'cryptsetup',
                         'less',
                         'vim',
                         'git',
                         'docker.io',
                         'time',
                         # Dependencies for Mesos which the deb doesn't actually list
                         'libsvn1',
                         'libcurl4-openssl-dev',
                         'libapr1',
                         # Dependencies for singularity
                         'containernetworking-plugins',
                         'libfuse2',
                         'fuse',
                         'fuse2fs',
                         'uidmap',
                         'squashfs-tools-ng',
                         'singularity-container',
                         # Dependencies for singularity on kubernetes
                         'tzdata',
                         # Dependencies for building pysam when we need it for Cactus testing and there's no wheel
                         'libbz2-dev',
                         'liblzma-dev'])

# pymesos's http-parser dependency can't build on Python later than 3.10, as
# released in 0.9.0. The upstream pymesos can, but we write it out of Toil's
# dependencies on later Python versions, since a working http-parser is not
# available in PyPI. So we need to manually inject a working http-parser, and
# pymesos, into the Docker images.
extra_mesos_python_modules = {
    'python3.10': [],
    'python3.11': ['http-parser@git+https://github.com/adamnovak/http-parser.git@5a63516597bb4c93a7ba178b1e4bab939da5afb3', 'pymesos==0.3.15'],
    'python3.12': ['http-parser@git+https://github.com/adamnovak/http-parser.git@5a63516597bb4c93a7ba178b1e4bab939da5afb3', 'pymesos==0.3.15'],
    'python3.13': ['http-parser@git+https://github.com/adamnovak/http-parser.git@5a63516597bb4c93a7ba178b1e4bab939da5afb3', 'pymesos==0.3.15'],
    'python3.14': ['http-parser@git+https://github.com/adamnovak/http-parser.git@5a63516597bb4c93a7ba178b1e4bab939da5afb3', 'pymesos==0.3.15']
}

extra_python_modules = " ".join(extra_mesos_python_modules[python])


def heredoc(s):
    s = textwrap.dedent(s).format(**globals())
    return s[1:] if s.startswith('\n') else s


motd = heredoc('''

    This is the Toil appliance. You can run your Toil workflow directly on the appliance.
    For more information see http://toil.readthedocs.io/en/latest/

    Copyright (C) 2015-2025 Regents of the University of California

    Version: {applianceSelf}

''')

# Prepare motd to be echoed in the Dockerfile using a RUN statement that uses bash's print
motd = ''.join(l + '\\n\\\n' for l in motd.splitlines())

print(heredoc('''
    FROM ubuntu:26.04

    ARG TARGETARCH

    RUN if [ -z "$TARGETARCH" ] ; then echo "Specify a TARGETARCH argument to build this container"; exit 1; fi

    # Try to avoid "Failed to fetch ...  Undetermined Error" from apt
    # See <https://stackoverflow.com/a/66523384>
    RUN printf 'Acquire::http::Pipeline-Depth "0";\\nAcquire::http::No-Cache=True;\\nAcquire::BrokenProxy=true;\\n' >/etc/apt/apt.conf.d/99fixbadproxy

    RUN apt-get -y update --fix-missing && apt-get -y install --no-upgrade apt-transport-https ca-certificates software-properties-common curl && apt-get clean && rm -rf /var/lib/apt/lists/*

    RUN add-apt-repository -y ppa:deadsnakes/ppa

    # Find a repo with a Mesos build.
    # This one was archived like:
    # mkdir mesos-repo && cd mesos-repo
    # wget --recursive --restrict-file-names=windows -k --convert-links --no-parent --page-requisites -m https://rpm.aventer.biz/Ubuntu/ https://www.aventer.biz/assets/support_aventer.asc https://rpm.aventer.biz/README.txt
    # ipfs add -r .
    # It contains a GPG key that will expire 2026-09-28
    # This is served out of /public/groups/cgl/public_html on the GI public infrastructure.
    # Make sure to use the current signing key setup as described in <https://askubuntu.com/a/1307181>
    RUN echo "deb [signed-by=/etc/apt/keyrings/support_aventer.asc] https://public.gi.ucsc.edu/cgl/ci/toil/dependencies/ipfs/Qmcd6B5gS42p99BzKjNsWuBJ9X4dEk7JEh7N9Hr6EYMzfn/rpm.aventer.biz/Ubuntu/noble noble main" \
        > /etc/apt/sources.list.d/mesos.list \
        && mkdir -p /etc/apt/keyrings/ \
        && curl https://public.gi.ucsc.edu/cgl/ci/toil/dependencies/ipfs/Qmcd6B5gS42p99BzKjNsWuBJ9X4dEk7JEh7N9Hr6EYMzfn/www.aventer.biz/assets/support_aventer.asc >/etc/apt/keyrings/support_aventer.asc

    RUN apt-get -y update --fix-missing && \
        DEBIAN_FRONTEND=noninteractive apt-get -y install --no-upgrade {dependencies} && \
        if [ $TARGETARCH = amd64 ] ; then DEBIAN_FRONTEND=noninteractive apt-get -y install --no-upgrade aventer-mesos ; mesos-agent --help >/dev/null ; fi && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*

    # Set up Singularity configuration and move out of the way for wrapper
    RUN sed -i 's!bind path = /etc/localtime!#bind path = /etc/localtime!g' /etc/singularity/singularity.conf && \
        mkdir -p /usr/local/libexec/toil && \
        mv /usr/bin/singularity /usr/local/libexec/toil/singularity-real \
        && /usr/local/libexec/toil/singularity-real version

    RUN mkdir /root/.ssh && \
        chmod 700 /root/.ssh

    ADD waitForKey.sh /usr/bin/waitForKey.sh

    ADD customDockerInit.sh /usr/bin/customDockerInit.sh

    # Wrap Singularity to use a Docker mirror instead of always Docker Hub
    # We need to put it where the installed singularity expects singularity to actually be.
    ADD singularity-wrapper.sh /usr/bin/singularity

    RUN chmod 777 /usr/bin/waitForKey.sh && chmod 777 /usr/bin/customDockerInit.sh && chmod 777 /usr/bin/singularity

    # The stock pip is too old and can't install from sdist with extras
    RUN curl -sS https://bootstrap.pypa.io/get-pip.py | {python}

    # Include virtualenv, as it is still the recommended way to deploy
    # pipelines.
    #
    # We need to --ignore-installed here to allow shadowing system packages
    # from apt in /usr/lib/python3/dist-packages when the installed package
    # needs newer versions. We just hope that doesn't break the Ubuntu system
    # too badly when we're actually on the system Python, or if Toil needs to
    # upgrade a distutils or setuptools dependency. On the deadsnakes Pythons,
    # installations into the version-specific package directory won't be seen
    # by the system Python which is a different version.
    #
    # TODO: Change to nested virtual environments and .pth files and teach Toil
    # to just ship the user-level one for hot deploy.
    RUN {pip} install --ignore-installed --upgrade 'virtualenv>=20.25.1,<21'

    RUN {pip} install --ignore-installed --upgrade 'setuptools>=80,<81'

    # Fix for https://issues.apache.org/jira/browse/MESOS-3793
    ENV MESOS_LAUNCHER=posix

    # Fix for `screen` (https://github.com/BD2KGenomics/toil/pull/1386#issuecomment-267424561)
    ENV TERM linux

    # Run bash instead of sh inside of screen
    ENV SHELL /bin/bash
    RUN echo "defshell -bash" > ~/.screenrc

    # An appliance may need to start more appliances, e.g. when the leader appliance launches the
    # worker appliance on a worker node. To support this, we embed a self-reference into the image:
    ENV TOIL_APPLIANCE_SELF {applianceSelf}

    RUN mkdir /var/lib/toil
    ENV TOIL_WORKDIR /var/lib/toil

    # We want to get binaries mounted in from the environemnt on Toil-managed Kubernetes
    env PATH /opt/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

    # We want to pick the right Python when the user runs it
    RUN rm -f /usr/bin/python3 && rm -f /usr/bin/python && \
        ln -s /usr/bin/{python} /usr/bin/python3 && \
        ln -s /usr/bin/python3 /usr/bin/python

    # This component changes most frequently and keeping it last maximizes Docker cache hits.
    COPY {sdistName} .
    RUN {pip} install --ignore-installed --upgrade {sdistName}[all] {extra_python_modules}
    RUN rm {sdistName}

    # We intentionally inherit the default ENTRYPOINT and CMD from the base image, to the effect
    # that the running appliance just gives you a shell. To start the Mesos daemons, the user
    # should override the entrypoint via --entrypoint.

    RUN echo '[ ! -z "$TERM" -a -r /etc/motd ] && cat /etc/motd' >> /etc/bash.bashrc \
        && printf '{motd}' > /etc/motd
    
    # Final check that Toil actually runs at all in the container
    RUN toil --version
'''))
