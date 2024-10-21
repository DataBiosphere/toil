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
python_packages = {'python3.9': ['python3.9-distutils', 'python3.9-venv'],
                   'python3.10': ['python3.10-distutils', 'python3.10-venv'],
                   'python3.11': ['python3.11-distutils', 'python3.11-venv'],
                   'python3.12': ['python3.12-distutils', 'python3.12-venv']}

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
                         'libcurl4-nss-dev',
                         'libapr1',
                         # Dependencies for singularity
                         'containernetworking-plugins',
                         'libfuse2',
                         'fuse',
                         'fuse2fs',
                         'uidmap',
                         'squashfs-tools-ng',
                         # Dependencies for singularity on kubernetes
                         'tzdata'])


def heredoc(s):
    s = textwrap.dedent(s).format(**globals())
    return s[1:] if s.startswith('\n') else s


motd = heredoc('''

    This is the Toil appliance. You can run your Toil script directly on the appliance.
    Run toil <workflow>.py --help to see all options for running your workflow.
    For more information see http://toil.readthedocs.io/en/latest/

    Copyright (C) 2015-2022 Regents of the University of California

    Version: {applianceSelf}

''')

# Prepare motd to be echoed in the Dockerfile using a RUN statement that uses bash's print
motd = ''.join(l + '\\n\\\n' for l in motd.splitlines())

print(heredoc('''
    FROM ubuntu:22.04

    ARG TARGETARCH

    RUN if [ -z "$TARGETARCH" ] ; then echo "Specify a TARGETARCH argument to build this container"; exit 1; fi

    # Try to avoid "Failed to fetch ...  Undetermined Error" from apt
    # See <https://stackoverflow.com/a/66523384>
    RUN printf 'Acquire::http::Pipeline-Depth "0";\\nAcquire::http::No-Cache=True;\\nAcquire::BrokenProxy=true;\\n' >/etc/apt/apt.conf.d/99fixbadproxy

    RUN apt-get -y update --fix-missing && apt-get -y upgrade && apt-get -y install apt-transport-https ca-certificates software-properties-common curl && apt-get clean && rm -rf /var/lib/apt/lists/*

    RUN add-apt-repository -y ppa:deadsnakes/ppa

    # Find a repo with a Mesos build.
    # This one was archived like:
    # mkdir mesos-repo && cd mesos-repo
    # wget --recursive --restrict-file-names=windows -k --convert-links --no-parent --page-requisites https://rpm.aventer.biz/Ubuntu/ https://www.aventer.biz/assets/support_aventer.asc https://rpm.aventer.biz/README.txt
    # ipfs add -r .
    RUN echo "deb https://public.gi.ucsc.edu/~anovak/outbox/toil/ipfs/QmeaErHzK4Dajz2mCMd36eUDQp7GX2bSECVRpGfrqdragR/rpm.aventer.biz/Ubuntu/focal focal main" \
        > /etc/apt/sources.list.d/mesos.list \
        && curl https://public.gi.ucsc.edu/~anovak/outbox/toil/ipfs/QmeaErHzK4Dajz2mCMd36eUDQp7GX2bSECVRpGfrqdragR/www.aventer.biz/assets/support_aventer.asc | apt-key add -

    RUN apt-get -y update --fix-missing && \
        DEBIAN_FRONTEND=noninteractive apt-get -y upgrade && \
        DEBIAN_FRONTEND=noninteractive apt-get -y install {dependencies} && \
        if [ $TARGETARCH = amd64 ] ; then DEBIAN_FRONTEND=noninteractive apt-get -y install mesos ; mesos-agent --help >/dev/null ; fi && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*

    # Install a particular old Debian Sid Singularity from somewhere.
    # It's 3.10, which is new enough to use cgroups2, but it needs a newer libc
    # than Ubuntu 20.04 ships. So we need a 22.04+ base.
    #
    # But 22.04 ships squashfs-tools 4.4 or 4.5 or so, which is new enough that
    # errors encountered during extraction produce a nonzero exit code, without
    # a special option:
    # <https://github.com/plougher/squashfs-tools/commit/1dd7f32e79b7600d379a4f26fb8d138ebdfc70be>.
    # If unsquashfs thinks it is root, but it can't change UIDs and GIDs freely, it
    # will continue but fail the whole command instead of returning success. It complains:
    #
    # set_attributes: failed to change uid and gids on /image/rootfs/etc/gshadow, because Invalid argument
    #
    # But inside a Kubernetes container we can be root but not actually be
    # allowed to set UIDs and GIDs arbitrarily. Singularity can't handle this,
    # and can't pass the flag to ignore these errors, and we can't wrap
    # unsquashfs with a shell script because of how it gets mounted into the
    # container under construction along with its libraries (see
    # <https://github.com/apptainer/singularity/issues/6113#issuecomment-901897566>).
    #
    # So we need to make sure to install a downgraded squashfs first.
    #
    # TODO: Singularity has since resolved this on their end
    # https://github.com/sylabs/singularity/pull/267
    # This works by checking that the UID of the caller is not root
    # In a Kubernetes pod, the default setup will have UID 0 even if the pod is unprivileged
    # https://github.com/sylabs/singularity/issues/2727
    # It is possible to avoid this by changing the user of the pod (such as runAsUser: 1000)
    # but this may cause permission issues, ex: changed read/write permissions
    # so the downgraded squashfs stays, but options for updated squashfs are possible
    ADD extra-debs.tsv /etc/singularity/extra-debs.tsv
    RUN wget -q "$(cat /etc/singularity/extra-debs.tsv | grep "^squashfs-tools.$TARGETARCH" | cut -f4)" && \
        dpkg -i squashfs-tools_*.deb && \
        wget -q "$(cat /etc/singularity/extra-debs.tsv | grep "^singularity-container.$TARGETARCH" | cut -f4)" && \
        dpkg -i singularity-container_*.deb && \
        rm singularity-container_*.deb && \
        sed -i 's!bind path = /etc/localtime!#bind path = /etc/localtime!g' /etc/singularity/singularity.conf && \
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

    # Include virtualenv, as it is still the recommended way to deploy pipelines
    RUN {pip} install --upgrade virtualenv==20.25.1

    # Install s3am (--never-download prevents silent upgrades to pip, wheel and setuptools)
    # Install setuptools within the virtual environment to properly access distutils due to PEP 632 and gh-95299 in Python 3.12 release notes
    # https://docs.python.org/3/whatsnew/3.12.html#summary-release-highlights
    RUN virtualenv --python {python} --never-download /home/s3am \
        && /home/s3am/bin/pip install setuptools \
        && /home/s3am/bin/pip install s3am==2.0 \
        && ln -s /home/s3am/bin/s3am /usr/local/bin/
    
    RUN {pip} install --upgrade setuptools==69.2.0

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
    RUN {pip} install {sdistName}[all]
    RUN rm {sdistName}

    # We intentionally inherit the default ENTRYPOINT and CMD from the base image, to the effect
    # that the running appliance just gives you a shell. To start the Mesos daemons, the user
    # should override the entrypoint via --entrypoint.

    RUN echo '[ ! -z "$TERM" -a -r /etc/motd ] && cat /etc/motd' >> /etc/bash.bashrc \
        && printf '{motd}' > /etc/motd
    
    # Final check that Toil actually runs at all in the container
    RUN toil --version
'''))
