# Copyright (C) 2015-2016 Regents of the University of California
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

import argparse
import textwrap

parser = argparse.ArgumentParser()
parser.add_argument('--sdist', required=True)
parser.add_argument('--self', required=True)
options = parser.parse_args()

dependencies = ' '.join(['libffi-dev',  # For client side encryption for 'azure' extra with PyNACL
                         'python-dev',  # For installing Python packages with native code
                         'python-pip',  # Bootstrap pip, but needs upgrading, see below
                         'libcurl4-openssl-dev',
                         'libssl-dev',
                         'wget',
                         'curl',
                         'mesos=1.0.0-2.0.89.ubuntu1404'])


def heredoc(s):
    s = textwrap.dedent(s).format(**globals())
    return s[1:] if s.startswith('\n') else s


motd = heredoc('''

    This is the Toil appliance. You can run your Toil script directly on the appliance, but only
    in single-machine mode. Alternatively, create a Toil cluster with `toil launch-cluster`,
    log into the leader of that cluster with `toil ssh-cluster` and run your Toil script there.

    For more information see http://toil.readthedocs.io/en/latest/

    Copyright (C) 2015-2016 Regents of the University of California

    Version: {options.self}

''')

# Prepare motd to be echoed in the Dockerfile using a RUN statement that uses bash's print
motd = ''.join(l + '\\n\\\n' for l in motd.splitlines())

print heredoc('''
    FROM ubuntu:14.04

    RUN echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" \
        > /etc/apt/sources.list.d/mesosphere.list \
        && apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF \
        && apt-get -y update \
        && apt-get -y install {dependencies} \
        && apt-get clean && rm -rf /var/lib/apt/lists/*

    # The stock pip is too old and can't install from sdist with extras
    RUN pip install --upgrade pip==8.1.2

    # Include virtualenv, as it is still the recommended way to deploy pipelines
    RUN pip install --upgrade virtualenv==15.0.3

    # Install s3am (--never-download prevents silent upgrades to pip, wheel and setuptools)
    RUN virtualenv --never-download /home/s3am \
        && /home/s3am/bin/pip install s3am==2.0 \
        && ln -s /home/s3am/bin/s3am /usr/local/bin/

    # Install statically linked version of docker client
    RUN wget -O /usr/bin/docker https://get.docker.com/builds/Linux/x86_64/docker-1.10.3 \
        && chmod +x /usr/bin/docker
    
    # Fix for Mesos interface dependency missing on ubuntu
    RUN pip install protobuf==3.0.0

    # Move the Mesos module onto the Python path
    RUN ln -s /usr/lib/python2.7/site-packages/mesos /usr/local/lib/python2.7/dist-packages/mesos

    # Fix for https://issues.apache.org/jira/browse/MESOS-3793
    ENV MESOS_LAUNCHER=posix

    # An appliance may need to start more appliances, e.g. when the leader appliance launches the
    # worker appliance on a worker node. To support this, we embed a self-reference into the image:
    ENV TOIL_APPLIANCE_SELF {options.self}

    # This component changes most frequently and keeping it last maximizes Docker cache hits.
    COPY {options.sdist} .
    RUN pip install {options.sdist}[aws,mesos,encryption,cwl]
    RUN rm {options.sdist}

    # We intentionally inherit the default ENTRYPOINT and CMD from the base image, to the effect
    # that the running appliance just gives you a shell. To start the Mesos master or slave
    # daemons, the user # should override the entrypoint via --entrypoint.

    RUN echo '[ ! -z "$TERM" -a -r /etc/motd ] && cat /etc/motd' >> /etc/bash.bashrc \
        && printf '{motd}' > /etc/motd
''')
