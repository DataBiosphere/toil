import argparse
import textwrap

parser = argparse.ArgumentParser()
parser.add_argument('--role', required=True, choices=('leader', 'worker'))
parser.add_argument('--sdist', required=True )
parser.add_argument('--self', required=True )
options = parser.parse_args()

mesos_role = dict(leader='master', worker='slave')[options.role]

dependencies = ' '.join(['libffi-dev',   # For client side encryption for 'azure' extra with PyNACL
                         'python-dev',   # For installing Python packages with native code
                         'python-pip'])  # Bootstrap pip, but needs upgrading, see below

print textwrap.dedent('''
    FROM mesosphere/mesos-{mesos_role}:1.0.0

    RUN apt-get update && apt-get install -y {dependencies}

    # The stock pip is too old and can't install from sdist with extras
    RUN pip install --upgrade pip

    # Mesos interface dependency missing on ubuntu
    RUN pip install protobuf==3.0.0

    COPY {options.sdist} .
    RUN pip install {options.sdist}[aws,mesos,encryption,cwl]

    # Move the Mesos module onto the python path
    RUN ln -s /usr/lib/python2.7/site-packages/mesos /usr/local/lib/python2.7/dist-packages/mesos

    # Fix for https://issues.apache.org/jira/browse/MESOS-3793
    ENV MESOS_LAUNCHER=posix

    # An appliance may need to start more appliances, e.g. when the leader appliance launches the
    # worker appliance on a worker node. To support this, we embed a self-reference into the image:
    ENV TOIL_APPLIANCE_SELF {options.self}
'''.format(**locals())).lstrip()
