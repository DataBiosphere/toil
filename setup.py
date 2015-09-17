# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import sys
from version import version
from setuptools import find_packages, setup

kwargs = dict(
    name='toil',
    version=version,
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/toil",
    install_requires=[ 'bd2k-python-lib==1.7.dev1' ],
    tests_require=[ 'mock==1.0.1', 'pytest==2.7.2' ],
    test_suite='toil',
    extras_require={
        'mesos': [
            'mesos.interface==0.22.0',
            'psutil==3.0.1' ],
        'aws': [
            'boto==2.38.0' ],
        'azure': [
            'azure==0.11.1' ],
        'encryption': [
            'pynacl==0.3.0' ]},
    package_dir={ '': 'src' },
    packages=find_packages( 'src', exclude=[ '*.test' ] ),
    entry_points={
        'console_scripts': [
            'toil = toil.utils.toilMain:main',
            'toil-mesos-executor = toil.batchSystems.mesos.executor:main [mesos]' ] } )

from setuptools.command.test import test as TestCommand


class PyTest( TestCommand ):
    user_options = [ ('pytest-args=', 'a', "Arguments to pass to py.test") ]

    def initialize_options( self ):
        TestCommand.initialize_options( self )
        self.pytest_args = [ ]

    def finalize_options( self ):
        TestCommand.finalize_options( self )
        self.test_args = [ ]
        self.test_suite = True

    def run_tests( self ):
        import pytest
        # Sanitize command line arguments to avoid confusing Toil code attempting to parse them
        sys.argv[1:] = []
        errno = pytest.main( self.pytest_args )
        sys.exit( errno )


kwargs[ 'cmdclass' ] = { 'test': PyTest }

setup( **kwargs )
