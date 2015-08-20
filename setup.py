import sys

from setuptools import find_packages, setup

kwargs = dict(
    name='toil',
    version='3.0.6a1',
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
            'boto==2.38.0' ] },
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
        errno = pytest.main( self.pytest_args )
        sys.exit( errno )


kwargs[ 'cmdclass' ] = { 'test': PyTest }

setup( **kwargs )
