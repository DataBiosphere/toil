from setuptools import setup, find_packages

setup(
    name='toil',
    version='3.0.6a1',
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/toil",
    install_requires=[ 'bd2k-python-lib>=1.7.dev1' ],
    tests_require=[ 'mock==1.0.1' ],
    test_suite = 'toil',
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
            'toilKill = toil.utils.toilKill:main',
            'toilStatus = toil.utils.toilStatus:main',
            'toilStats = toil.utils.toilStats:main',
            'toilRestarts = toil.utils.toilRestarts:main',
            'multijob = toil.batchSystems.multijob:main',
            'toil-mesos-executor = toil.batchSystems.mesos.executor:main [mesos]' ] } )
