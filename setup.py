from setuptools import setup, find_packages

setup(
    name='toil',
    version='3.0.3.dev6',
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/toil",
    install_requires=[ ],
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
            'multijob = toil.batchSystems.multijob:main' ] } )
