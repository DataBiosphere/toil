from setuptools import setup, find_packages

setup(
    name='jobTree',
    version='2.0.dev1',
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/jobTree",
    
    package_dir={ '': 'src' },
    packages=find_packages( 'src', exclude=[ '*.test' ] ),
    entry_points={
        'console_scripts': [
            'jobTreeKill = jobTree.utils.jobTreeKill:main',
            'jobTreeStatus = jobTree.utils.jobTreeStatus:main',
            'jobTreeStats = jobTree.utils.jobTreeStats:main',
            'jobTreeRestarts = jobTree.utils.jobTreeRestarts:main',
            'multijob = jobTree.batchSystems.multijob:main'
        ],
    })
