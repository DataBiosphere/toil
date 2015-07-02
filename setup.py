from setuptools import setup, find_packages

mesos_requirements = []
try:
    import mesos.native
    mesos_requirements = ['psutil>=3.0.1', 'mesos.interface>=0.22.1']
except:
    pass

setup(
    name='jobTree',
    version='3.0.3.dev1',
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/jobTree",
    install_requires = mesos_requirements,
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
