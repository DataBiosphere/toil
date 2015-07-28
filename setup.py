from setuptools import setup, find_packages

mesos_requirements = []
# only set requirements that are not already satisfied. For some reason, setting requirements that are already satisfied
# causes setup.py to reinstall them. This is bad, especially in cases like in cgcloud mesos right now, when mesos.native and
# mesos.interface are installed via an egg. This then uninstalls mesos.interface, pip installs it, and now there are two
# mesos modules instead of the one we want.
try:
    import mesos.native
    try:
        import mesos.interface
    except:
        mesos_requirements.append('mesos.interface>=0.22.1')
    try:
        import psutil
    except:
        mesos_requirements.append('psutil>=3.0.1')
except:
    pass

setup(
    name='toil',
    version='3.0.3.dev1',
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/toil",
    install_requires = mesos_requirements,
    package_dir={ '': 'src' },
    packages=find_packages( 'src', exclude=[ '*.test' ] ),
    entry_points={
        'console_scripts': [
            'toilKill = toil.utils.toilKill:main',
            'toilStatus = toil.utils.toilStatus:main',
            'toilStats = toil.utils.toilStats:main',
            'toilRestarts = toil.utils.toilRestarts:main',
            'multijob = toil.batchSystems.multijob:main'
        ],
    })
