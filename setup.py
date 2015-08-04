import importlib

from setuptools import setup, find_packages

install_requires = [ ]


def __try_import( name ):
    try:
        importlib.import_module( name )
    except ImportError:
        print "Failed to import '%s'" % name
        return 0
    else:
        print "Successfully imported '%s'" % name
        return 1


# The mesos.native package can't be installed solely via pip, it must be installed by installing
# Mesos as a whole. And while mesos.interface *can* be installed via pip, it is not sufficient
# for our purposes. We therefore don't explicitly depend on neither mesos.native nor
# mesos.interface. Instead, we require them both to be absent or present. In the latter case,
# we dynamically add the dependencies we need to use Mesos.

mesos_imports = __try_import( 'mesos.native' ) + __try_import( 'mesos.interface' )

if mesos_imports == 0:
    pass
elif mesos_imports == 1:
    raise RuntimeError( "Please install matching versions of mesos.native and mesos.interface. "
                        "One of them is missing at the moment." )
elif mesos_imports == 2:
    install_requires.append( 'psutil>=3.0.1' )
else:
    raise RuntimeError( "Internal error." )

setup(
    name='toil',
    version='3.0.3.dev1',
    description='Pipeline management software for clusters.',
    author='Benedict Paten',
    author_email='benedict@soe.usc.edu',
    url="https://github.com/BD2KGenomics/toil",
    install_requires=install_requires,
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
    } )
