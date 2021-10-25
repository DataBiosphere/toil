import imp
import os

from tempfile import NamedTemporaryFile

def get_requirements(extra=None):
    """
    Load the requirements for the given extra from the appropriate
    requirements-extra.txt, or the main requirements.txt if no extra is
    specified.
    """

    filename = f"requirements-{extra}.txt" if extra else "requirements.txt"

    with open(filename) as fp:
        # Parse out as one per line
        return [l.strip() for l in fp.readlines() if l.strip()]


def import_version():
    """Return the module object for src/toil/version.py, generate from the template if required."""
    if not os.path.exists('src/toil/version.py'):
        for req in get_requirements("cwl"):
            # Determine cwltool version from requirements file
            if req.startswith("cwltool=="):
                cwltool_version = req[len("cwltool=="):]
                break
        # Use the template to generate src/toil/version.py
        import version_template
        with NamedTemporaryFile(mode='w', dir='src/toil', prefix='version.py.', delete=False) as f:
            f.write(version_template.expand_(others={
                # expose the dependency versions that we may need to access in Toil
                'cwltool_version': cwltool_version,
            }))
        os.rename(f.name, 'src/toil/version.py')

    # Unfortunately, we can't use a straight import here because that would also load the stuff
    # defined in "src/toil/__init__.py" which imports modules from external dependencies that may
    # yet to be installed when setup.py is invoked.
    #
    # This is also the reason we cannot switch from the "deprecated" imp library
    # and use:
    #     from importlib.machinery import SourceFileLoader
    #     return SourceFileLoader('toil.version', path='src/toil/version.py').load_module()
    #
    # Because SourceFileLoader will error and load "src/toil/__init__.py" .
    return imp.load_source('toil.version', 'src/toil/version.py')

if __name__ == '__main__':
    version = import_version()