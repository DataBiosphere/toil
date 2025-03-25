# Copyright (C) 2015-2021 Regents of the University of California
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
import os
import types
from importlib.machinery import SourceFileLoader
from tempfile import NamedTemporaryFile

from setuptools import find_packages, setup

SETUP_DIR = os.path.dirname(__file__)
README = os.path.join(SETUP_DIR, "README.rst")


def get_requirements(extra=None):
    """
    Load the requirements for the given extra.

    Uses the appropriate requirements-extra.txt, or the main requirements.txt
    if no extra is specified.
    """
    filename = f"requirements-{extra}.txt" if extra else "requirements.txt"

    with open(filename) as fp:
        # Parse out as one per line, dropping comments
        return [
            l.split("#")[0].strip() for l in fp.readlines() if l.split("#")[0].strip()
        ]


def run_setup():
    """
    Call setup().

    This function exists so the setup() invocation preceded more internal functionality.
    The `version` module is imported dynamically by import_version() below.
    """
    install_requires = get_requirements()

    extras_require = {}
    # htcondor is not supported by apple
    # this is tricky to conditionally support in 'all' due
    # to how wheels work, so it is not included in all and
    # must be explicitly installed as an extra
    all_reqs = []

    non_htcondor_extras = [
        "aws",
        "cwl",
        "encryption",
        "google",
        "kubernetes",
        "wdl",
        "server",
    ]
    for extra in non_htcondor_extras:
        extras_require[extra] = get_requirements(extra)
        all_reqs += extras_require[extra]
    # We exclude htcondor from "all" because it can't be on Mac
    extras_require['htcondor:sys_platform!="darwin"'] = get_requirements("htcondor")
    extras_require["mesos"] = get_requirements("mesos")
    all_reqs += get_requirements("mesos")
    extras_require["all"] = all_reqs
    setup(
        name="toil",
        version=version.distVersion,
        long_description=open(README).read(),
        long_description_content_type="text/x-rst",
        description="Pipeline management software for clusters.",
        author="Benedict Paten and the Toil community",
        author_email="toil-community@googlegroups.com",
        url="https://github.com/DataBiosphere/toil",
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Environment :: Console",
            "Intended Audience :: Developers",
            "Intended Audience :: Science/Research",
            "Intended Audience :: Healthcare Industry",
            "License :: OSI Approved :: Apache Software License",
            "Natural Language :: English",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: POSIX",
            "Operating System :: POSIX :: Linux",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: Python :: 3.13",
            "Topic :: Scientific/Engineering",
            "Topic :: Scientific/Engineering :: Bio-Informatics",
            "Topic :: Scientific/Engineering :: Astronomy",
            "Topic :: Scientific/Engineering :: Atmospheric Science",
            "Topic :: Scientific/Engineering :: Information Analysis",
            "Topic :: Scientific/Engineering :: Medical Science Apps.",
            "Topic :: System :: Distributed Computing",
            "Topic :: Utilities",
        ],
        license="Apache License v2.0",
        python_requires=">=3.9",
        install_requires=install_requires,
        extras_require=extras_require,
        package_dir={"": "src"},
        packages=find_packages(where="src"),
        include_package_data=True,
        # Unfortunately, the names of the entry points are hard-coded elsewhere in the code base so
        # you can't just change them here. Luckily, most of them are pretty unique strings, and thus
        # easy to search for.
        entry_points={
            "console_scripts": [
                "toil = toil.utils.toilMain:main",
                "_toil_worker = toil.worker:main",
                "cwltoil = toil.cwl.cwltoil:cwltoil_was_removed [cwl]",
                "toil-cwl-runner = toil.cwl.cwltoil:main [cwl]",
                "toil-wdl-runner = toil.wdl.wdltoil:main [wdl]",
                "toil-wes-cwl-runner = toil.server.cli.wes_cwl_runner:main [server]",
                "_toil_mesos_executor = toil.batchSystems.mesos.executor:main [mesos]",
                "_toil_contained_executor = toil.batchSystems.contained_executor:executor",
            ]
        },
    )


def import_version():
    """Return the module object for src/toil/version.py, generate from the template if required."""
    if not os.path.exists("src/toil/version.py"):
        for req in get_requirements("cwl"):
            # Determine cwltool version from requirements file
            if req.startswith("cwltool=="):
                cwltool_version = req[len("cwltool==") :]
                break
        # Use the template to generate src/toil/version.py
        import version_template

        with NamedTemporaryFile(
            mode="w", dir="src/toil", prefix="version.py.", delete=False
        ) as f:
            f.write(
                version_template.expand_(
                    others={
                        # expose the dependency versions that we may need to access in Toil
                        "cwltool_version": cwltool_version,
                    }
                )
            )
        os.rename(f.name, "src/toil/version.py")

    loader = SourceFileLoader("toil.version", "src/toil/version.py")
    mod = types.ModuleType(loader.name)
    loader.exec_module(mod)
    return mod


version = import_version()
run_setup()
