#!/usr/bin/env python

from setuptools import setup
import subprocess
import distutils.command.build_py

class BuildWithMake(distutils.command.build_py.build_py):
    """
    Build using make.
    Then do the default build logic.
    
    """
    def run(self):
        # Call make.
        subprocess.check_call(["make"])
        
        # Keep installing the Python stuff
        distutils.command.build_py.build_py.run(self)


setup(name="jobTree",
    version="1.0",
    description="Pipeline management software for clusters.",
    author="Benedict Paten",
    author_email="benedict@soe.ucsc.edu",
    url="http://hgwdev.cse.ucsc.edu/~benedict/code/jobTree.html",
    packages=["jobTree", "jobTree.src", "jobTree.test", 
    "jobTree.test.sort", "jobTree.batchSystems", "jobTree.scriptTree"],
    install_requires=["sonLib"],
    # Hook the build command to also build with make
    cmdclass={"build_py": BuildWithMake},
    # Install all the executable scripts somewhere on the PATH
    scripts=["bin/jobTreeKill", "bin/jobTreeStatus", 
    "bin/scriptTreeTest_Sort.py", "bin/jobTreeRun", 
    "bin/jobTreeTest_Dependencies.py", "bin/scriptTreeTest_Wrapper.py", 
    "bin/jobTreeStats", "bin/multijob", "bin/scriptTreeTest_Wrapper2.py"])

    
