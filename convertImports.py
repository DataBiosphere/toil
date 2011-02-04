#!/usr/bin/env python

"""Script to change the imports to the new jobtree module structure.
"""

import os
import sys
from lib.bioio import system

def fn2(file, oldPath, newPath):
    system("sed 's/%s/%s/g' %s > temporaryConversionFile" % (oldPath, newPath, file))
    system("mv temporaryConversionFile %s" % file)

def fn(dir):
    for i in os.listdir(dir):
        i = os.path.join(dir, i)
        if os.path.isdir(i):
            print "Entering directory", i
            fn(i)
        else:
            assert os.path.isfile(i)
            if i[-3:] == ".py" and "convertImports.py" not in i:
                print "Modifying", i
                fn2(i, "workflow\.jobTree", "jobTree") #Get rid of the 'workflow' bit
                fn2(i, "jobTree\.bin", "jobTree.lib")
            else:
                print "Not modifying", i
    
fn(sys.argv[1])
        

