# Copyright (C) 2015-2018 Regents of the University of California
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

# 5.14.2018: copied into Toil from https://github.com/BD2KGenomics/bd2k-python-lib

from __future__ import absolute_import
from future.utils import raise_
from builtins import object
import sys


class panic( object ):
    """
    The Python idiom for reraising a primary exception fails when the except block raises a
    secondary exception, e.g. while trying to cleanup. In that case the original exception is
    lost and the secondary exception is reraised. The solution seems to be to save the primary
    exception info as returned from sys.exc_info() and then reraise that.

    This is a contextmanager that should be used like this

    try:
         # do something that can fail
    except:
        with panic( log ):
            # do cleanup that can also fail

    If a logging logger is passed to panic(), any secondary Exception raised within the with
    block will be logged. Otherwise those exceptions are swallowed. At the end of the with block
    the primary exception will be reraised.
    """

    def __init__( self, log=None ):
        super( panic, self ).__init__( )
        self.log = log
        self.exc_info = None

    def __enter__( self ):
        self.exc_info = sys.exc_info( )

    def __exit__( self, *exc_info ):
        if self.log is not None and exc_info and exc_info[ 0 ]:
            self.log.warn( "Exception during panic", exc_info=exc_info )
        exc_type, exc_value, traceback = self.exc_info
        raise_(exc_type, exc_value, traceback)
