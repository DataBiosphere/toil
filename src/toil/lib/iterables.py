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

# 5.14.2018: copied into Toil from https://github.com/BD2KGenomics/bd2k-python-lib



def flatten( iterables ):
    """ Flatten an iterable, except for string elements. """
    for it in iterables:
        if isinstance(it, str):
            yield it
        else:
            for element in it:
                yield element

# noinspection PyPep8Naming
class concat(object):
    """
    A literal iterable that lets you combine sequence literals (lists, set) with generators or list
    comprehensions. Instead of

    >>> [ -1 ] + [ x * 2 for x in range( 3 ) ] + [ -1 ]
    [-1, 0, 2, 4, -1]

    you can write

    >>> list( concat( -1, ( x * 2 for x in range( 3 ) ), -1 ) )
    [-1, 0, 2, 4, -1]

    This is slightly shorter (not counting the list constructor) and does not involve array
    construction or concatenation.

    Note that concat() flattens (or chains) all iterable arguments into a single result iterable:

    >>> list( concat( 1, range( 2, 4 ), 4 ) )
    [1, 2, 3, 4]

    It only does so one level deep. If you need to recursively flatten a data structure,
    check out crush().

    If you want to prevent that flattening for an iterable argument, wrap it in concat():

    >>> list( concat( 1, concat( range( 2, 4 ) ), 4 ) )
    [1, range(2, 4), 4]

    Some more example.

    >>> list( concat() ) # empty concat
    []
    >>> list( concat( 1 ) ) # non-iterable
    [1]
    >>> list( concat( concat() ) ) # empty iterable
    []
    >>> list( concat( concat( 1 ) ) ) # singleton iterable
    [1]
    >>> list( concat( 1, concat( 2 ), 3 ) ) # flattened iterable
    [1, 2, 3]
    >>> list( concat( 1, [2], 3 ) ) # flattened iterable
    [1, 2, 3]
    >>> list( concat( 1, concat( [2] ), 3 ) ) # protecting an iterable from being flattened
    [1, [2], 3]
    >>> list( concat( 1, concat( [2], 3 ), 4 ) ) # protection only works with a single argument
    [1, 2, 3, 4]
    >>> list( concat( 1, 2, concat( 3, 4 ), 5, 6 ) )
    [1, 2, 3, 4, 5, 6]
    >>> list( concat( 1, 2, concat( [ 3, 4 ] ), 5, 6 ) )
    [1, 2, [3, 4], 5, 6]

    Note that while strings are technically iterable, concat() does not flatten them.

    >>> list( concat( 'ab' ) )
    ['ab']
    >>> list( concat( concat( 'ab' ) ) )
    ['ab']
    """

    def __init__( self, *args ):
        super( concat, self ).__init__( )
        self.args = args

    def __iter__( self ):
        def expand( x ):
            if isinstance( x, concat ) and len( x.args ) == 1:
                i = x.args
            elif not isinstance(x, str):
                try:
                    i = x.__iter__( )
                except AttributeError:
                    i = x,
            else:
                i = x
            return i

        return flatten( map( expand, self.args ) )
