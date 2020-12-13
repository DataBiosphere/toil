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


class Expando(dict):
    """
    Pass inital attributes to the constructor:

    >>> o = Expando(foo=42)
    >>> o.foo
    42

    Dynamically create new attributes:

    >>> o.bar = 'hi'
    >>> o.bar
    'hi'

    Expando is a dictionary:

    >>> isinstance(o,dict)
    True
    >>> o['foo']
    42

    Works great with JSON:

    >>> import json
    >>> s='{"foo":42}'
    >>> o = json.loads(s,object_hook=Expando)
    >>> o.foo
    42
    >>> o.bar = 'hi'
    >>> o.bar
    'hi'

    And since Expando is a dict, it serializes back to JSON just fine:

    >>> json.dumps(o, sort_keys=True)
    '{"bar": "hi", "foo": 42}'

    Attributes can be deleted, too:

    >>> o = Expando(foo=42)
    >>> o.foo
    42
    >>> del o.foo
    >>> o.foo
    Traceback (most recent call last):
    ...
    AttributeError: 'Expando' object has no attribute 'foo'
    >>> o['foo']
    Traceback (most recent call last):
    ...
    KeyError: 'foo'

    >>> del o.foo
    Traceback (most recent call last):
    ...
    AttributeError: foo

    And copied:

    >>> o = Expando(foo=42)
    >>> p = o.copy()
    >>> isinstance(p,Expando)
    True
    >>> o == p
    True
    >>> o is p
    False

    Same with MagicExpando ...

    >>> o = MagicExpando()
    >>> o.foo.bar = 42
    >>> p = o.copy()
    >>> isinstance(p,MagicExpando)
    True
    >>> o == p
    True
    >>> o is p
    False

    ... but the copy is shallow:

    >>> o.foo is p.foo
    True
    """

    def __init__( self, *args, **kwargs ):
        super( Expando, self ).__init__( *args, **kwargs )
        self.__slots__ = None
        self.__dict__ = self

    def copy(self):
        return type(self)(self)

class MagicExpando(Expando):
    """
    Use MagicExpando for chained attribute access. The first time a missing attribute is
    accessed, it will be set to a new child MagicExpando.

    >>> o=MagicExpando()
    >>> o.foo = 42
    >>> o
    {'foo': 42}
    >>> o.bar.hello = 'hi'
    >>> o.bar
    {'hello': 'hi'}
    """
    def __getattribute__( self, name ):
        try:
            return super( Expando, self ).__getattribute__( name )
        except AttributeError:
            child = self.__class__( )
            self[name] = child
            return child
