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
from toil.lib.memoize import sync_memoize


class InnerClass(object):
    """
    Note that this is EXPERIMENTAL code.

    A nested class (the inner class) decorated with this will have an additional attribute called
    'outer' referencing the instance of the nesting class (the outer class) that was used to
    create the inner class. The outer instance does not need to be passed to the inner class's
    constructor, it will be set magically. Shamelessly stolen from

    http://stackoverflow.com/questions/2278426/inner-classes-how-can-i-get-the-outer-class-object-at-construction-time#answer-2278595.

    with names made more descriptive (I hope) and added caching of the BoundInner classes.

    Caveat: Within the inner class, self.__class__ will not be the inner class but a dynamically
    created subclass thereof. It's name will be the same as that of the inner class,
    but its __module__ will be different. There will be one such dynamic subclass per inner class
    and instance of outer class, if that outer class instance created any instances of inner the
    class.

    >>> class Outer(object):
    ...     def new_inner(self):
    ...         # self is an instance of the outer class
    ...         inner = self.Inner()
    ...         # the inner instance's 'outer' attribute is set to the outer instance
    ...         assert inner.outer is self
    ...         return inner
    ...     @InnerClass
    ...     class Inner(object):
    ...         def get_outer(self):
    ...             return self.outer
    ...         @classmethod
    ...         def new_inner(cls):
    ...             return cls()
    >>> o = Outer()
    >>> i = o.new_inner()
    >>> i # doctest: +ELLIPSIS
    <toil.lib.objects.Inner...> bound to <toil.lib.objects.Outer object at ...>

    >>> i.get_outer() # doctest: +ELLIPSIS
    <toil.lib.objects.Outer object at ...>

    Now with inheritance for both inner and outer:

    >>> class DerivedOuter(Outer):
    ...     def new_inner(self):
    ...         return self.DerivedInner()
    ...     @InnerClass
    ...     class DerivedInner(Outer.Inner):
    ...         def get_outer(self):
    ...             assert super( DerivedOuter.DerivedInner, self ).get_outer() == self.outer
    ...             return self.outer
    >>> derived_outer = DerivedOuter()
    >>> derived_inner = derived_outer.new_inner()
    >>> derived_inner # doctest: +ELLIPSIS
    <toil.lib.objects...> bound to <toil.lib.objects.DerivedOuter object at ...>

    >>> derived_inner.get_outer() # doctest: +ELLIPSIS
    <toil.lib.objects.DerivedOuter object at ...>

    Test a static references:
    >>> Outer.Inner # doctest: +ELLIPSIS
    <class 'toil.lib.objects...Inner'>
    >>> DerivedOuter.Inner # doctest: +ELLIPSIS
    <class 'toil.lib.objects...Inner'>
    >>> DerivedOuter.DerivedInner #doctest: +ELLIPSIS
    <class 'toil.lib.objects...DerivedInner'>

    Can't decorate top-level classes. Unfortunately, this is detected when the instance is
    created, not when the class is defined.
    >>> @InnerClass
    ... class Foo(object):
    ...    pass
    >>> Foo()
    Traceback (most recent call last):
    ...
    RuntimeError: Inner classes must be nested in another class.

    All inner instances should refer to a single outer instance:
    >>> o = Outer()
    >>> o.new_inner().outer == o == o.new_inner().outer
    True

    All inner instances should be of the same class ...
    >>> o.new_inner().__class__ == o.new_inner().__class__
    True

    ... but that class isn't the inner class ...
    >>> o.new_inner().__class__ != Outer.Inner
    True

    ... but a subclass of the inner class.
    >>> isinstance( o.new_inner(), Outer.Inner )
    True

    Static and class methods, e.g. should work, too

    >>> o.Inner.new_inner().outer == o
    True
    """

    def __init__(self, inner_class):
        super(InnerClass, self).__init__()
        self.inner_class = inner_class

    # noinspection PyUnusedLocal
    def __get__(self, instance, owner):
        # No need to wrap a static reference, i.e one that is made via 'Outer.' rather than 'self.'
        if instance is None:
            return self.inner_class
        else:
            return self._bind( instance )

    @sync_memoize
    def _bind( self, _outer):
        class BoundInner(self.inner_class):
            outer = _outer

            def __repr__(self):
                return "%s bound to %s" % (super(BoundInner, self).__repr__(), repr(_outer))

        BoundInner.__name__ = self.inner_class.__name__
        BoundInner.__module__ = self.inner_class.__module__
        return BoundInner

    def __call__(*args, **kwargs):
        raise RuntimeError("Inner classes must be nested in another class.")
