import functools
import warnings
from typing import Any, Dict, Callable, Union, TypeVar, overload


def deprecated(new_function_name: str) -> Callable[..., Any]:
    def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def call(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(f'WARNING: "{func.__name__}()" is deprecated.  Please use "{new_function_name}()" instead.',
                          DeprecationWarning)
            return func(*args, **kwargs)
        return call
    return decorate


def compat_bytes(s: Union[bytes, str]) -> str:
    return s.decode('utf-8') if isinstance(s, bytes) else s

# MyPy can't yet support the recursive type we would need to say "we go through
# any structure of dicts, tuples, lists, and sets and convert all bytes types
# in the keys and values to strings". I also can't work out how to make a
# TypeVar T that represents a generic where I could say wer go from T[bytes] ->
# T[str].
def compat_bytes_recursive(data: Any) -> Any:
    """
    Convert a tree of objects over bytes to objects over strings.
    """
    if isinstance(data, dict):
        # Keyed collection
        return type(data)((compat_bytes_recursive(i) for i in data.items()))
    elif isinstance(data, (tuple, list, set)):
        # Flat collection
        return type(data)((compat_bytes_recursive(i) for i in data))
    elif isinstance(data, bytes):
        # Leaf bytes
        return data.decode('utf-8')
    else:
        # Leaf non-bytes
        return data
