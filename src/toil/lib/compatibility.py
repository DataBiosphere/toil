import functools
import warnings
from typing import Any, Callable, Union


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
