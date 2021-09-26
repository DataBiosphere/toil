import functools
import logging

from typing import Union, Callable, Any

logger = logging.getLogger(__name__)


def deprecated(new_function_name: str) -> Callable[..., Any]:
    def decorate(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def call(*args: Any, **kwargs: Any) -> Any:
            logger.warning(f'WARNING: "{func.__name__}()" is deprecated.  Please use "{new_function_name}()" instead.')
            return func(*args, **kwargs)
        return call
    return decorate


def compat_bytes(s: Union[bytes, str]) -> str:
    return s.decode('utf-8') if isinstance(s, bytes) else s
