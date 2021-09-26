import functools
import logging

from typing import Union

logger = logging.getLogger(__name__)


def deprecated(new_function_name):
    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            logger.warning(f'WARNING: "{func.__name__}()" is deprecated.  Please use "{new_function_name}()" instead.')
            return func(*args, **kwargs)
        return call
    return decorate


def compat_bytes(s: Union[bytes, str]) -> str:
    return s.decode('utf-8') if isinstance(s, bytes) else s
