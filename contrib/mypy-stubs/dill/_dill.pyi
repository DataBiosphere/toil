import sys
from typing import IO, Any, Callable, Iterable, Protocol, Union

from _typeshed import ReadableBuffer
from typing_extensions import TypeAlias, final

class _ReadableFileobj(Protocol):
    def read(self, __n: int) -> bytes: ...
    def readline(self) -> bytes: ...

class _WritableFileobj(Protocol):
    def write(self, __b: bytes) -> Any: ...

if sys.version_info >= (3, 8):
    @final
    class PickleBuffer:
        def __init__(self, buffer: ReadableBuffer) -> None: ...
        def raw(self) -> memoryview: ...
        def release(self) -> None: ...
    _BufferCallback: TypeAlias = Callable[[PickleBuffer], Any] | None

    def dump(
        obj: Any,
        file: _WritableFileobj,
        protocol: int | None = ...,
        *,
        fix_imports: bool = ...,
        buffer_callback: _BufferCallback = ...,
    ) -> None: ...
    def dumps(
        obj: Any,
        protocol: int | None = ...,
        *,
        fix_imports: bool = ...,
        buffer_callback: _BufferCallback = ...,
    ) -> bytes: ...
    def load(
        file: _ReadableFileobj,
        *,
        fix_imports: bool = ...,
        encoding: str = ...,
        errors: str = ...,
        buffers: Iterable[Any] | None = ...,
    ) -> Any: ...
    def loads(
        __data: ReadableBuffer,
        *,
        fix_imports: bool = ...,
        encoding: str = ...,
        errors: str = ...,
        buffers: Iterable[Any] | None = ...,
    ) -> Any: ...

else:
    def dump(
        obj: Any,
        file: _WritableFileobj,
        protocol: int | None = ...,
        *,
        fix_imports: bool = ...,
    ) -> None: ...
    def dumps(
        obj: Any, protocol: int | None = ..., *, fix_imports: bool = ...
    ) -> bytes: ...
    def load(
        file: _ReadableFileobj,
        *,
        fix_imports: bool = ...,
        encoding: str = ...,
        errors: str = ...,
    ) -> Any: ...
    def loads(
        data: ReadableBuffer,
        *,
        fix_imports: bool = ...,
        encoding: str = ...,
        errors: str = ...,
    ) -> Any: ...
