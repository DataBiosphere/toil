from collections.abc import Generator
from typing import Any, Callable, TypeVar, Protocol, overload, Literal

PYDOC_RETURN_LABEL: str
PYDOC_FOLLOW_PARAM: str
TYPE_LIST_SUFFIX: str

CovItemT = TypeVar("CovItemT", covariant=True)

class _WatchEvent(Protocol[CovItemT]):
    """
    An event from a Kubernetes watch stream.
    See https://github.com/kubernetes-client/python/blob/1271465acdb80bf174c50564a384fd6898635ea6/kubernetes/base/watch/watch.py#L130-L136.
    """

    # TODO: this can't be a TypedDict until we drop Python 3.10 because of
    # https://stackoverflow.com/a/71945477 so we go with an overloaded
    # __getitem__ instead.

    @overload
    def __getitem__(self, name: Literal["type"]) -> str: ...
    @overload
    def __getitem__(self, name: Literal["object"]) -> "CovItemT": ...
    @overload
    def __getitem__(self, name: Literal["raw_object"]) -> dict[str, Any]: ...

class Watch[CovItemT]:
    def __init__(self, return_type: Any = None) -> None: ...
    # def stop(self) -> None: ...
    # def get_return_type(self, func): ...
    # def get_watch_argument_name(self, func): ...
    # def unmarshal_event(self, data, return_type): ...
    def stream(
        self, func: Callable[[Any], Any], *args: Any, **kwargs: Any
    ) -> Generator[_WatchEvent[CovItemT]]: ...
