from types import ModuleType
from typing import Any, Callable, Literal, Mapping, Sequence

from _typeshed import Incomplete

from .callables import CallArgsInfo as CallArgsInfo
from .callables import ListenerMismatchError as ListenerMismatchError
from .callables import UserListener as UserListener

class Topic: ...

class IListenerExcHandler:
    def __call__(self, listenerID: str, topicObj: Topic) -> None: ...

class Listener:
    AUTO_TOPIC: Incomplete
    acceptsAllKwargs: Incomplete
    curriedArgs: Incomplete
    def __init__(
        self,
        callable_obj: UserListener,
        argsInfo: CallArgsInfo,
        curriedArgs: Mapping[str, Any] = ...,
        onDead: Callable[[Listener], None] = ...,
    ) -> None: ...
    def name(self) -> str: ...
    def typeName(self) -> str: ...
    def module(self) -> ModuleType: ...
    def getCallable(self) -> UserListener: ...
    def isDead(self) -> bool: ...
    def wantsTopicObjOnCall(self) -> bool: ...
    def wantsAllMessageData(self) -> bool: ...
    def setCurriedArgs(self, **curriedArgs: Any) -> None: ...
    def __call__(
        self,
        kwargs: Mapping[str, Any],
        actualTopic: Topic,
        allKwargs: Mapping[str, Any] = ...,
    ) -> Literal[True]: ...

class ListenerValidator:
    def __init__(
        self, topicArgs: Sequence[str], topicKwargs: Sequence[str]
    ) -> None: ...
    def validate(
        self, listener: UserListener, curriedArgNames: Sequence[str] = ...
    ) -> CallArgsInfo: ...
    def isValid(
        self, listener: UserListener, curriedArgNames: Sequence[str] = ...
    ) -> bool: ...
