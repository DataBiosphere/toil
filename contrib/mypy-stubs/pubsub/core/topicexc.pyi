from typing import Sequence

from _typeshed import Incomplete

from .annotations import annotationType

class Topic: ...

class TopicNameError(ValueError):
    def __init__(self, name: str, msg: str) -> None: ...

class TopicDefnError(RuntimeError):
    def __init__(self, topicNameTuple: Sequence[str]) -> None: ...

class MessageDataSpecError(RuntimeError):
    def __init__(self, msg: str, args: Sequence[str]) -> None: ...

class ExcHandlerError(RuntimeError):
    badExcListenerID: Incomplete
    exc: Incomplete
    def __init__(
        self, badExcListenerID: str, topicObj: Topic, origExc: Exception = ...
    ) -> None: ...

class UnrecognizedSourceFormatError(ValueError):
    def __init__(self) -> None: ...
