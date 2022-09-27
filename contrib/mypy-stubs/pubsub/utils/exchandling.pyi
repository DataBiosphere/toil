from typing import List

from _typeshed import Incomplete

from ..core.listener import IListenerExcHandler
from ..core.topicmgr import TopicManager

class TracebackInfo:
    ExcClass: Incomplete
    excArg: Incomplete
    traceback: Incomplete
    def __init__(self) -> None: ...
    def getFormattedList(self) -> List[str]: ...
    def getFormattedString(self) -> str: ...

class ExcPublisher(IListenerExcHandler):
    topicUncaughtExc: str
    def __init__(self, topicMgr: TopicManager = ...) -> None: ...
    def init(self, topicMgr: TopicManager): ...
    def __call__(self, listenerID: str, topicObj): ...
