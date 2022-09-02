from ..core.topictreetraverser import ITopicTreeVisitor as ITopicTreeVisitor, TopicTreeTraverser as TopicTreeTraverser
from _typeshed import Incomplete
from typing import TextIO

class TopicTreePrinter(ITopicTreeVisitor):
    allowedExtras: Incomplete
    ALL_TOPICS_NAME: str
    def __init__(self, extra: Incomplete | None = ..., width: int = ..., indentStep: int = ..., bulletTopic: str = ..., bulletTopicItem: str = ..., bulletTopicArg: str = ..., fileObj: TextIO = ...) -> None: ...
    def getOutput(self): ...

def printTreeDocs(rootTopic: Incomplete | None = ..., topicMgr: Incomplete | None = ..., **kwargs) -> None: ...
