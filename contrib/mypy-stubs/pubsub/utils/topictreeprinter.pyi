from typing import TextIO

from _typeshed import Incomplete

from ..core.topictreetraverser import ITopicTreeVisitor, TopicTreeTraverser

class TopicTreePrinter(ITopicTreeVisitor):
    allowedExtras: Incomplete
    ALL_TOPICS_NAME: str
    def __init__(
        self,
        extra: Incomplete | None = ...,
        width: int = ...,
        indentStep: int = ...,
        bulletTopic: str = ...,
        bulletTopicItem: str = ...,
        bulletTopicArg: str = ...,
        fileObj: TextIO = ...,
    ) -> None: ...
    def getOutput(self): ...

def printTreeDocs(
    rootTopic: Incomplete | None = ..., topicMgr: Incomplete | None = ..., **kwargs
) -> None: ...
