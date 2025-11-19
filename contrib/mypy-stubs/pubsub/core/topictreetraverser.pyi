from enum import IntEnum
from typing import Optional

from .topicobj import Topic as Topic

class ITopicTreeVisitor: ...

class TreeTraversal(IntEnum):
    DEPTH = 0
    BREADTH = 1
    MAP = 2

class TopicTreeTraverser:
    def __init__(self, visitor: Optional[ITopicTreeVisitor] = None) -> None: ...
    def setVisitor(self, visitor: ITopicTreeVisitor) -> None: ...
    def traverse(
        self, topicObj: Topic, how: TreeTraversal = ..., onlyFiltered: bool = True
    ) -> None: ...
