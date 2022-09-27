from enum import IntEnum

from _typeshed import Incomplete

from .topicobj import Topic

class ITopicTreeVisitor: ...

class TreeTraversal(IntEnum):
    DEPTH: Incomplete
    BREADTH: Incomplete
    MAP: Incomplete

class TopicTreeTraverser:
    def __init__(self, visitor: ITopicTreeVisitor = ...) -> None: ...
    def setVisitor(self, visitor: ITopicTreeVisitor) -> None: ...
    def traverse(
        self, topicObj: Topic, how: TreeTraversal = ..., onlyFiltered: bool = ...
    ) -> None: ...
