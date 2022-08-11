from .topicobj import Topic as Topic
from _typeshed import Incomplete
from enum import IntEnum

class ITopicTreeVisitor: ...

class TreeTraversal(IntEnum):
    DEPTH: Incomplete
    BREADTH: Incomplete
    MAP: Incomplete

class TopicTreeTraverser:
    def __init__(self, visitor: ITopicTreeVisitor = ...) -> None: ...
    def setVisitor(self, visitor: ITopicTreeVisitor): ...
    def traverse(self, topicObj: Topic, how: TreeTraversal = ..., onlyFiltered: bool = ...): ...
