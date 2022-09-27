from _typeshed import Incomplete

from .core import (
    ALL_TOPICS,
    AUTO_TOPIC,
    TOPIC_TREE_FROM_CLASS,
    TOPIC_TREE_FROM_MODULE,
    TOPIC_TREE_FROM_STRING,
    ExcHandlerError,
    IListenerExcHandler,
    ListenerMismatchError,
    Topic,
    TopicManager,
    TopicNameError,
    TopicTreeTraverser,
    exportTopicTreeSpec,
)

subscribe: Incomplete
unsubscribe: Incomplete
unsubAll: Incomplete
sendMessage: Incomplete
getListenerExcHandler: Incomplete
setListenerExcHandler: Incomplete
addNotificationHandler: Incomplete
clearNotificationHandlers: Incomplete
setNotificationFlags: Incomplete
getNotificationFlags: Incomplete
setTopicUnspecifiedFatal: Incomplete
topicTreeRoot: Incomplete
topicsMap: Incomplete

def isValid(listener, topicName, curriedArgNames: Incomplete | None = ...) -> bool: ...
def validate(listener, topicName, curriedArgNames: Incomplete | None = ...) -> None: ...
def isSubscribed(listener, topicName) -> bool: ...
def getDefaultTopicMgr() -> TopicManager: ...

addTopicDefnProvider: Incomplete
clearTopicDefnProviders: Incomplete
getNumTopicDefnProviders: Incomplete

# Names in __all__ with no definition:
#   instantiateAllDefinedTopicsTopicDefnError
