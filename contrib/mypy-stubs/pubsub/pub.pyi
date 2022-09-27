from .core import ALL_TOPICS as ALL_TOPICS, AUTO_TOPIC as AUTO_TOPIC, ExcHandlerError as ExcHandlerError, IListenerExcHandler as IListenerExcHandler, ListenerMismatchError as ListenerMismatchError, TOPIC_TREE_FROM_CLASS as TOPIC_TREE_FROM_CLASS, TOPIC_TREE_FROM_MODULE as TOPIC_TREE_FROM_MODULE, TOPIC_TREE_FROM_STRING as TOPIC_TREE_FROM_STRING, Topic as Topic, TopicManager as TopicManager, TopicNameError as TopicNameError, TopicTreeTraverser as TopicTreeTraverser, exportTopicTreeSpec as exportTopicTreeSpec
from _typeshed import Incomplete

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
