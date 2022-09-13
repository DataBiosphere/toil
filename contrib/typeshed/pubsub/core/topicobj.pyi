from .annotations import annotationType as annotationType
from .listener import CallArgsInfo as CallArgsInfo, Listener as Listener, ListenerValidator as ListenerValidator, UserListener as UserListener
from .topicargspec import ArgSpecGiven as ArgSpecGiven, ArgsDocs as ArgsDocs, ArgsInfo as ArgsInfo, MessageDataSpecError as MessageDataSpecError, MsgData as MsgData, SenderMissingReqdMsgDataError as SenderMissingReqdMsgDataError, SenderUnknownMsgDataError as SenderUnknownMsgDataError, topicArgsFromCallable as topicArgsFromCallable
from .topicexc import ExcHandlerError as ExcHandlerError, TopicDefnError as TopicDefnError, TopicNameError as TopicNameError
from .topicutils import ALL_TOPICS as ALL_TOPICS, smartDedent as smartDedent, stringize as stringize, tupleize as tupleize, validateName as validateName
from _typeshed import Incomplete
from typing import Callable, Iterator, List, Sequence, Tuple, Union, ValuesView

class TreeConfig: ...
ListenerFilter = Callable[[Listener], bool]

class Topic:
    def __init__(self, treeConfig: TreeConfig, nameTuple: Tuple[str, ...], description: str, msgArgsInfo: ArgsInfo, parent: Topic = ...) -> None: ...
    def setDescription(self, desc: str): ...
    def getDescription(self) -> str: ...
    def setMsgArgSpec(self, argsDocs: ArgsDocs, required: Sequence[str] = ...): ...
    def getArgs(self) -> Tuple[Sequence[str], Sequence[str]]: ...
    def getArgDescriptions(self) -> ArgsDocs: ...
    def setArgDescriptions(self, **docs: ArgsDocs): ...
    def hasMDS(self) -> bool: ...
    def filterMsgArgs(self, msgData: MsgData, check: bool = ...) -> MsgData: ...
    def isAll(self) -> bool: ...
    def isRoot(self) -> bool: ...
    def getName(self) -> str: ...
    def getNameTuple(self) -> Tuple[str, ...]: ...
    def getNodeName(self) -> str: ...
    def getParent(self) -> Topic: ...
    def hasSubtopic(self, name: str = ...) -> bool: ...
    def getSubtopic(self, relName: Union[str, Tuple[str, ...]]) -> Topic: ...
    def getSubtopics(self) -> ValuesView[Topic]: ...
    def getNumListeners(self) -> int: ...
    def hasListener(self, listener: UserListener) -> bool: ...
    def hasListeners(self) -> bool: ...
    def getListeners(self) -> List[Listener]: ...
    def getListenersIter(self) -> Iterator[Listener]: ...
    def validate(self, listener: UserListener, curriedArgNames: Sequence[str] = ...) -> CallArgsInfo: ...
    def isValid(self, listener: UserListener, curriedArgNames: Sequence[str] = ...) -> bool: ...
    def subscribe(self, listener: UserListener, **curriedArgs) -> Tuple[Listener, bool]: ...
    def unsubscribe(self, listener: UserListener) -> Listener: ...
    def unsubscribeAllListeners(self, filter: ListenerFilter = ...) -> List[Listener]: ...
    def publish(self, **msgData) -> None: ...
    name: Incomplete
    parent: Incomplete
    subtopics: Incomplete
    description: Incomplete
    listeners: Incomplete
    numListeners: Incomplete
    args: Incomplete
    argDescriptions: Incomplete