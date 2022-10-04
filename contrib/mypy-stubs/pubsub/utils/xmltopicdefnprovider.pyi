from _typeshed import Incomplete

from ..core.topicdefnprovider import ITopicDefnProvider
from ..core.topictreetraverser import ITopicTreeVisitor

TOPIC_TREE_FROM_FILE: str

class XmlTopicDefnProvider(ITopicDefnProvider):
    class XmlParserError(RuntimeError): ...
    class UnrecognizedSourceFormatError(ValueError): ...

    def __init__(self, xml, format=...) -> None: ...
    def getDefn(self, topicNameTuple): ...
    def topicNames(self): ...
    def getTreeDoc(self): ...

class XmlVisitor(ITopicTreeVisitor):
    tree: Incomplete
    known_topics: Incomplete
    def __init__(self, elem) -> None: ...

def exportTopicTreeSpecXml(
    moduleName: Incomplete | None = ...,
    rootTopic: Incomplete | None = ...,
    bak: str = ...,
    moduleDoc: Incomplete | None = ...,
): ...
