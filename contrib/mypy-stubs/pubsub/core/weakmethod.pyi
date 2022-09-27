from typing import Callable, Union
from weakref import ref as WeakRef

from weakrefmethod import WeakMethod

WeakObjOrMethod = Union[WeakMethod, WeakRef]
DeadRefObserver = Callable[[WeakObjOrMethod], None]

def getWeakRef(obj, notifyDead: DeadRefObserver = ...): ...
