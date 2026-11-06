# Partial stubs for the parts of the HTCondor Python bindings that Toil uses.
#
# Signatures follow the HTCondor 24.0 Python bindings documentation:
# https://htcondor.readthedocs.io/en/24.0/apis/python-bindings/api/htcondor.html
# and the keyword names registered in the bindings source:
# https://github.com/htcondor/htcondor/blob/V24_0-branch/src/python-bindings/schedd.cpp

from enum import Enum
from types import TracebackType
from typing import Any

from classad import ClassAd

# Documented as inheriting from the built-in exception raised before 8.9.9.
class HTCondorIOError(IOError): ...

class JobAction(Enum):
    Remove = ...

class DaemonTypes(Enum):
    Schedd = ...

class QueryOpts():
    ...

class AdTypes():
    ...

class EditResult():
    ...

# Context manager support is not documented; it matches how Toil uses
# Schedd.transaction().
class Transaction:
    def __enter__(self) -> Transaction: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...

# Iteration is not documented; it matches how Toil uses Schedd.xquery().
class QueryIterator:
    def __iter__(self) -> QueryIterator: ...
    def __next__(self) -> ClassAd: ...

class Submit:
    def __init__(self, input: str | dict[str, Any] = ...) -> None: ...
    def queue(
        self, txn: Transaction, count: int = ..., ad_results: list[ClassAd] = ...
    ) -> int: ...

class Schedd:
    def __init__(self, location_ad: ClassAd | None = ...) -> None: ...
    def transaction(
        self, flags: int = ..., continue_txn: bool = ...
    ) -> Transaction: ...
    def xquery(
        self,
        constraint: str = ...,
        projection: list[str] = ...,
        limit: int = ...,
        opts: QueryOpts = ...,
        name: str | None = ...,
    ) -> QueryIterator: ...
    def edit(self, job_spec: str, attr: str, value: str, flags: int = ...) -> EditResult: ...
    def act(self, action: JobAction, job_spec: str, reason: str | None = ...) -> ClassAd: ...

class Collector:
    def __init__(self, pool: str | None = ...) -> None: ...
    def query(
        self,
        ad_type: AdTypes = ...,
        constraint: str = ...,
        projection: list[str] = ...,
        statistics: str = ...,
    ) -> list[ClassAd]: ...
    def locate(
        self, daemon_type: DaemonTypes, name: str | None = ...
    ) -> ClassAd: ...
