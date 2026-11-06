# Partial stubs for the parts of the ClassAd Python bindings that Toil uses.
#
# Signatures follow the HTCondor 24.0 Python bindings documentation:
# https://htcondor.readthedocs.io/en/24.0/apis/python-bindings/api/classad.html

from typing import Any

# Documented as implementing the dictionary protocol.
class ClassAd:
    def __getitem__(self, key: str) -> Any: ...
    def get(self, key: str, default: Any = ...) -> Any: ...
