import argparse
from typing import Sequence, Any, TypeVar, OrderedDict

__all__ = [
    "ArgumentParser",
    "YAMLConfigFileParser",
    "ConfigFileParser"
]
_N = TypeVar("_N")

class ConfigFileParser(object):
    def get_syntax_description(self) -> Any: ...
    def parse(self, stream: Any) -> Any: ...
    def serialize(self, items: OrderedDict[Any, Any]) -> Any: ...

class YAMLConfigFileParser(ConfigFileParser):
    def get_syntax_description(self) -> str: ...
    def parse(self, stream: Any) -> OrderedDict[Any, Any]: ...
    def serialize(self, items: OrderedDict[Any, Any], default_flow_style: bool = ...) -> Any: ...

class ArgumentParser(argparse.ArgumentParser):
    @property
    def _config_file_parser(self) -> Any: ...

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    # There may be a better way of type hinting this without a type: ignore, but mypy gets unhappy pretty much no matter what as the signatures for parse_args doesn't match with its superclass in argparse
    def parse_args(self, args: Sequence[str] | None = None, namespace: Namespace | None = None, config_file_contents: str | None = None, env_vars: Any=None) -> Namespace: ... # type: ignore[override]

Namespace = argparse.Namespace
ArgParser = ArgumentParser