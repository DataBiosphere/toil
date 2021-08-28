from abc import ABC
from typing import Any, Dict, Optional

from toil.job import Promise


class WDLRuntimeError(RuntimeError):
    pass


class WDLType:
    """
    Represents a primitive or compound WDL type:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#types
    """

    def __init__(self, optional: bool = False):
        self.optional = optional

    @property
    def name(self) -> str:
        """
        Type name as string. Used in display messages / 'mappings.out' if dev
        mode is enabled.
        """
        raise NotImplementedError

    @property
    def default_value(self) -> Optional[str]:
        """
        Default value if optional.
        """
        return None

    def create(self, value: Any, output: bool = False) -> Any:
        """
        Calls at runtime. Returns an instance of the current type. An error may
        be raised if the value is not in the correct format.

        :param value: a Python object
        """
        if value is None:
            # check if input is in fact an optional.
            if self.optional:
                return self.default_value
            else:
                raise WDLRuntimeError(f"Required input for '{self.name}' type not specified.")

        if isinstance(value, Promise):
            return value

        return self._create(value)

    def _create(self, value: Any) -> Any:
        raise NotImplementedError

    def __eq__(self, other: Any) -> bool:
        return self.name.__eq__(other)

    def __str__(self) -> str:
        return self.name.__str__()

    def __repr__(self) -> str:
        return self.name.__repr__()


class WDLCompoundType(WDLType, ABC):
    """
    Represents a WDL compound type.
    """
    pass


class WDLStringType(WDLType):
    """ Represents a WDL String primitive type."""

    @property
    def name(self) -> str:
        return 'String'

    @property
    def default_value(self) -> str:
        return ''

    def _create(self, value: Any) -> Any:
        return str(value)


class WDLIntType(WDLType):
    """ Represents a WDL Int primitive type."""

    @property
    def name(self) -> str:
        return 'Int'

    def _create(self, value: Any) -> Any:
        return int(value)


class WDLFloatType(WDLType):
    """ Represents a WDL Float primitive type."""

    @property
    def name(self) -> str:
        return 'Float'

    def _create(self, value: Any) -> Any:
        return float(value)


class WDLBooleanType(WDLType):
    """ Represents a WDL Boolean primitive type."""

    @property
    def name(self) -> str:
        return 'Boolean'

    def _create(self, value: Any) -> Any:
        return True if value else False


class WDLFileType(WDLType):
    """ Represents a WDL File primitive type."""

    @property
    def name(self) -> str:
        return 'File'

    @property
    def default_value(self) -> str:
        return ''

    def _create(self, value: Any) -> Any:
        if isinstance(value, (WDLFile, Promise)):
            # return the original file if it's passed from task to task.
            return value

        return WDLFile(file_path=value, imported=False)


class WDLArrayType(WDLCompoundType):
    """ Represents a WDL Array compound type."""

    def __init__(self, element: WDLType, optional: bool = False):
        super().__init__(optional)
        self.element = element

    @property
    def name(self) -> str:
        return f'Array[{self.element.name}]'

    def _create(self, value: Any) -> Any:
        if not isinstance(value, list):
            raise WDLRuntimeError(f"Expected an array input for Array, but got '{type(value)}'")

        return [self.element.create(val) for val in value]


class WDLPairType(WDLCompoundType):
    """ Represents a WDL Pair compound type."""

    def __init__(self, left: WDLType, right: WDLType, optional: bool = False):
        super().__init__(optional)
        self.left = left
        self.right = right

    @property
    def name(self) -> str:
        return f'Pair[{self.left.name}, {self.right.name}]'

    def _create(self, value: Any) -> Any:
        if isinstance(value, WDLPair):
            return value
        elif isinstance(value, tuple):
            if len(value) != 2:
                raise WDLRuntimeError('Only support Pair len == 2')
            left, right = value
        elif isinstance(value, dict):
            if 'left' not in value or 'right' not in value:
                raise WDLRuntimeError('Pair needs \'left\' and \'right\' keys')
            left = value.get('left')
            right = value.get('right')
        else:
            raise WDLRuntimeError(f"Expected a pair input for Pair, but got '{type(value)}'")

        return WDLPair(self.left.create(left), self.right.create(right))


class WDLMapType(WDLCompoundType):
    """ Represents a WDL Map compound type."""

    def __init__(self, key: WDLType, value: WDLType, optional: bool = False):
        super().__init__(optional)
        self.key = key
        self.value = value

    @property
    def name(self) -> str:
        return f'Map[{self.key.name}, {self.value.name}]'

    def _create(self, value: Any) -> Any:
        if not isinstance(value, dict):
            raise WDLRuntimeError(f"Expected a map input for Map, but got '{type(value)}'")

        return {self.key.create(k): self.value.create(v) for k, v in value.items()}


class WDLFile:
    """
    Represents a WDL File.
    """
    def __init__(self, file_path: str, file_name: Optional[str] = None, imported: bool = False):
        """
        :param file_path: Path to file.
        :param file_name: Optional. Preserved file name.
        :param imported: If True, this file has been imported to the fileStore
                              via fileStore.importFile().
        """
        self.file_path = file_path
        self.file_name = file_name
        self.imported = imported


class WDLPair:
    """
    Represents a WDL Pair literal defined at
    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#pair-literals
    """

    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_dict(self) -> Dict[str, Any]:
        return {'left': self.left, 'right': self.right}

    def __eq__(self, other: Any) -> Any:
        if not isinstance(other, WDLPair):
            return False
        return self.left == other.left and self.right == other.right

    def __repr__(self) -> str:
        return str(self.to_dict())
