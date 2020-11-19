from abc import ABC
from typing import Any


class WDLArgumentError(RuntimeError):
    def __init__(self, message=None):
        super(WDLArgumentError, self).__init__(f'Invalid argument: {message}')


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
        Type name as string.
        """
        raise NotImplementedError

    def as_compiled_string(self) -> str:
        """
        Return a string that preserves the construction of this WDL type so it
        can be passed into the compiled script.
        """
        return f"{self.__class__.__name__}({'optional=True' if self.optional else ''})"

    def create(self, value: Any) -> Any:
        """
        Calls at runtime. Returns an instance of the current type. Some types
        (e.g.: WDLFileType and WDLPairType) require special treatment.

        :param value: a Python object
        """
        raise NotImplementedError

    def __eq__(self, other):
        # TODO: remove after refactor
        return self.name.__eq__(other)

    def __str__(self):
        return self.name.__str__()

    def __repr__(self):
        return self.name.__repr__()


class WDLCompoundType(WDLType, ABC):
    """
    Represent a WDL compound type.
    """
    pass


class WDLStringType(WDLType):
    """
    """

    @property
    def name(self) -> str:
        return 'String'

    def create(self, value: Any) -> Any:
        return value


class WDLIntType(WDLType):
    """
    """

    @property
    def name(self) -> str:
        return 'Int'

    def create(self, value: Any) -> Any:
        return value


class WDLFloatType(WDLType):
    """
    """

    @property
    def name(self) -> str:
        return 'Float'

    def create(self, value: Any) -> Any:
        return value


class WDLBooleanType(WDLType):
    """
    """

    @property
    def name(self) -> str:
        return 'Boolean'

    def create(self, value: Any) -> Any:
        return value


class WDLFileType(WDLType):
    """
    """

    @property
    def name(self) -> str:
        return 'File'

    def create(self, value: Any) -> Any:
        # return WDLFile(value, optional=self.optional)
        return value


class WDLArrayType(WDLCompoundType):
    """
    """

    def __init__(self, element: WDLType, optional: bool = False):
        super().__init__(optional)
        self.element = element

    def __eq__(self, other):
        # TODO: remove after refactor
        return self.element.__eq__(other)

    @property
    def name(self) -> str:
        return f'Array[{self.element.name}]'

    def as_compiled_string(self) -> str:
        return f"WDLArrayType({self.element.as_compiled_string()}{', optional=True' if self.optional else ''})"

    def create(self, value: Any) -> Any:
        if not value:
            return value

        return [self.element.create(val) for val in value]


class WDLPairType(WDLCompoundType):
    """
    """

    def __init__(self, left: WDLType, right: WDLType, optional: bool = False):
        super().__init__(optional)
        self.left = left
        self.right = right

    @property
    def name(self) -> str:
        return f'Pair[{self.left.name}, {self.right.name}]'

    def as_compiled_string(self) -> str:
        return f"WDLPairType({self.left.as_compiled_string()}, {self.right.as_compiled_string()}" \
               f"{', optional=True' if self.optional else ''})"

    def create(self, value: Any) -> Any:

        if isinstance(value, WDLPair):
            return value
        elif isinstance(value, tuple):
            if len(value) != 2:
                raise WDLArgumentError('Only support Pair len == 2')
            left, right = value
        elif isinstance(value, dict):
            if 'left' not in value or 'right' not in value:
                raise WDLArgumentError('Pair needs \'left\' and \'right\' keys')
            left = value.get('left')
            right = value.get('right')
        else:
            raise WDLArgumentError(f'Invalid pair type {type(value)}')

        return WDLPair(self.left.create(left), self.right.create(right))


class WDLMapType(WDLCompoundType):
    """
    """

    def __init__(self, key: WDLType, value: WDLType, optional: bool = False):
        super().__init__(optional)
        self.key = key
        self.value = value

    @property
    def name(self) -> str:
        return f'Map[{self.key.name}, {self.value.name}]'

    def as_compiled_string(self) -> str:
        return f"WDLMapType({self.key.as_compiled_string()}, {self.value.as_compiled_string()}" \
               f"{', optional=True' if self.optional else ''})"

    def create(self, value: Any) -> Any:
        return {self.key.create(k): self.value.create(v) for k, v in value}


#
# WDL instances
#

class WDLFile:
    """
    """

    def __init__(self, file_name, optional):
        self.file_name = file_name
        self.optional = optional

        self.imported = False

    def import_to_job_store(self):
        if self.imported:
            return False

        # import

        self.imported = True


class WDLPair:
    """
    Represent a WDL Pair literal defined at
    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#pair-literals
    """

    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_dict(self):
        return {'left': self.left, 'right': self.right}

    def __repr__(self):
        return str(self.to_dict())
