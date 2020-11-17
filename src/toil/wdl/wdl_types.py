
class WDLType:
    """
    Represents a primitive or compound WDL type:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#types
    """
    def __init__(self, optional: bool = False):
        self.optional = optional

    @property
    def type_name(self) -> str:
        """
        Type name as string.
        """
        return 'Unassigned'

    def validate(self):
        """

        """
        pass

    def __eq__(self, other):
        # TODO: remove after refactor
        return self.type_name.__eq__(other)

    def __str__(self):
        return self.type_name.__str__()

    def __repr__(self):
        return self.type_name.__repr__()


class WDLCompoundType(WDLType):
    """
    Represent a WDL compound type.
    """
    pass


class WDLStringType(WDLType):
    """

    """
    @property
    def type_name(self) -> str:
        return 'String'


class WDLIntType(WDLType):
    """

    """
    @property
    def type_name(self) -> str:
        return 'Int'


class WDLFloatType(WDLType):
    """

    """
    @property
    def type_name(self) -> str:
        return 'Float'


class WDLBooleanType(WDLType):
    """

    """
    @property
    def type_name(self) -> str:
        return 'Boolean'


class WDLFileType(WDLType):
    """

    """
    @property
    def type_name(self) -> str:
        return 'File'


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
    def type_name(self) -> str:
        return f'Array[{self.element.type_name}]'


class WDLPairType(WDLCompoundType):
    """

    """
    def __init__(self, left: WDLType, right: WDLType, optional: bool = False):
        super().__init__(optional)
        self.left = left
        self.right = right

    @property
    def type_name(self) -> str:
        return f'Pair[{self.left.type_name}, {self.right.type_name}]'


class WDLMapType(WDLCompoundType):
    """

    """
    def __init__(self, key: WDLType, value: WDLType, optional: bool = False):
        super().__init__(optional)
        self.key = key
        self.value = value

    @property
    def type_name(self) -> str:
        return f'Map[{self.key.type_name}, {self.value.type_name}]'
