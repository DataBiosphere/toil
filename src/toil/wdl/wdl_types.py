from typing import Any


class WDLType(str):
    """
    Represents a primitive or compound WDL type:

    https://github.com/openwdl/wdl/blob/main/versions/development/SPEC.md#types
    """
    # TODO: make this into its own class instead of extending string.
    #  This requires all WDL types to be a WDLType.
    pass


class WDLPairType(WDLType):
    def __new__(cls, left: str, right: str):
        return super(WDLPairType, cls).__new__(cls, 'Pair')

    def __init__(self, left: str, right: str):
        super(WDLPairType, self).__init__()
        self.left = left
        self.right = right

    def __repr__(self):
        return f'WDLPairType({repr(self.left)}, {repr(self.right)})'


class WDLMapType(WDLType):
    def __new__(cls, key: str, value: str):
        return super(WDLMapType, cls).__new__(cls, 'Map')

    def __init__(self, key: str, value: str):
        super(WDLMapType, self).__init__()
        self.key = key
        self.value = value

    def __repr__(self):
        return f'WDLMapType({repr(self.key)}, {repr(self.value)})'


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
