from toil.wdl.wdl_analysis import WDLArrayType, WDLPairType, WDLMapType
from toil.wdl.wdl_functions import parse_value_from_type, write_json


# t = WDLArrayType(WDLPairType('String', WDLPairType('String', 'Int')))
# v = [('testA', ('a', 1)), ('testB', ('b', 2)), ('testC', ('c', 3))]


# t = WDLMapType('String', WDLPairType('Int', 'Int'))
# v = {'a': (1, 2)}


t = WDLArrayType(WDLMapType('String', WDLPairType('Int', 'Int')))
v = [{'a': (1, 2)}, {'b': (3, 4)}, {'c': (5, 6)}]


res = parse_value_from_type(v, var_type=t, from_json=False)
print(res)
# write_json(res, temp_dir='./toiltemp')
