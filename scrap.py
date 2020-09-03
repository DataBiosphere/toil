from sqlite3 import OperationalError

import traceback

try:
    raise OperationalError()
except OperationalError as e:
    print(e.__cause__)
    print(traceback.format_exc())

e = AssertionError()
assert isinstance(e, Exception)
