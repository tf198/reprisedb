NUL = '\x00'
ONE = '\x01'
DELETED = '\x10\x7F\x1B'  # DLE DEL ESC

class RepriseDataError(Exception):
    pass