import doctest
from reprisedb import packers, entries

def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(packers))
    tests.addTests(doctest.DocTestSuite(entries))
    return tests