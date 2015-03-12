from . import RepriseDBTestCase

from reprisedb import drivers, datastore

import logging

class DatastoreTestCase(RepriseDBTestCase):
    
    def setUp(self):
        driver = drivers.LMDBDriver(self.TESTDIR)
        self.rds = datastore.RevisionDataStore(driver.get_db('testing'), 0)
        self.rds.store([('a\x00', 'A'),
                        ('c\x00', 'C'),
                        ('e\x00', 'E')], 1)
        self.rds.store([('b\x00', 'B'),
                        ('d\x00', 'D'),
                        ('f\x00', 'F')], 2)
        self.rds.store([('c\x00', 'CHARLIE'),
                        ('d\x00', 'DELTA')], 3)
        
    def test_iter_get(self):
        
        def f(**kwargs):
            keys = ['a\x00', 'c\x00', 'f\x00']
            return [ x[2] for x in self.rds.iter_get(keys, **kwargs) ]
        
        # all revisions
        self.assertEqual(f(), ['A', 'CHARLIE', 'F'])
        
        # revisions 1 and 2
        self.assertEqual(f(end_revision=2), ['A', 'C', 'F'])
        
        # revisions 2 and 3
        self.assertEqual(f(start_revision=2), [None, 'CHARLIE', 'F'])
        
        # revision 2 only
        self.assertEqual(f(start_revision=2, end_revision=2), [None, None, 'F'])
        
        
        
    def test_iter_items(self):
        
        def f(**kwargs):
            return [ x[2] for x in self.rds.iter_items(**kwargs) ]
        
        self.assertEqual(f(), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        
        self.assertEqual(f(end_revision=1), ['A', 'C', 'E'])
        self.assertEqual(f(end_revision=2), ['A', 'B', 'C', 'D', 'E', 'F'])
        self.assertEqual(f(end_revision=3), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        
        self.assertEqual(f(start_revision=2), ['B', 'CHARLIE', 'DELTA', 'F'])
        
        self.assertEqual(f(start_revision=2, end_revision=2), ['B', 'D', 'F'])
        
        self.assertEqual(f(start_key='c\x00'), ['CHARLIE', 'DELTA', 'E', 'F'])
        self.assertEqual(f(start_key='c\x00', end_key='f\x00'), ['CHARLIE', 'DELTA', 'E'])
        
        self.assertEqual(f(start_key='c\x00', end_key='f\x00', end_revision=1), ['C', 'E'])
        self.assertEqual(f(start_key='c\x00', end_key='f\x00', end_revision=2), ['C', 'D', 'E'])
        self.assertEqual(f(start_key='c\x00', end_key='f\x00', start_revision=2), ['CHARLIE', 'DELTA'])
        
    def test_get_item(self):
        
        def f(**kwargs):
            return [ self.rds.get_item('%s\x00' % x, **kwargs)[1] for x in 'abcdef' ]
        
        self.assertEqual(f(), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        
        self.assertEqual(f(end_revision=1), ['A', None, 'C', None, 'E', None])
        self.assertEqual(f(end_revision=2), ['A', 'B', 'C', 'D', 'E', 'F'])
        self.assertEqual(f(end_revision=3), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        self.assertEqual(f(end_revision=4), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        
        self.assertEqual(f(start_revision=1), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        self.assertEqual(f(start_revision=2), [None, 'B', 'CHARLIE', 'DELTA', None, 'F'])
        self.assertEqual(f(start_revision=3), [None, None, 'CHARLIE', 'DELTA', None, None])
        
        self.assertEqual(f(start_revision=2, end_revision=2), [None, 'B', None, 'D', None, 'F'])
        self.assertEqual(f(start_revision=2, end_revision=3), [None, 'B', 'CHARLIE', 'DELTA', None, 'F'])
        
class TransactionDatastoreTestCase(RepriseDBTestCase):
    
    def setUp(self):
        driver = drivers.LMDBDriver(self.TESTDIR)
        self.rds = datastore.RevisionDataStore(driver.get_db('testing'), 0)
        self.rds.store([('a\x00', 'A'),
                        ('c\x00', 'C'),
                        ('e\x00', 'E')], 1)
        self.rds.store([('b\x00', 'B'),
                        ('d\x00', 'D'),
                        ('f\x00', 'F')], 2)
        self.rds.store([('c\x00', 'CHARLIE'),
                        ('d\x00', 'DELTA')], 3)
        self.tds = datastore.TransactionDataStore(self.rds)
        
    def test_iter_items(self):
        
        def f(**kwargs):
            return [ x[2] for x in self.tds.iter_items(**kwargs) ]
        
        self.assertEqual(f(), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        self.assertEqual(f(end_revision=2), ['A', 'B', 'C', 'D', 'E', 'F'])
        
        self.tds.store([('b\x00', 'BRAVO'),
                        ('e\x00', 'ECHO'),
                        ('g\x00', 'GOLF')])
        
        self.assertEqual(f(), ['A', 'BRAVO', 'CHARLIE', 'DELTA', 'ECHO', 'F', 'GOLF'])
        
        self.tds.store([('ba\x00', 'ONE')])
        
        self.assertEqual(f(), ['A', 'BRAVO', 'ONE', 'CHARLIE', 'DELTA', 'ECHO', 'F', 'GOLF'])
        
    def test_iter_get(self):
        
        def f(**kwargs):
            keys = ['a\x00', 'c\x00', 'f\x00', 'g\x00']
            return [ x[2] for x in self.tds.iter_get(keys, **kwargs) ]
        
        # all revisions
        self.assertEqual(f(), ['A', 'CHARLIE', 'F', None])
        
        self.tds.store([('b\x00', 'BRAVO'),
                        ('e\x00', 'ECHO'),
                        ('g\x00', 'GOLF')])
        
        self.assertEqual(f(), ['A', 'CHARLIE', 'F', 'GOLF'])
        self.assertEqual(f(end_revision=1), ['A', 'C', None, 'GOLF'])
        self.assertEqual(f(start_revision=2), [None, 'CHARLIE', 'F', 'GOLF'])
        
    def test_get_item(self):
        
        def f(**kwargs):
            return [ self.tds.get_item('%s\x00' % x, **kwargs)[1] for x in 'abcdefg' ]
        
        self.assertEqual(f(), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F', None])
        
        self.tds.store([('b\x00', 'BRAVO'),
                        ('e\x00', 'ECHO'),
                        ('g\x00', 'GOLF')])
        
        self.assertEqual(f(), ['A', 'BRAVO', 'CHARLIE', 'DELTA', 'ECHO', 'F', 'GOLF'])
        self.assertEqual(f(end_revision=1), ['A', 'BRAVO', 'C', None, 'ECHO', None, 'GOLF'])
        