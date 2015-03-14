from . import RepriseDBTestCase

from reprisedb import drivers, datastore

import logging

class BaseDataStoreTestCase(RepriseDBTestCase):
    
    def setUp(self):
        self.ds = self.get_datastore()
        self.ds.store([('a\x00', 'A'),
                       ('c\x00', 'C'),
                       ('e\x00', 'E')], 1)
        self.ds.store([('b\x00', 'B'),
                       ('d\x00', 'D'),
                       ('f\x00', 'F')], 2)
        self.ds.store([('c\x00', 'CHARLIE'),
                       ('d\x00', 'DELTA')], 3)
        
    def get_items(self, keys, **kwargs):
        result = []
        for x in keys:
            try:
                result.append(self.ds.get_item("%s\x00" % x, **kwargs)[1])
            except KeyError:
                result.append(None)
        return result
    
    def iter_items(self, **kwargs):
        return [ x[2] for x in self.ds.iter_items(**kwargs) ]
        
    def iter_get(self, keys=['a\x00', 'c\x00', 'f\x00'], **kwargs):
        return [ x[2] for x in self.ds.iter_get(keys, **kwargs) ]

class RevisionDataStoreTestCase(BaseDataStoreTestCase):
    
    datastore_class = datastore.RevisionDataStore
    datastore_args = (0, )
    
    def get_datastore(self):
        driver = drivers.LMDBDriver(self.TESTDIR)
        return datastore.RevisionDataStore(driver.get_db('testing'), 0)
        
    def test_iter_get(self):
        
        f = self.iter_get
        
        # all revisions
        self.assertEqual(f(), ['A', 'CHARLIE', 'F'])
        
        # revisions 1 and 2
        self.assertEqual(f(end_revision=2), ['A', 'C', 'F'])
        
        # revisions 2 and 3
        self.assertEqual(f(start_revision=2), [None, 'CHARLIE', 'F'])
        
        # revision 2 only
        self.assertEqual(f(start_revision=2, end_revision=2), [None, None, 'F'])
        
        
        
    def test_iter_items(self):
        
        f = self.iter_items
        
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
            return self.get_items('abcdef', **kwargs)
        
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
        
    def test_history(self):
        self.ds.store([('a\x00', 'A4'),
                       ('b\x00', 'B4')], 4)
        self.ds.store([('a\x00', 'A5'),
                       ('c\x00', 'C5')], 5)
        self.ds.store([('a\x00', 'A6'),
                       ('c\x00', 'C6')], 6)
        self.ds.store([('a\x00', 'A7'),
                       ('b\x00', 'B7'),
                       ('c\x00', 'C7')], 7)        
        
        self.assertEqual( list(self.ds.history('c\x00')), [(7, 'C7'), (6, 'C6'), (5, 'C5'), (3, 'CHARLIE'), (1, 'C')])
        self.assertEqual( list(self.ds.history('c\x00', end_revision=5)), [(5, 'C5'), (3, 'CHARLIE'), (1, 'C')])
        self.assertEqual( list(self.ds.history('c\x00', start_revision=3)), [(7, 'C7'), (6, 'C6'), (5, 'C5')])
        self.assertEqual( list(self.ds.history('c\x00', end_revision=6, start_revision=3)), [(6, 'C6'), (5, 'C5')])
        
    def test_iter_prune(self):
        self.ds.store([('a\x00', 'A4'),
                       ('b\x00', 'B4')], 4)
        self.ds.store([('a\x00', 'A5'),
                       ('c\x00', 'C5')], 5)
        self.ds.store([('a\x00', 'A6'),
                       ('c\x00', 'C6')], 6)
        self.ds.store([('a\x00', 'A7'),
                       ('b\x00', 'B7'),
                       ('c\x00', 'C7')], 7)
        
        self.assertEqual([ x[2] for x in self.ds.iter_prune(3) ], ['A4', 'A', 'CHARLIE', 'C'])
        self.assertEqual([ x[2] for x in self.ds.iter_prune(2) ], ['A5', 'B', 'C5'])
        

class MemoryDataStoreTestCase(BaseDataStoreTestCase):
    
    def get_datastore(self):
        return datastore.MemoryDataStore()
    
    def test_get_item(self):
        # no revisions
        self.assertEqual(self.get_items('abcdef'), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        
    def test_iter_items(self):
        
        f = self.iter_items
        self.assertEqual(f(), ['A', 'B', 'CHARLIE', 'DELTA', 'E', 'F'])
        
        self.assertEqual(f(start_key='c\x00'), ['CHARLIE', 'DELTA', 'E', 'F'])
        
    def test_iter_get(self):
        
        f = self.iter_get
        self.assertEqual(f(), ['A', 'CHARLIE', 'F'])
        
        self.assertEqual(f(['a\x00', 'g\x00']), ['A', None])
        
class ArchiveDataStoreTestCase(RevisionDataStoreTestCase):
    
    def get_datastore(self):
        driver = drivers.LMDBDriver(self.TESTDIR)
        return datastore.ArchiveDataStore(driver.get_db('testing'), self.TESTDIR + "/testing-archive" , 0)
    
    def test_history(self):
        self.skipTest("Need to delete from archive")
        
    def test_iter_prune(self):
        self.skipTest("Need to delete from archive")
    
class ProxyDataStoreTestCase(MemoryDataStoreTestCase):
        
    def setUp(self):
        self.ds1 = datastore.MemoryDataStore()
        self.ds1.store([('a\x00', 'A'),
                        ('c\x00', 'C'),
                        ('e\x00', 'E')], 1)
        self.ds2 = datastore.MemoryDataStore()
        self.ds2.store([('b\x00', 'B'),
                        ('d\x00', 'D'),
                        ('f\x00', 'F')], 2)
        self.ds3 = datastore.MemoryDataStore()
        self.ds3.store([('c\x00', 'CHARLIE'),
                        ('d\x00', 'DELTA')], 3)
        self.ds = datastore.ProxyDataStore([self.ds3, self.ds2, self.ds1])
        
            