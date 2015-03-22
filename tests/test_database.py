from unittest import TestCase
import os.path

import logging

from reprisedb import database, drivers

class DatabaseTestCase(TestCase):
    
    TESTDIR = 'test_output'
    
    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.TESTDIR)
        
    @classmethod
    def tearDownClass(cls):
        for f in os.listdir(cls.TESTDIR):
            os.unlink(os.path.join(cls.TESTDIR, f))
        os.rmdir(cls.TESTDIR)
    
    def setUp(self):
        self.db = database.RepriseDB(path=self.TESTDIR, driver=drivers.BSDDBDriver)
        
    def tearDown(self):
        for f in os.listdir(self.TESTDIR):
            os.unlink(os.path.join(self.TESTDIR, f))
    
    def load_data(self, collection, data, key_packer='p_uint32', value_packer='p_dict'):
        t = self.db.begin()
        t.create_collection(collection, key_packer, value_packer)
        
        if hasattr(data, 'iteritems'):
            data = data.iteritems()
        
        for k, v in data:
            t.put(collection, k, v)
        t.commit()
    
    def test_load_data(self):
        t = self.db.begin()
        t.create_collection('people', value_packer='p_string')
        t.bulk_put('people', ({1: 'Bob', 2: 'Fred'}))
        t.commit()
        
        t = self.db.begin()
        
        self.assertEqual(t.get('people', 1), 'Bob')
        self.assertEqual(t.keys('people'), [1, 2])
        
    def test_lookups(self):
        t = self.db.begin()
        t.create_collection('people')
        t.add_index('people', 'name', 'string')
        t.bulk_put('people', ((3 , {'name': 'Bob'}),
                              (6 , {'name': 'Brenda'}),
                              (23, {'name': 'Borris'}),
                              (9 , {'name': 'Andy'}),
                              (12, {'name': 'Zavier'})))
        t.commit()
        
        t = self.db.begin()
        
        self.assertEqual(t.lookup('people', 'name', '', '~'), [9, 3, 23, 6, 12])
        
        self.assertEqual(t.lookup('people', 'name', 'Bob'), [3])
        self.assertEqual(t.lookup('people', 'name', 'Bo', 'Bp'), [3, 23])
        
        t.put('people', 21, {'name': 'Andrew'})
        t.put('people', 14, {'name': 'Bruce'})
        t.delete('people', 23)
        
        self.assertEqual(t.lookup('people', 'name', '', '~'), [21, 9, 3, 6, 14, 12])
        
        self.assertEqual(t.lookup('people', 'name', 'Bob'), [3])
        self.assertEqual(t.lookup('people', 'name', 'Bruce'), [14])
        self.assertEqual(t.lookup('people', 'name', 'Br', 'Bs'), [6, 14])
        
        # test offset and length
        self.assertEqual(t.lookup('people', 'name', 'Andy', 'C'), [9, 3, 6, 14])
        self.assertEqual(t.lookup('people', 'name', 'Andy', 'C', offset=2), [6, 14])
        self.assertEqual(t.lookup('people', 'name', 'Andy', 'C', length=2), [9, 3])
        self.assertEqual(t.lookup('people', 'name', 'Andy', 'C', offset=1, length=2), [3, 6])
        
    def test_indexing(self):
        t = self.db.begin()
        t.create_collection('people')
        t.bulk_put('people', {1: {'first_name': 'Bob'},
                              2: {'first_name': 'Fred', 'age': 29}})
        t.commit()
        
        t.add_index('people', 'first_name', 'string')
        t.add_index('people', 'age', 'uint8')
        
        self.assertEqual(t.lookup('people', 'first_name', 'Bob'), [1])
        self.assertEqual(t.lookup('people', 'age', 18, 30), [2])
        
        t.commit()
        
        t.bulk_put('people', {1: {'first_name': 'Robert', 'age': 18},
                              3: {'first_name': 'Dave', 'age': 23},
                              4: {'first_name': 'Andy', 'age': 30}})
        
        self.assertEqual(t.lookup('people', 'first_name', 'Robert'), [1])
        self.assertEqual(t.lookup('people', 'first_name', 'Bob'), [])
        
        self.assertEqual(t.lookup('people', 'age', 18, 30), [1, 3, 2])
        
        
    def test_blocked_commit(self):
        t = self.db.begin()
        t.create_collection('people', value_packer='p_string')
        t.bulk_put('people', {1: 'Bob', 2: 'Fred'})
        t.commit()
        
        t1 = self.db.begin()
        t2 = self.db.begin()
        
        t1.put('people', 3, 'Dave')
        c1 = t1.commit()
        
        t2.put('people', 3, 'Andy')
        t2.put('people', 1, 'Jane')
        
        print "C1", c1
        
        with self.assertRaisesRegexp(database.RepriseDBIntegrityError, 'Current commit is %d' % c1):
            t2.commit()
            
        self.assertEqual(t2.conflicts(), [('people', 3, 3)])
                
    def test_nonblocked_commit(self):
        t = self.db.begin()
        t.create_collection('people', value_packer='p_string')
        t.bulk_put('people', {1: 'Bob', 2: 'Fred'})
        t.commit()
        
        t1 = self.db.begin()
        t2 = self.db.begin()
        
        t1.put('people', 3, 'Dave')
        c1 = t1.commit()
        
        self.assertEqual(t1.get('_commits', c1)['updates'], {'people': [3]})
        
        t2.put('people', 2, 'Andy')
        t2.put('people', 1, 'Jane')
        
        t2.commit()
            
        t = self.db.begin()
        self.assertEqual([ t.get('people', x) for x in [1, 2, 3]], ['Jane', 'Andy', 'Dave'])