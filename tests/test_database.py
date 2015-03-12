from unittest import TestCase
import os.path
import itertools

import logging

from reprisedb import database, packers, DELETED

class DatabaseTestCase(TestCase):
    
    TESTDIR = 'test_output'
    
    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.TESTDIR)
        
    @classmethod
    def tearDownClass(cls):
        os.rmdir(cls.TESTDIR)
    
    def setUp(self):
        self.db = database.RepriseDB(path=self.TESTDIR)
        
    def tearDown(self):
        for f in os.listdir(self.TESTDIR):
            os.unlink(os.path.join(self.TESTDIR, f))
    
    def load_data(self, data):
        t = self.db.begin()
        for c, k, v in data:
            t.put(c, k, v)
        t.commit()
    
    def test_load_data(self):
        self.db.create_collection('people', value_packer='p_string')
        self.load_data((('people', 1, 'Bob'),
                        ('people', 2, 'Fred')))
        
        t = self.db.begin()
        
        self.assertEqual(t.get('people', 1), 'Bob')
        self.assertEqual(t.keys('people'), [1, 2])
        
    def test_lookups(self):
        self.db.create_collection('people', value_packer='p_dict')
        self.db.add_index('people', 'name', 'p_string')
        self.load_data((('people', 3, {'name': 'Bob'}),
                        ('people', 6, {'name': 'Brenda'}),
                        ('people', 23, {'name': 'Borris'}),
                        ('people', 9, {'name': 'Andy'}),
                        ('people', 12, {'name': 'Zavier'})))
        
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
        
    def test_blocked_commit(self):
        self.db.create_collection('people', value_packer='p_string')
        self.load_data((('people', 1, 'Bob'),
                        ('people', 2, 'Fred')))
        
        t1 = self.db.begin()
        t2 = self.db.begin()
        
        t1.put('people', 3, 'Dave')
        c1 = t1.commit()
        
        t2.put('people', 3, 'Andy')
        t2.put('people', 1, 'Jane')
        
        with self.assertRaisesRegexp(database.RepriseDBIntegrityError, 'Current commit is %d' % c1):
            t2.commit()
            
        self.assertEqual(t2.conflicts(), [('people', 3, 4, 'Dave')])
        
    def test_nonblocked_commit(self):
        self.db.create_collection('people', value_packer='p_string')
        self.load_data((('people', 1, 'Bob'),
                        ('people', 2, 'Fred')))
        
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