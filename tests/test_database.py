from unittest import TestCase
import os.path

from reprisedb import drivers, database

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
        self.assertEqual(t.keys('people'), [1, 2])
        self.assertEqual(t.get('people', 1), 'Bob')
        
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
        
        t2.put('people', 2, 'Andy')
        t2.put('people', 1, 'Jane')
        
        t2.commit()
            
        t = self.db.begin()
        self.assertEqual([ t.get('people', x) for x in [1, 2, 3]], ['Jane', 'Andy', 'Dave'])