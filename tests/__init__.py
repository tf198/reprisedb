from unittest import TestCase
from reprisedb import database

import os.path


class RepriseDBTestCase(TestCase):
    
    TESTDIR = 'test_output'
    
    @classmethod
    def setUpClass(cls):
        os.makedirs(cls.TESTDIR)
        
    @classmethod
    def tearDownClass(cls):
        cls.clean_dir()
        os.rmdir(cls.TESTDIR)
    
    @classmethod
    def clean_dir(cls):
        for f in os.listdir(cls.TESTDIR):
            os.unlink(os.path.join(cls.TESTDIR, f))
    
    def tearDown(self):
        self.clean_dir()