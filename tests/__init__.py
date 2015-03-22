from unittest import TestCase
from reprisedb import database

import os.path
import shutil


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
            p = os.path.join(cls.TESTDIR, f)
            if os.path.isdir(p):
                shutil.rmtree(p)
            else:
                os.unlink(p)
    
    def tearDown(self):
        self.clean_dir()