
from reprisedb import drivers, datastore

from tests.test_datastore import RevisionDataStoreTestCase

class BSDDBDriverTestCase(RevisionDataStoreTestCase):
    
    def get_datastore(self):
        driver = drivers.BSDDBDriver(self.TESTDIR)
        return datastore.RevisionDataStore(driver.get_db('testing'), 0)
    
class LMDBDriverTestCase(RevisionDataStoreTestCase):
    
    def get_datastore(self):
        driver = drivers.LMDBDriver(self.TESTDIR)
        return datastore.RevisionDataStore(driver.get_db('testing'), 0)