import os.path

import logging
logger = logging.getLogger(__name__)

try:
    import lmdb
    HAS_LMDB = True
except ImportError:
    HAS_LMDB = False
    
class BaseDriver(object):
    pass
    
class LMDBDriver(BaseDriver):
    
    def __init__(self, path):
        if not HAS_LMDB:
            raise RuntimeError("LMDB library not found")
        
        self.path = os.path.realpath(path)
        
        if not os.path.isdir(self.path):
            os.makedirs(self.path, 0700)
            
        self.dbs = {}
        
    def get_db(self, name):
        if not name in self.dbs:
            self.dbs[name] = lmdb.open(os.path.join(self.path, name), subdir=False, lock=False)
        
        return self.dbs[name]
    
    def drop_db(self, name):
        filename = os.path.join(self.path, name)
        if os.path.exists(filename):
            os.unlink(filename)
            logger.debug("Removed %s", filename)
                
        return True
