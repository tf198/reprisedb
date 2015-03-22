import os.path

import logging
logger = logging.getLogger(__name__)

from contextlib import contextmanager

try:
    import lmdb
    HAS_LMDB = True
except ImportError: HAS_LMDB = False
    
try:
    import bsddb
    HAS_BSDDB = True
except ImportError: HAS_BSDDB = False
    

class BaseDriver(object):
    
    def __init__(self, path, **config):
        
        self.config = config
        self.path = os.path.realpath(path)
        
        if not os.path.isdir(self.path):
            os.makedirs(self.path, 0700)
            
        self.dbs = {}
        
    def get_db(self, name):
        if not name in self.dbs:
            self.dbs[name] = self.open_db(name)
        
        return self.dbs[name]
    
    def drop_db(self, name):
        
        if name in self.dbs:
            self.dbs[name].close()
            del self.dbs[name]
        
        filename = os.path.join(self.path, name)
        if os.path.exists(filename):
            os.unlink(filename)
            logger.debug("Removed %s", filename)
            
        return True

class LMDBDriver(BaseDriver):
    
    def __init__(self, path, **config):
        if not HAS_LMDB:
            raise RuntimeError("LMDB library not installed")
        
        super(LMDBDriver, self).__init__(path, **config)
        config.setdefault('max_dbs', 128)
        logger.debug("Driver config: %r", config)
        
        self.env = lmdb.open(os.path.join(self.path, 'data'), subdir=False, **config)
    
    def open_db(self, name):
        return LMDBDatabase(self.env, self.env.open_db(name))
        
    def drop_db(self, name):
        with self.env.begin(write=True) as txn:
            txn.drop_db(name, True)
        del self.dbs[name]
            
class LMDBDatabase(object):
    
    def __init__(self, env, db):
        self.env = env
        self.db = db
        
    def begin(self, **opts):
        return self.env.begin(db=self.db, **opts)

class BSDDBDriver(BaseDriver):
    
    def open_db(self, name):
        return BSDDBDatabase(os.path.join(self.path, name))

class BSDDBDatabase(object):
    
    def __init__(self, path):
        self._path = path
        self._size = None
        
    def path(self):
        return self._path
    
    def close(self):
        pass
    
    def stat(self):
        s = os.stat(self._path)
        
        r = { x: getattr(s, x) for x in ['st_size']}
        r['entries'] = self._size
        return r
        
    @contextmanager
    def begin(self, write=False):
        if not os.path.exists(self._path): write=True
        
        mode = 'c' if write else 'r'
        bt = bsddb.btopen(self._path, mode)
        yield BSDDBTransaction(bt)
        self._size = len(bt)
        bt.close()
        
        
class BSDDBTransaction(object):
    
    def __init__(self, db):
        self.db = db
        
    @contextmanager
    def cursor(self):
        yield BSDDBCursor(self.db)
        
class BSDDBCursor(object):
    
    def __init__(self, db):
        self.db = db
        self._value = None
        self._key = None
        
    def key(self):
        return self._key
    
    def value(self):
        return self._value
    
    def item(self):
        return self._key, self._value
        
    def putmulti(self, d):
        self.db.update(d)
        return None, None
    
    def delete(self):
        del self.db[self._key]
        self.set_range(self._key)
    
    def _call_and_set(self, method, *args, **kwargs):
        try:
            self._key, self._value = getattr(self.db, method)(*args, **kwargs)
            return self._key is not None
        except:
            self._key = self._value = None
            return False
    
    def first(self):
        return self._call_and_set('first')
    
    def next(self):
        return self._call_and_set('next')
        
    def last(self):
        return self._call_and_set('last')
    
    def set_range(self, key):
        return self._call_and_set('set_location', key)
        
    def __iter__(self):
        return self.db.iteritems()
        
    