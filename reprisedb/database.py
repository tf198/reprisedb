import drivers
from reprisedb import packers, entries
from reprisedb.datastore import RevisionDataStore, MemoryDataStore
from contextlib import contextmanager
import hashlib

from sortedcontainers import SortedDict

import logging
logger = logging.getLogger(__name__)

def dotted_accessor(d, accessor, default=None):
    if d is None:
        return default
    
    try:
        for p in accessor.split('.'):
            d = d[p]
        return d
    except KeyError:
        return default

class RepriseDBIntegrityError(Exception): pass

class RepriseDB(object):
    
    def __init__(self, **config):
        
        path = config.get('path', 'data')
         
        self.driver = config.get('driver', drivers.LMDBDriver)(path)
        
        self._collections = {}
        
        # manually configure the meta collection
        self._collections['_meta'] = Collection({'name': '_meta', 
                                                 'key_packer': 'p_string',
                                                 'value_packer': 'p_obj',
                                                 'indexes': {}})
        
        self._collections['_commits'] = Collection({'name': '_commits',
                                                    'key_packer': 'p_uint32',
                                                    'value_packer': 'p_obj',
                                                    'indexes': {}})
        
        self._rds = {}
        
        self._current_commit = 0 # need to set to something
        t = Transaction(self, None)
        self._current_commit = t.get('_commits', 0)
        
        if self._current_commit is None:
            logger.debug("Initialising database")
            self._current_commit = 0
            t.put('_meta', 'info:version', '0.1.1', False)
            assert t.commit(0) == 1
        
        logger.debug("Current commit: %d", self._current_commit)
    
    def meta_key(self, collection):
        return "collection:{0}".format(collection)
    
    def get_collection(self, name):
        
        if not name in self._collections:
        
            t = self.begin()
            meta = t.get('_meta', self.meta_key(name))
            
            if meta is None:
                raise Exception("No collection named %s" % name)
            
            logger.debug("Creating collection %s", name)
            
            self._collections[name] = Collection(meta)
            
        return self._collections[name]
    
    def get_rds(self, name):
        if not name in self._rds:
            self._rds[name] = RevisionDataStore(self.driver.get_db(name), self._current_commit)
        return self._rds[name]
    
    def drop_collection(self, name):
        c = self.get_collection(name)
            
        for accessor in c.meta['indexes']:
            self.driver.drop_db('{0}.{1}'.format(name, accessor))
            
        self.driver.drop_db(name)
        
        t = self.begin()
        t.delete('_meta', self.meta_key(name))
        t.commit()
        
        del self._collections[name]
    
    def create_collection(self, name, key_packer='p_uint32', value_packer='p_dict'):
        
        if name in self._collections:
            raise Exception("Already a collection named %s" % name)
        
        t = self.begin()
        
        meta_key = self.meta_key(name)
        
        meta = t.get('_meta', meta_key)
        
        if meta is not None:
            raise Exception("Already a collection named %s" % name)
        
        meta = {'name': name,
                'key_packer': key_packer,
                'value_packer': value_packer,
                'indexes': {}}
        
        t.put('_meta', meta_key, meta)
        t.commit()
        
        self._collections[name] = Collection(meta)
        
        return self._collections[name]
    
    def add_index(self, collection, accessor, value_packer):
        c = self.get_collection(collection)
        
        current = c.meta['indexes']
        if accessor in current:
            raise Exception("Already an index on %s" % accessor)
        
        current[accessor] = value_packer
        
        t = self.begin()
        t.put('_meta', self.meta_key(collection), c.meta)
        t.commit()
    
    def current_commit(self):
        return self._current_commit
    
    def begin(self, commit=None):
        if commit is None: commit = self._current_commit
        return Transaction(self, commit)
    
    @contextmanager
    def commit_resource(self):
        try:
            self._current_commit += 1
            yield self._current_commit
        except:
            self._current_commit -= 1
            raise
        
    def do_commit(self, commit):
        
        if commit != self._current_commit:
            raise RepriseDBIntegrityError("Current commit is %d" % self._current_commit)
        
        self._current_commit += 1
        return self._current_commit
        
            
            
class Collection(object):
    '''
    A collection is an Entry, controlled by a key_packer and value_packer.
    It also can contain indexes
    '''
    
    def __init__(self, meta):
        self.meta = meta
        self.name = meta['name']
        self.key_packer = getattr(packers, meta['key_packer'])
        self.value_packer = getattr(packers, meta['value_packer'])
        
        self.entry = entries.Entry(self.key_packer, self.value_packer)
        
        self._indexes = {}
    
    def index(self, pk, new_item, old_item):
        result = []
        
        for accessor in self.meta['indexes']:
            new_value = dotted_accessor(new_item, accessor)
            old_value = dotted_accessor(old_item, accessor)
            
            if new_value != old_value:
                
                db, indexer = self.get_indexer(accessor)
                
                if old_value != None:
                    k, v = indexer.prepare(old_value, pk, '-')
                    result.append((db, k, v))
                
                if new_value != None:
                    k, v = indexer.prepare(new_value, pk, '+')
                    result.append((db, k, v))
            
            
        return result
    
    def get_indexer(self, accessor):
        if not accessor in self._indexes:
            if not accessor in self.meta['indexes']:
                raise Exception("No index available for %s" % accessor)
            
            packer = self.meta['indexes'][accessor]
            self._indexes[accessor] = entries.Index(self.key_packer, getattr(packers, packer))
            
        return "{0}.{1}".format(self.name, accessor), self._indexes[accessor]
    
class Transaction(object):
    
    def __init__(self, db, current_commit):
        self.db = db
        self.current_commit = current_commit
        #self._data = SortedList()
        self._cache = {}
        self._updates = {}
        self._checksum = hashlib.sha1()
        
        self._ms = SortedDict()
        
    def get_ms(self, name):
        if not name in self._ms:
            self._ms[name] = MemoryDataStore(self.current_commit)
            
        return self._ms[name]
    
    def get_datastores(self, name):
        return [self.get_ms(name), self.db.get_rds(name)]
        
    def get(self, collection, key, default=None, track=True):
        c = self.db.get_collection(collection)
        try:
            return c.entry.get(self.get_datastores(collection), key, end_commit=self.current_commit)
        except KeyError:
            return default
    
    def put(self, collection, pk, value, index=True, track=True):
        
        #print "PUT", collection, pk, value
        
        if track:
            old_value = self.get(collection, pk, track=track)
        
            if value == old_value:
                logger.debug("Skipping put - no changes")
                return False
        
        
        c = self.db.get_collection(collection)
        
        #self._data.add((collection, ) + c.entry.prepare(pk, value))
        self.get_ms(collection).store([c.entry.prepare(pk, value)], self.current_commit)
        
        if track and index:
            for n, k, v in c.index(pk, value, old_value):
                self.get_ms(n).store([(k, v)], self.current_commit)
        
        self._updates.setdefault(collection, set()).add(pk)
        
        return True
        
        
    def delete(self, collection, pk):
        self.put(collection, pk, None)
        
    def keys(self, collection):
        c = self.db.get_collection(collection)
        return c.entry.keys(self.get_datastores(collection), end_commit=self.current_commit)
        
    def lookup(self, collection, accessor, start_key, end_key=None):
        c = self.db.get_collection(collection)
        
        db, indexer = c.get_indexer(accessor)
        return indexer.lookup(self.get_datastores(db), start_key, end_key, end_commit=self.current_commit)
        
    def commit(self, c=None):
        
        if c is None: c = self.current_commit
        
        # this throws an exception if the transaction is not commitable
        try:
            self.current_commit = self.db.do_commit(c)
        except RepriseDBIntegrityError:
            # try and resolve them
            if self.conflicts() is None:
                self.current_commit = self.db.do_commit(self.current_commit)
            else:
                raise
        
        commit = {'updates': { k: list(s) for k, s in self._updates.iteritems() },
                  'checksum': self._checksum.digest()}
        
        self.put('_commits', self.current_commit, commit, track=False)
        self.put('_commits', 0, self.current_commit, track=False)
        
        # send the data to the datastores
        for n, ms in self._ms.iteritems():
            self.db.get_rds(n).store(ms._data.iteritems(), self.current_commit)
        
        self._ms.clear()
        self._cache = {}
        self._updates = {}
        
        return self.current_commit
    
    def conflicts(self):
        result = []
        
        current = self.db.current_commit()
        for c in xrange(self.current_commit+1, current+1):
            t = self.db.begin(commit=c)
            commit = t.get('_commits', c)
            
            # compare the changes
            for n, items in commit['updates'].iteritems():
                for k in self._updates.setdefault(n, set()) & set(items):
                    result.append((n, k, c, t.get(n, k)))
            
            if not result:
                logger.debug("Non hlocking commit: %d", c)
                self.current_commit = c
        
        if result == []: return None
        
        return result
        
            
if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG)
    
    db = RepriseDB(path='testing')
    
    db.create_collection('people')
    
    db.add_index('people', 'first_name', 'p_string')
    
    t1 = db.begin()
    
    t1.put('people', 1, {'first_name': 'Bob'})
    print t1.get('people', 1)
    assert t1.get('people', 1)['first_name'] == 'Bob'
    assert t1.keys('people') == [1]
    
    print "COMMIT", t1.commit()
    
    assert t1.lookup('people', 'first_name', 'Boa', 'Bod') == [1]
    assert t1.lookup('people', 'first_name', 'Char', 'Chad') == []
    
    t2 = db.begin()
    assert t2.get('people', 1)['first_name'] == 'Bob'
    assert t2.lookup('people', 'first_name', 'Boa', 'Bod') == [1]
     
    t2.put('people', 1, {'first_name': 'Robert'})
    t2.put('people', 2, {'first_name': 'Andy'})
    t2.put('people', 3, {'first_name': 'Andrew'})
     
    assert t2.get('people', 1)['first_name'] == 'Robert'
    assert t1.get('people', 1)['first_name'] == 'Bob'
     
    assert t2.lookup('people', 'first_name', 'A', 'C') == [2, 3]
    assert t1.lookup('people', 'first_name', 'A', 'C') == [1]
    
    t2.commit()
    
    t1.put('people', 2, {'first_name': 'Dave'})
    t1.commit()
    
#     t2.put('people', 1, {'first_name': 'Robert'})
#     t2.put('people', 2, {'first_name': 'Andy'})
#     assert t2.get('people', 1)['first_name'] == 'Robert'
#     assert t2.get('people', 2)['first_name'] == 'Andy'
#     assert t2.keys('people') == [1, 2]
#     
#     # cross isolation
#     assert t1.get('people', 2) == None
#     
#     c1 = t1.commit()
#     print t1.get('_commits', c1) #== {'people': [1]}
#     
#     assert t1.get('people', 1)['first_name'] == 'Bob'
#     assert t2.get('people', 1)['first_name'] == 'Robert'
#     
#     c2 = t2.commit()
#     print t1.get('people', 1)['first_name']
#     
#     print t2.get('_commits', c2)# == {'people': [1, 2]}
#     
#     t3 = db.begin()
#     
#     db.get_rds('people.first_name').dump()
#     
#     print "1", t3.get('people', 1)
#     print "Bob", t3.lookup('people', 'first_name', 'Bob')
#   
    logging.getLogger().setLevel(logging.WARNING)
    db.drop_collection('people')
    
#     
    
    