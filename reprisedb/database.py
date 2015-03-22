from reprisedb import packers, entries, drivers, utils, datastore

from contextlib import contextmanager
import hashlib
import itertools

from sortedcontainers import SortedDict

import logging
logger = logging.getLogger(__name__)

class RepriseDBIntegrityError(Exception): pass

class RepriseDB(object):
    
    META_META = {'name': '_meta', 
                 'key_packer': 'p_string',
                 'value_packer': 'p_obj',
                 'indexes': {}}
    
    COMMIT_META = {'name': '_commits',
                   'key_packer': 'p_uint32',
                   'value_packer': 'p_obj',
                   'indexes': {}}
    
    def __init__(self, **config):
        
        path = config.pop('path', 'data')
         
        driver = config.pop('driver', drivers.LMDBDriver)
         
        self.driver = driver(path, **config)
        
        self._collections = {}
        
        self.meta_entry = entries.Entry(packers.p_string, packers.p_obj)
        
        self._rds = {}
        
        self._current_commit = 0 # need to set to something
        t = self.begin()
        self._current_commit = t.get('_commits', 0)
       
        if self._current_commit is None:
            logger.debug("Initialising database")
            self._current_commit = 0
            t.put('_meta', 'info:version', '0.1.1', False)
            assert t.commit(0) == 1
        
        logger.debug("Current commit: %s", self._current_commit)
    
    def meta_key(self, collection):
        return "collection:{0}".format(collection)
    
    def get_rds(self, name):
        if not name in self._rds:
            self._rds[name] = datastore.RevisionDataStore(self.driver.get_db(name), self._current_commit)
        return self._rds[name]
    
    def current_commit(self):
        return self._current_commit
    
    def begin(self, commit=None):
        if commit is None: commit = self._current_commit
        return Transaction(self, commit)
        
    def _try_commit(self, commit):
        
        if commit != self._current_commit:
            raise RepriseDBIntegrityError("Current commit is %d" % self._current_commit)
        
        self._current_commit += 1
        return self._current_commit
              
class Collection(object):
    '''
    A collection is an Entry (controlled by a key_packer and value_packer) and
    its associated indexes.
    '''
    
    def __init__(self, meta):
        self.meta = meta
        self.name = meta['name']
        self.key_packer = getattr(packers, meta['key_packer'])
        self.value_packer = getattr(packers, meta['value_packer'])
        
        self.entry = entries.Entry(self.key_packer, self.value_packer)
        
        self._indexes = {}
    
    def index_item(self, pk, new_item, old_item):
        result = []
        
        for accessor in self.meta['indexes']:
            new_value = utils.dotted_accessor(new_item, accessor)
            old_value = utils.dotted_accessor(old_item, accessor)
            
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
                raise KeyError("No index available for %s" % accessor)
            
            index_type, value_packer = self.meta['indexes'][accessor]
            index_cls = getattr(entries, index_type)
            self._indexes[accessor] = index_cls(self.key_packer, packers.registry[value_packer])
            
        return "{0}.{1}".format(self.name, accessor), self._indexes[accessor]
    
class Transaction(object):
    
    def __init__(self, db, current_commit):
        self.db = db
        self.current_commit = current_commit
        
        self._datastores = SortedDict()
        self._updates = {}
        
        self._collections = {}
        
        self._collections['_meta'] = Collection(self.db.META_META)
        self._collections['_commits'] = Collection(self.db.COMMIT_META)
        self._post_commit = []
        
    def get_collection(self, name):
        
        if not name in self._collections:
            # manually pull the meta
            meta_entry = entries.BoundEntry(self.db.meta_entry, self.db.get_rds('_meta'), self.current_commit)
            meta = meta_entry.get(self.db.meta_key(name))
            self._collections[name] = Collection(meta)
        
        return self._collections[name]

    def create_collection(self, name, key_packer='p_uint32', value_packer='p_dict'):
         
        meta_key = self.db.meta_key(name)
         
        meta = self.get('_meta', meta_key)
         
        if meta is not None:
            raise Exception("Already a collection named %s" % name)
         
        meta = {'name': name,
                'key_packer': key_packer,
                'value_packer': value_packer,
                'indexes': {}}
         
        self.put('_meta', meta_key, meta)
         
        self._collections[name] = Collection(meta)
         
        return self._collections[name]
    
    def drop_collection(self, name):
        c = self.get_collection(name)        
        
        to_remove = [ '{0}.{1}'.format(name, accessor) for accessor in c.meta['indexes'] ]
        to_remove.append(name)
        
        def cleanup():
            logger.debug("Cleaning up database files")
            for f in to_remove:
                self.db.driver.drop_db(f)
        
        self._post_commit.append(cleanup)
        self.delete('_meta', self.db.meta_key(name))
        
        del self._collections[name]
      
    def list_collections(self):
        return [ x[11:] for x in self.keys('_meta', start_key='collection:', end_key='collection:~') ]
    
    def add_index(self, collection, accessor, value_packer='string'):
        c = self.get_collection(collection)
         
        current = c.meta['indexes']
        if accessor in current:
            raise Exception("Already an index on %s" % accessor)
         
        if not value_packer in packers.registry:
            raise Exception("Unknown value packer: %s" % value_packer)
         
        current[accessor] = ['SimpleIndex', value_packer]
        
        db, indexer = c.get_indexer(accessor)
        
        rds = self.db.get_rds(db)
        
        def g():
            pk = pr = pv = None
            
            ds = self.get_datastore(collection)
            ds = ds.datastores[1] # TODO: FIX
            
            for k, r, v in ds.iter_revisions(end_revision=self.current_commit):
                k = c.entry.from_db_key(k)
                v = c.entry.from_db_value(v)
                
                # will get them in latest to earliest order
                if k == pk:
                    # add an un-index for the previous key
                    data = indexer.prepare(pv, pk, '-')
                    yield data[0] + pr, data[1]
                    
                v = utils.dotted_accessor(v, accessor)
                
                if v is not None:
                
                    data = indexer.prepare(v, k, '+')
                    #print "DATA", data
                    yield data[0] + r, data[1]
                
                pk, pr, pv = k, r, v
            
        rds.raw_store(g())
         
        self.put('_meta', self.db.meta_key(collection), c.meta)
        
    def drop_index(self, collection, accessor):
        c = self.get_collection(collection)
        
        db, _indexer = c.get_indexer(accessor)
        
        def cleanup():
            logger.debug("Removing index datafile for %s", db)
            self.db.driver.drop_db(db)
        
        del c.meta['indexes'][accessor]
        del c._indexes[accessor]
        
        self._put('_meta', self.db.meta_key(collection), c.meta)
    
    def get_datastore(self, name):
        if not name in self._datastores:
            mds = datastore.MemoryDataStore()
            
            self._datastores[name] = datastore.ProxyDataStore((mds, self.db.get_rds(name)))
        return self._datastores[name]
    
    def get_entry(self, collection):
        return entries.BoundEntry(self.get_collection(collection).entry, self.get_datastore(collection), self.current_commit)
    
    def get_index(self, collection, accessor):
        c = self.get_collection(collection)
        db, indexer = c.get_indexer(accessor)
        return entries.BoundIndex(indexer, self.get_datastore(db), self.current_commit)
    
    def get(self, collection, key, default=None):
        ' convenience method for t.get_entry(collection).get(key) '
        try:
            return self.get_entry(collection).get(key)
        except KeyError:
            return default
    
    def each(self, collection, start_key=None, end_key=None):
        return self.get_entry(collection).iter_items(start_key, end_key)
    
    def put(self, collection, pk, value, index=True, track=True):
        
        #print "PUT", collection, pk, value
        
        if track:
            old_value = self.get(collection, pk)
        
            if value == old_value:
                logger.debug("Skipping put - no changes")
                return False
        
        
        c = self.get_collection(collection)
        
        #self._data.add((collection, ) + c.entry.prepare(pk, value))
        self.get_datastore(collection).store([c.entry.prepare(pk, value)])
        
        if track and index:
            for n, k, v in c.index_item(pk, value, old_value):
                self.get_datastore(n).store([(k, v)])
        
        self._updates.setdefault(collection, set()).add(pk)
        
        return True
    
    def bulk_put(self, collection, items, index=True, track=True):
        
        if hasattr(items, 'iteritems'):
            items = items.iteritems()
            
        for k, v in items:
            self.put(collection, k, v, index, track)
    
    def delete(self, collection, pk):
        self.put(collection, pk, None)
        
    def keys(self, collection, start_key=None, end_key=None):
        return self.get_entry(collection).keys(start_key, end_key)

    def lookup(self, collection, accessor, start_key, end_key=None, offset=0, length=None):
        
        index = self.get_index(collection, accessor)
        
        i = index.iter_lookup_keys(start_key, end_key)
        
        if offset or length:
            if length is not None: length += offset
            i = itertools.islice(i, offset, length)
        
        return list(i)
    
    def index(self, collection, accessor):
        c = self.get_collection(collection)
        
        ds = self.get_datastore(collection)
        
        db, indexer = c.get_indexer(accessor)
        
        def g():
            pk, pr, pv = None
            
            for k, r, v in ds.iter_revisions(end_revision=self._current_commit):
                # will get them in latest to earliest order
                if k == pk:
                    # add an un-index for the previous key
                    yield indexer.prepare(pv, pk, '-')
                    
                v = utils.dotted_accessor(v, accessor)
                
                yield indexer.prepare(v, k, '+')
                
                pk, pr, pv = k, r, v
                
        return g()
        
    def commit(self, c=None, autoresolve=True):
        
        if c is None: c = self.current_commit
        
        # this throws an exception if the transaction is not commitable
        try:
            self.current_commit = self.db._try_commit(c)
        except RepriseDBIntegrityError:
            # try and resolve them
            if autoresolve and self.conflicts() is None:
                self.current_commit = self.db._try_commit(self.current_commit)
            else:
                raise
        
        commit = {'updates': { k: list(s) for k, s in self._updates.iteritems() },
                  'checksum': ""}
        #
        self.put('_commits', self.current_commit, commit, track=False)
        self.put('_commits', 0, self.current_commit, track=False)
        
        # send the data to the datastores
        for n, ds in self._datastores.iteritems():
            ms = ds.datastores[0]
            self.db.get_rds(n).store(ms.iteritems(), self.current_commit)
        
        for m in self._post_commit:
            m()
        
        self._post_commit = []
        self._datastores.clear()
        self._updates = {}
        
        return self.current_commit
    
    def conflicts(self):
        result = []
        
        logger.debug("Checking for conflicts: %r", self._updates)
        
        current = self.db.current_commit()
        for c in xrange(self.current_commit+1, current+1):
            t = self.db.begin(commit=c)
            commit = t.get('_commits', c)
            
            # compare the changes
            for n, items in commit['updates'].iteritems():
                for k in self._updates.setdefault(n, set()) & set(items):
                    logger.debug("CONFLICT: %r, %r, %r", n, k, c)
                    result.append((n, k, c))
            
            if not result:
                logger.debug("Non blocking commit: %d", c)
                self.current_commit = c
        
        if result == []:
            logger.debug("No blocking commits found")
            return None
        
        logger.debug("Blocking commits: %r", result)
        return result
        
            
if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG)
    
    db = RepriseDB(path='testing')
    
    t1 = db.begin()
    
    t1.create_collection('people')
    
    t1.add_index('people', 'first_name', 'p_string')

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
    try:
        t1.commit()
    except RepriseDBIntegrityError:
        pass
    
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
    t = db.begin()
    t.drop_collection('people')
    t.commit()
    
#     
    
    