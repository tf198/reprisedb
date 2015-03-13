'''
A DataStore implements a basic interface

    store(data, revision)
    get_item(key, end_revision=None, start_revision=None)
    iter_get(keys, end_revision=None, start_revision=None)
    iter_items(start_key=None, end_key=None, end_revision=None, start_revision=None)
    
'''

from sortedcontainers import SortedDict
import zipfile, base64
import packers
import logging
logger = logging.getLogger(__name__)

class RevisionDataStore(object):
    '''
    Revision based datastore using LMDB backend.
    
    btkeys are in the format '<key><revision>' where revision is a 4 byte inverse uint32
    '''
    
    revision_packer = packers.p_revision

    def __init__(self, env, current_revision):
        self.env = env
        self.current_revision = current_revision

    def unpack_revision(self, r):
        ' Returns revision as an integer '
        return self.revision_packer.unpack(r)

    def store(self, data, revision):
        '''
        data should be a list of (key, value) tuples
        '''
        if self.current_revision > revision:
            raise Exception("Cannot revision version %d, already at %d" % (revision, self.current_revision))
        
        items = ( (self.revision_packer.append_last(key, revision), value) for key, value in data )
        with self.env.begin(write=True) as txn:
            with txn.cursor() as c:
                consumed, _added = c.putmulti(items)
        
        self.current_revision = revision
        
        return consumed

    def iter_get(self, keys, end_revision=None, start_revision=None):
        '''
        Generator that yields three tuples of (key, packed_revision, value) for every
        key in `keys`.  Keys *must* be naturally sorted.
        '''
        last = self.revision_packer.pack(start_revision or 0)
        first= self.revision_packer.pack(end_revision or self.revision_packer.max)
        
        logger.debug("RANGE: %r -> %r", first, last)
        
        with self.env.begin() as txn:
            with txn.cursor() as c:
                for key in keys:
                    logger.debug("SET_RANGE: %r", key+first)
                    if c.set_range(key + first):
                        k, v = c.item()
                        k, r = k[:-4], k[-4:]
                        logger.debug("ITEM %r %r %r", k, r, v)
                        
                        if k != key or r > last: 
                            r = v = None
                    else:
                        logger.debug("get_item(%r, %r, %r): item not found", key, end_revision, start_revision)
                        r = v = None
                    yield key, r, v
                
        logger.debug("get_item(%r, %r, %r): %r => %r [%r]", key, end_revision, start_revision, k, r, v)
        
        

    def get_item(self, key, end_revision=None, start_revision=None):
        '''
        Returns two tuple of (packed revision, value) or None
        '''
        
        # revisions are inverted
        last = self.revision_packer.pack(start_revision or 0)
        first= self.revision_packer.pack(end_revision or self.revision_packer.max)
        
        #logger.debug("RANGE: %r -> %r", first, last)
        
        with self.env.begin() as txn:
            with txn.cursor() as c:
                #logger.debug("SET_RANGE: %r", key + first)
                if not c.set_range(key + first):
                    #logger.debug("get_item(%r, %r, %r): item not found", key, end_revision, start_revision)
                    raise KeyError("Key not found")
                
                k, v = c.item()
        
        k, r = k[:-4], k[-4:]
        
        #logger.debug("get_item(%r, %r, %r): %r => %r [%r]", key, end_revision, start_revision, k, r, v)
        
        if k != key or r > last:
            raise KeyError("Key not found")
                
        return r, v

    def iter_items(self, start_key=None, end_key=None, end_revision=None, start_revision=None):
        '''
        Generator that yields three tuples of (key, packed_revision, value) where `key >= start_key`
        and `key < end_key`.
        '''
        
        last = self.revision_packer.pack(start_revision or 0)
        first = self.revision_packer.pack(end_revision or self.revision_packer.max)
        
        #logger.debug("ITER_ITEMS %r - %r", first, last)
        
        with self.env.begin() as txn:
            with txn.cursor() as c:
                if start_key:
                    if not c.set_range(start_key):
                        #logger.debug("No matches")
                        return
                else:
                    c.first()
                
                while True:
                    key = c.key()
                    #logger.debug("KEY: %r", key)
                    
                    if key[-4:] < first: # out of range - jump to next
                        #logger.debug("SKIPPING TO %r", key[:-4] + first)
                        if not c.set_range(key[:-4] + first):
                            break
                        new_key = c.key()
                        if new_key[:-4] > key[:-4]:
                            #logger.debug("No valid key for this commit %r > %r", new_key[:-4], key[:-4])
                            continue
                        key = new_key
                    #logger.debug("NEW KEY: %r", key)
                        
                    if end_key and key > end_key: break
                    
                    k, r = key[:-4], key[-4:]
                    
                    # if in range then yield
                    if r <= last:
                        #logger.debug("YIELD: %r %r %r", k, r, c.value())
                        yield k, r, c.value()
                    
                    # seek to next item
                    #logger.debug("SEEK: %r", k+'\xFF\xFF\xFF\xFF')
                    if not c.set_range(k + '\xFF\xFF\xFF\xFF'):
                        break
            
    def stat(self):
        return self.env.stat()
    
    def info(self):
        return self.env.info()
    
    def iter_revisions(self, start_revision=None, end_revision=None):
        '''
        Generator yielding (key, packed revision, value)
        '''
        
        first = self.revision_packer.pack(end_revision or self.revision_packer.max)
        last = self.revision_packer.pack(start_revision or 0)
        
        logger.debug("RANGE: %r => %r", first, last)
        
        with self.env.begin() as txn:
            with txn.cursor() as c:
                for k, v in iter(c):
                    k, r = k[:-4], k[-4:]
                    if r <= last and r > first:
                        yield k, r, v
                        
    def revisions(self,  start_revision, end_revision=None):
        return [ (k, v) for k, _rp, v in self.iter_revisions(start_revision, end_revision) ]
    
    def dump(self):
        print "=== ENV: %s ===", self.env.path()
        with self.env.begin() as txn:
            with txn.cursor() as c:
                for dk, v in c.iternext():
                    k, r = self.revision_packer.extract_last(dk)
                    print "%r => %r [%d]" % (k, v, r)
        print "=== END ENV ==="

class MemoryDataStore(object):
    '''
    A dummy datastore which doesn't implement revisioning
    Used as the working space for a transaction.
    '''
    
    current_revision = '\x00\x00\x00\x00' # last possible revision
    
    def __init__(self, backend=None, current_revision=None):
        if backend is None:
            backend = SortedDict()
        self._data = backend
        
        if current_revision is not None:
            self.current_revision = packers.p_revision.pack(current_revision)
        
    def store(self, data, revision=None):
        self._data.update(data)
        if revision:
            self.current_revision = packers.p_revision.pack(revision)
        
    def get_item(self, key, end_revision=None, start_revision=None):
        return self.current_revision, self._data[key]
    
    def iter_get(self, keys, **kwargs):
        for k in keys:
            try:
                yield k, self.current_revision, self._data[k]
            except KeyError:
                yield k, self.current_revision, None
            
    def iter_items(self, start_key=None, end_key=None, end_revision=None, start_revision=None):
        
        for k, v in self._data.iteritems():
            if k < start_key: continue
            if end_key and k >= end_key: break
            
            yield k, self.current_revision, v
        
class ArchiveDataStore(RevisionDataStore):
    '''
    Extension of RevisionDataStore but stores actual data in a zip archive.
    '''
    
    def __init__(self, env, archive, current_commit):
        self._archive = archive
        super(ArchiveDataStore, self).__init__(env, current_commit)
        
    def store(self, data, revision):
        self.archive(( (k, self.revision_packer.pack(revision), v) for k, v in data ))
            
    def archive(self, data):
        '''
        data should be an iterable of three tuples (key, packed_revision, value)
        '''
        with self.env.begin(write=True) as txn:
            with zipfile.ZipFile(self._archive, 'a') as zf:
                for k, r, v in data:
                    filename = base64.b64encode(k + r)
                    zf.writestr(filename, v)
                    txn.put(k + r, filename)
                
    def get_item(self, key, end_revision=None, start_revision=None):
        r, filename = super(ArchiveDataStore, self).get_item(key, end_revision, start_revision)
        with zipfile.ZipFile(self._archive, 'r') as zf:
            return r, zf.read(filename)
    
    def iter_extract(self, i):
        with zipfile.ZipFile(self._archive, 'a') as zf:
            for k, r, v in i:
                if v is None:
                    yield k, r, v
                else:
                    yield k, r, zf.read(v)
    
    def iter_get(self, keys, end_revision=None, start_revision=None):
        return self.iter_extract(super(ArchiveDataStore, self).iter_get(keys, end_revision, start_revision))
                    
    def iter_items(self, start_key=None, end_key=None, end_revision=None, start_revision=None):
        return self.iter_extract(super(ArchiveDataStore, self).iter_items(start_key, end_key, end_revision, start_revision))

class ProxyDataStore(object):
    '''
    Uses a stack of different datastores behaving as one
    Common uses include transactions (MemoryDataStore, RevisionDataStore) and 
    loading archives (ArchiveDataStore(1), ArchiveDataStore(2), ...)
    '''
    
    def __init__(self, datastores):
        self.datastores = datastores
        
    def store(self, data, commit=None):
        # just pass to top level datastore
        return self.datastores[0].store(data, commit)
    
    def get_item(self, key, end_revision=None, start_revision=None):
        for ds in self.datastores:
            try:
                return ds.get_item(key, end_revision, start_revision)
            except KeyError:
                pass
        raise KeyError("Key not found in any datastore")
    
    def iter_items(self, start_key=None, end_key=None, end_revision=None, start_revision=None):
        '''
        Create an iterator pool.
        '''
        
        # key, revision, value iterator
        items = [ ( None, None, None, ds.iter_items(start_key, end_key, end_revision, start_revision)) for ds in self.datastores ]
        
        while items:
            # find the lowest one and yield it
            lk, r, v, i = min(items)
            if lk is not None:
                yield lk, r, v
            
            new_items = []
            for k, r, v, i in items:
                try:
                    if k == lk:
                        new_items.append(i.next() + (i, ))
                    else:
                        new_items.append((k, r, v, i))
                except StopIteration:
                    pass
            
            items = new_items
            
    def iter_get(self, keys, end_revision=None, start_revision=None):
        
        for key in keys:
            found = False
            for ds in self.datastores:
                try:
                    yield (key, ) + ds.get_item(key, end_revision, start_revision)
                    found = True
                    break
                except KeyError:
                    pass
            if not found:
                yield key, None, None
                
                
        
