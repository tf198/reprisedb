from sortedcontainers import SortedDict

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
                        
                        if k != key or r > last: r = v = None
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
                    return None, None
                
                k, v = c.item()
        
        k, r = k[:-4], k[-4:]
        
        #logger.debug("get_item(%r, %r, %r): %r => %r [%r]", key, end_revision, start_revision, k, r, v)
        
        if k != key: return None, None
        if r > last: return r, None
                
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
    
    def iter_revisions(self, start_revision, end_revision=None):
        '''
        Generator yielding (key, packed revision, value)
        '''
        
        first = self.revision_packer.pack(end_revision or start_revision + 1)
        last = self.revision_packer.pack(start_revision)
        
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
        
class TransactionDataStore(object):
    '''
    Wraps a DataStore object and proxies calls.
    '''
    
    def __init__(self, ds):
        self._data = SortedDict()
        self.parent = ds
        
    def store(self, data):
        self._data.update(data)
        
    def get_item(self, key, end_revision=None, start_revision=None):
        try:
            return None, self._data[key]
        except KeyError:
            return self.parent.get_item(key, end_revision, start_revision)
    
    def iter_get(self, keys, end_revision=None, start_revision=None):
        
        for k, r, v in self.parent.iter_get(keys, end_revision, start_revision):
            try:
                yield k, None, self._data[k]
            except KeyError:
                yield k, r, v
    
    def iter_items(self, start_key='\x00', end_key=None, end_revision=None, start_revision=None):
        
        i_self = self._data.iteritems()
        i_ds = self.parent.iter_items(start_key, end_key, end_revision, start_revision)
        
        try:
            k_ds, r_ds, v_ds = i_ds.next()
        except StopIteration:
            logger.debug("No proxy items")
            k_ds = None
        
        for k_self, v_self in i_self:
            # advance to first key
            if k_self < start_key: continue
            
            try:
                if k_ds is not None:
                    while k_ds < k_self:
                        logger.debug("YIELD PROXY %r => %r", k_ds, v_ds)
                        yield k_ds, r_ds, v_ds
                        k_ds, r_ds, v_ds = i_ds.next()
                        
                    if k_ds == k_self:
                        k_ds, r_ds, v_ds = i_ds.next()
            except StopIteration:
                k_ds = None
            
            if end_key and k_self > end_key: break
            
            logger.debug("YIELD LOCAL %r => %r", k_self, v_self)
            yield k_self, None, v_self
            
        logger.debug("Finished local items")
            
        # run out the rest of the ds iterator
        while k_ds:
            logger.debug("YIELD PROXY %r => %r", k_ds, v_ds)
            yield k_ds, r_ds, v_ds
            k_ds, r_ds, v_ds = i_ds.next()
            
        logger.debug("Finished proxy items")

