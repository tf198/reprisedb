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

    def __init__(self, env, current_commit):
        self.env = env
        self.current_commit = current_commit

    def store(self, data, commit):
        '''
        data should be a list of (key, value) tuples
        '''
        if self.current_commit > commit:
            raise Exception("Cannot commit version %d, already at %d" % (commit, self.current_commit))
        
        items = ( (self.revision_packer.append_last(key, commit), value) for key, value in data )
        with self.env.begin(write=True) as txn:
            with txn.cursor() as c:
                consumed, _added = c.putmulti(items)
        
        self.current_commit = commit
        
        return consumed

    def get_item(self, key, end_commit=None, start_commit=None):
        '''
        Returns two tuple of (packed commit, value) or None
        '''
        
        # commits are inverted
        last = self.revision_packer.pack(start_commit or 0)
        first= self.revision_packer.pack(end_commit or self.revision_packer.max)
        
        with self.env.begin() as txn:
            with txn.cursor() as c:
                #logger.debug("SET_RANGE: %r", key+first)
                if not c.set_range(key + first):
                    #logger.debug("get_item(%r, %r, %r): item not found", key, end_commit, start_commit)
                    return None, None
                
                k, v = c.item()
        
        k, r = self.revision_packer.extract_last(k)
        
        #logger.debug("get_item(%r, %r, %r): %r => %r [%r]", key, end_commit, start_commit, k, r, v)
        
        if k != key: return None, None
        if r > last: return r, None
                
        return r, v

    def iter_items(self, end_commit=None, start_commit=None, start_key='', end_key=None):
        
        last = self.revision_packer.pack(start_commit or 0)
        first = self.revision_packer.pack(end_commit or self.revision_packer.max)
        
        with self.env.begin() as txn:
            with txn.cursor() as c:
                c.set_range(start_key)
                while True:
                    key = c.key()
                    
                    if key[-4:] < first: # out of range - jump to next
                        if not c.set_range(key[:-4] + first):
                            break
                        key = c.key()
                        
                    if end_key and key > end_key: break
                    
                    k, r = key[:-4], key[-4:]
                    
                    # if in range then yield
                    if r < last:
                        yield k, r, c.value()
                    
                    # seek to next item
                    if not c.set_range(k + '\xFF\xFF\xFF\xFF'):
                        break
            
    def stat(self):
        return self.env.stat()
    
    def info(self):
        return self.env.info()
    
    def iter_commits(self, start_commit, end_commit=None):
        '''
        Generator yielding (key, packed revision, value)
        '''
        
        first = self.revision_packer.pack(end_commit or start_commit + 1)
        last = self.revision_packer.pack(start_commit)
        
        with self.env.begin() as txn:
            with txn.cursor() as c:
                for k, v in iter(c):
                    k, r = k[:-4], k[-4:]
                    if r <= last and r > first:
                        yield k, r, v
                        
    def commits(self,  start_commit, end_commit=None):
        return [ (k, v) for k, _rp, v in self.iter_commits(start_commit, end_commit) ]
    
    def dump(self):
        print "=== ENV: %s ===", self.env.path()
        with self.env.begin() as txn:
            with txn.cursor() as c:
                for dk, v in c.iternext():
                    k, r = self.revision_packer.extract_last(dk)
                    print "%r => %r [%d]" % (k, v, r)
        print "=== END ENV ==="
        
class MemoryDataStore(object):
    
    def __init__(self, commit):
        self._data = SortedDict()
        self.commit = commit
        
    def store(self, data, commit):
        self._data.update(data)
        self.commit = commit
        
    def get_item(self, key, end_commit=None, start_commit=None):
        try:
            return self.commit, self._data[key]
        except KeyError:
            return None, None
    
    def iter_items(self, end_commit=None, start_commit=None, start_key='', end_key=None):
        for k, v in self._data.iteritems():
            if k < start_key: continue
            if end_key and k > end_key: break
            
            yield k, self.commit, v
