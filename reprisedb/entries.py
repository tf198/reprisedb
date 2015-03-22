from . import RepriseDataError, NUL, ONE, DELETED

from reprisedb import packers # @UnusedImport

import logging
logger = logging.getLogger(__name__)

class BaseEntry(object):
    
    def __init__(self, key_packer, value_packer):
        self.key_packer = key_packer
        self.value_packer = value_packer
        
    def __repr__(self):
        return "<Entry key={0} value={1}>".format(self.key_packer, self.value_packer)

class Entry(BaseEntry):
        
    def to_db_key(self, key):
        '''
        >>> Entry(packers.p_uint32, packers.p_string).to_db_key(34)
        '\\x00\\x00\\x00\x22'
        '''
        return self.key_packer.pack(key, index=True)
    
    def from_db_key(self, db_key):
        '''
        >>> Entry(packers.p_uint32, packers.p_string).from_db_key('\\x00\\x00\\x00\\x22')
        34
        '''
        return self.key_packer.unpack(db_key, index=True)
    
    def to_db_value(self, value):
        if value is None:
            return DELETED
        else:
            return self.value_packer.pack(value)
    
    def from_db_value(self, value):
        if value == DELETED:
            raise KeyError("Key deleted")
        
        return self.value_packer.unpack(value)
    
    def prepare(self, pk, value):
        '''
        >>> Entry(packers.p_uint32, packers.p_string).prepare(34, 'Bob')
        ('\\x00\\x00\\x00\x22', 'Bob')
        >>> Entry(packers.p_string, packers.p_string).prepare('foo', 'bar')
        ('foo\\x00', 'bar')
        '''
        return self.to_db_key(pk), self.to_db_value(value)

class BoundEntry(object):
    
    def __init__(self, entry, ds, end_commit=None, start_commit=None):
        self.entry = entry
        self.ds = ds
        self.end_commit = end_commit
        self.start_commit = start_commit
    
    def get(self, pk):
        _r, v = self.ds.get_item(self.entry.to_db_key(pk), self.end_commit, self.start_commit)
        
        if v is not None:
            return self.entry.from_db_value(v)
        
        raise KeyError("Not found in datastore")
    
    def bulk_put(self, d, commit):
        if hasattr(d, 'iteritems'):
            d = d.iteritems()
        
        return self.ds.store(( self.entry.prepare(pk, value) for pk, value in d ), commit)
    
    def iter_get(self, keys):
        packed_keys = ( self.entry.to_db_key(k) for k in keys )
        
        return ( self.entry.from_db_value(v) for k, _r, v in self.ds.iter_get(packed_keys, self.end_commit, self.start_commit))
    
    def iter_items(self, start_key=None, end_key=None):
        if start_key is not None: start_key = self.entry.to_db_key(start_key)
        if end_key is not None: end_key = self.entry.to_db_key(end_key)
        
        return ( (self.entry.from_db_key(k),
                  self.entry.from_db_value(v)) for k, _r, v in self.ds.iter_items(start_key,
                                                                                  end_key,
                                                                                  self.end_commit,
                                                                                  self.start_commit) if v != DELETED )
    
    def iter_keys(self, start_key=None, end_key=None):
        return ( self.entry.from_db_key(k) for k, _r, v in self.ds.iter_items(start_key,
                                                                              end_key,
                                                                              self.end_commit,
                                                                              self.start_commit) if v != DELETED )
    
    def iter_values(self, start_key=None, end_key=None):
        return ( self.entry.from_db_value(v) for k, _r, v in self.ds.iter_items(start_key,
                                                                                end_key,
                                                                                self.end_commit,
                                                                                self.start_commit) if v != DELETED )
        
    def contains(self, pk):
        _r, v = self.ds.get_item(self.entry.to_db_key(pk), self.end_commit, self.start_commit)
            
        return v is not None
        
    def keys(self, start_key=None, end_key=None):
        return list(self.iter_keys(start_key, end_key))

class SimpleIndex(BaseEntry):
    
    def to_db_key(self, value, pk):
        '''
        >>> SimpleIndex(packers.p_uint32, packers.p_string).to_db_key('Bob', 34)
        'Bob\\x00\\x00\\x00\\x00\x22'
        
        >>> SimpleIndex(packers.p_string, packers.p_string).to_db_key('foo', 'bar')
        'foo\\x00bar\\x00\\x04'
        '''
        key = self.value_packer.pack(value, index=True)
        return self.key_packer.append_last(key, pk)
    
    def from_db_key(self, db_key):
        '''
        >>> SimpleIndex(packers.p_uint32, packers.p_string).from_db_key('Bob\\x00\\x00\\x00\\x00\\x22')
        ('Bob', 34)
        >>> SimpleIndex(packers.p_string, packers.p_string).from_db_key('foo\\x00bar\\x00\\x04')
        ('foo', 'bar')
        '''
        value, pk = self.key_packer.extract_last(db_key)
        return value[:-1], pk
    
    def prepare(self, value, pk, mark):
        '''
        >>> SimpleIndex(packers.p_uint32, packers.p_string).prepare('Bob', 34, '+')
        ('Bob\\x00\\x00\\x00\\x00\x22', '+')
        '''
        if not mark in ['+', '-']:
            raise RepriseDataError("Mark should be one of '+' or '-'")
        return self.to_db_key(value, pk), mark
    
    def key_range(self, start_key, end_key=None):
        '''
        >>> SimpleIndex(packers.p_uint32, packers.p_string).key_range('Boa', 'Bod')
        ('Boa\\x00', 'Bod\\x00')
        >>> SimpleIndex(packers.p_uint32, packers.p_string).key_range('Bob')
        ('Bob\\x00', 'Bob\\x01')
        '''
        a = self.value_packer.pack(start_key)
        
        if end_key is None:
            b = a + ONE
        else:
            b = self.value_packer.pack(end_key) + NUL
        
        return a + NUL, b

class BoundIndex(object):
    
    def __init__(self, index, ds, end_commit=None, start_commit=None):
        self.index = index
        self.ds = ds
        self.end_commit = end_commit
        self.start_commit = start_commit
        
    def add(self, value, key, mark, commit):
        return self.ds.store([self.index.prepare(value, key, mark)], commit)
    
    def bulk_add(self, l, commit):
        return self.ds.store(( self.index.prepare(*x) for x in l ), commit)
        
    def iter_lookup_keys(self, start_key, end_key=None):
        start_key, end_key = self.index.key_range(start_key, end_key)
        return ( self.index.from_db_key(k)[1] for k, _r, v in self.ds.iter_items(start_key,
                                                                                 end_key,
                                                                                 self.end_commit,
                                                                                 self.start_commit) if v == '+' )
    
    def lookup(self, start_key, end_key=None):
        return list(self.iter_lookup_keys(start_key, end_key))
    
if __name__ == '__main__':
    import doctest
    print doctest.testmod()
