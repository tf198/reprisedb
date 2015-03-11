from . import RepriseDataError, NUL, ONE, DELETED

from sortedcontainers import SortedDict, SortedList

import logging
logger = logging.getLogger(__name__)

from reprisedb import packers

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
    
    def bulk_prepare(self, d):
        return [ self.prepare(k, d[k]) for k in d ]
    
    def prepare(self, pk, value):
        '''
        >>> Entry(packers.p_uint32, packers.p_string).prepare(34, 'Bob')
        ('\\x00\\x00\\x00\x22', 'Bob')
        >>> Entry(packers.p_string, packers.p_string).prepare('foo', 'bar')
        ('foo\\x00', 'bar')
        '''
        return self.to_db_key(pk), self.to_db_value(value)
    
    def get(self, ds, pk, end_commit=None, start_commit=None):
        _r, v = ds.get_item(self.to_db_key(pk), end_commit, start_commit)
        
        if v is not None:
            return self.from_db_value(v)
        
        raise KeyError("Not found in datastore")
        
    def contains(self, ds, pk, end_commit=None, start_commit=None):

        _r, v = ds.get_item(self.to_db_key(pk), end_commit, start_commit)
            
        return v is not None
        
    def keys(self, ds, end_commit=None, start_commit=None):
        
        result = (( (k, v) for k, _r, v in ds.iter_items(end_commit, start_commit) ))
            
        return [ self.from_db_key(k) for k, v in result if v != DELETED ]

class Index(BaseEntry):
    
    def to_db_key(self, value, pk):
        '''
        >>> Index(packers.p_uint32, packers.p_string).to_db_key('Bob', 34)
        'Bob\\x00\\x00\\x00\\x00\x22'
        
        >>> Index(packers.p_string, packers.p_string).to_db_key('foo', 'bar')
        'foo\\x00bar\\x00\\x04'
        '''
        key = self.value_packer.pack(value, index=True)
        return self.key_packer.append_last(key, pk)
    
    def from_db_key(self, db_key):
        '''
        >>> Index(packers.p_uint32, packers.p_string).from_db_key('Bob\\x00\\x00\\x00\\x00\\x22')
        ('Bob', 34)
        >>> Index(packers.p_string, packers.p_string).from_db_key('foo\\x00bar\\x00\\x04')
        ('foo', 'bar')
        '''
        value, pk = self.key_packer.extract_last(db_key)
        return value[:-1], pk
    
    def prepare(self, value, pk, mark):
        '''
        >>> Index(packers.p_uint32, packers.p_string).prepare('Bob', 34, '+')
        ('Bob\\x00\\x00\\x00\\x00\x22', '+')
        '''
        if not mark in ['+', '-']:
            raise RepriseDataError("Mark should be one of '+' or '-'")
        return self.to_db_key(value, pk), mark
    
    def key_range(self, start_key, end_key=None):
        '''
        >>> Index(packers.p_uint32, packers.p_string).key_range('Boa', 'Bod')
        ('Boa\\x00', 'Bod\\x00')
        >>> Index(packers.p_uint32, packers.p_string).key_range('Bob')
        ('Bob\\x00', 'Bob\\x01')
        '''
        a = self.value_packer.pack(start_key)
        
        if end_key is None:
            b = a + ONE
        else:
            b = self.value_packer.pack(end_key) + NUL
        
        return a + NUL, b
    
    def lookup(self, ds, start_key, end_key=None, end_commit=None, start_commit=None):
        start_key, end_key = self.key_range(start_key, end_key)
        
        result = (( (k, v) for k, _r, v in ds.iter_items(end_commit, start_commit, start_key, end_key) ))
        
        return SortedList(( self.from_db_key(k)[1] for k, v in result if v == '+' ))

if __name__ == '__main__':
    import doctest
    print doctest.testmod()
    
    import os.path
    from datastore import RevisionDataStore
    
    import drivers
    
    for f in os.listdir('testing'):
        os.unlink(os.path.join('testing', f))
    
    db = drivers.LMDBDriver('testing')
    #db = drivers.MemoryDriver()
    
    d = RevisionDataStore(db.get_db('names'), 0)
    
    entry = Entry(packers.p_string, packers.p_string)
    
    assert repr(entry) == "<Entry key=<StringPacker> value=<StringPacker>>"
    
    d.store(entry.bulk_prepare({'134': 'Andy',
                                '156': 'Bob',
                                '148': 'Charlie'}), 1)
    
    assert entry.get(d, '148') == "Charlie"
    
    assert entry.keys(d) == ['134', '148', '156']
     
    d.store(entry.bulk_prepare({"134": 'Andrew',
                                "101": 'Dave',
                                "156": None}), 2)
    
    assert entry.keys(d) == ["101", "134", "148"]
    assert entry.keys(d, 1) == ["134", "148", "156"]
       
    assert entry.get(d, "148") == "Charlie"
    assert entry.get(d, "134") == 'Andrew'
    assert entry.get(d, "134", 1) == 'Andy'
    #assert entry.get(d, "156") == None
    assert entry.get(d, "156", 1) == 'Bob'
    
    #d.dump()
      
    #print d.commits(1)
      
    d = RevisionDataStore(db.get_db('people'), 0)
    entry = Entry(packers.p_uint32, packers.p_dict)
       
    d.store(entry.bulk_prepare({134: {'name': 'Andy'},
                                156: {'name': 'Bob'},
                                148: {'name': 'Charlie'}}), 1)
        
    assert entry.get(d, 134) == {'name': 'Andy'}
        
    assert entry.keys(d) == [134, 148, 156]
     
    d = RevisionDataStore(db.get_db('people_names'), 0)
    index = Index(packers.p_uint32, packers.p_string)
     
    d.store((index.prepare('Andy', 134, '+'),
             index.prepare('Bob', 156, '+'),
             index.prepare('Charlie', 148, '+')), 1)
    
    assert index.lookup(d, 'Bob') == [156]
        
    d.store((index.prepare('Andy', 134, '-'),
             index.prepare('Andrew', 134, '+'),
             index.prepare('Dave', 101, '+'),
             index.prepare('Bob', 156, '-')), 2)
    
    assert index.lookup(d, 'A', 'Z') == [101, 134, 148]
    assert index.lookup(d, 'B', 'D') == [148]
    assert index.lookup(d, 'B', 'D', 1) == [148, 156]
    
    for f in os.listdir('testing'):
        os.unlink(os.path.join('testing', f))
    
    print "All assertions passed"