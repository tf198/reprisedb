from . import RepriseDBTestCase

from reprisedb import drivers, datastore, entries, packers

import itertools

class EntriesTestCase(RepriseDBTestCase):
    
    def setUp(self):
        driver = drivers.LMDBDriver(self.TESTDIR)
        
        self.rds = datastore.RevisionDataStore(driver.get_db('testing'), 0)
    
    def test_string_string_entry(self):
        
        entry = entries.Entry(packers.p_string, packers.p_string)
        
        people = entries.BoundEntry(entry, self.rds)
        
        assert repr(people.entry) == "<Entry key=<StringPacker> value=<StringPacker>>"
        
        people.bulk_put({'134': 'Andy',
                         '156': 'Bob',
                         '148': 'Charlie'}, 1)
        
        assert people.get('148') == "Charlie"
        
        assert people.keys() == ['134', '148', '156']
         
        people.bulk_put({"134": 'Andrew',
                         "101": 'Dave',
                         "156": None}, 2)
         
        people_1 = entries.BoundEntry(entry, self.rds, 1)
         
        assert people.keys() == ["101", "134", "148"]
        assert people_1.keys() == ["134", "148", "156"]
            
        assert people.get("148") == "Charlie"
        assert people.get("134") == 'Andrew'
        assert people_1.get("134") == 'Andy'
        #assert people.get("156") == None
        assert people_1.get("156") == 'Bob'
        
    def test_int_dict_entry(self):
        entry = entries.Entry(packers.p_uint32, packers.p_dict)
        people = entries.BoundEntry(entry, self.rds)
         
        people.bulk_put({134: {'name': 'Andy'},
                         156: {'name': 'Bob'},
                         148: {'name': 'Charlie'}}, 1)
              
        assert people.get(134) == {'name': 'Andy'}
              
        assert people.keys() == [134, 148, 156]
        
    def test_int_string_index(self):
        index = entries.SimpleIndex(packers.p_uint32, packers.p_string)
        people_names = entries.BoundIndex(index, self.rds)
           
        people_names.bulk_add([('Andy', 134, '+'),
                               ('Bob', 156, '+'),
                               ('Charlie', 148, '+')], 1)
          
        assert people_names.lookup('Bob') == [156]
              
        people_names.bulk_add([('Andy', 134, '-'),
                               ('Andrew', 134, '+'),
                               ('Dave', 101, '+'),
                               ('Bob', 156, '-')], 2)
         
        people_names_1 = entries.BoundIndex(index, self.rds, 1)
          
        assert people_names.lookup('A', 'Z') == [134, 148, 101] # name order
        assert people_names.lookup('B', 'D') == [148]
        assert people_names_1.lookup('B', 'D') == [156, 148]
        
    def test_iter_items(self):
        
        entry = entries.Entry(packers.p_uint32, packers.p_string)
        people = entries.BoundEntry(entry, self.rds)
        
        def f(**kwargs):
            return [ x[1] for x in people.iter_items(**kwargs) ]
        
        people.bulk_put({3: 'Bob',
                         6: 'Fred',
                         23: 'Andy'}, 1)
        
        self.assertEqual(f(), ['Bob', 'Fred', 'Andy'])
         
        people.bulk_put({9: 'Charlie'}, 2)
        
        self.assertEqual(f(), ['Bob', 'Fred', 'Charlie', 'Andy'])
        
        people.bulk_put({43: 'Terry',
                              1: 'Ethan',
                              6: 'Frank',
                              3: None}, 3) 
         
        self.assertEqual(f(),
                         ['Ethan', 'Frank', 'Charlie', 'Andy', 'Terry'])
         
        self.assertEqual(f(start_key=6),
                         ['Frank', 'Charlie', 'Andy', 'Terry'])
        self.assertEqual(f(end_key=23),
                         ['Ethan', 'Frank', 'Charlie'])
        self.assertEqual(f(start_key=6, end_key=23),
                         ['Frank', 'Charlie'])
        
        people_2 = entries.BoundEntry(entry, self.rds, 2)
        self.assertEqual([ x[1] for x in people_2.iter_items(start_key=6, end_key=23) ],
                         ['Fred', 'Charlie'])
         
        # check we can slice it
        items = itertools.islice(people.iter_items(start_key=6), 1, 3)
        self.assertEqual( [ x[1] for x in items ], ['Charlie', 'Andy'])
        
    def test_iter_get(self):
        
        entry = entries.Entry(packers.p_uint32, packers.p_string)
        people = entries.BoundEntry(entry, self.rds)
        
        people.bulk_put({3:  'Bob',
                         6:  'Brenda',
                         23: 'Borris',
                         9:  'Andy',
                         12: 'Zaview'}, 1)
        
        def f(start=1, end=10, **kwargs):
            return list(people.iter_get(xrange(start, end), **kwargs))
        
        self.assertEqual(f(), [None, None, 'Bob', None, None, 'Brenda', None, None, 'Andy'])