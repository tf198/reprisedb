'''
Packs and unpacks datatypes into naturally sortable bytestrings.

All classes should implement `pack(value, index=False)` and `unpack(value, index=False)`
methods.  

The `index` parameter indicates whether the value will be used in a db key and
may containing trailing bytes - typically string packers will need to append a NUL byte
to maintain sortability while fixed length packers wont. e.g.

>>> 'foobar' < 'fooabar'
False
>>> 'foo\\x00bar' < 'fooa\\x00bar'
True

>>> repr(p_uint32)
'<UnsignedIntegerPacker(4)>'

>>> packed = p_string.append_last('foo\\x00', 'bar')
>>> packed
'foo\\x00bar\\x00\\x04'
>>> p_string.extract_last(packed)
('foo\\x00', 'bar')
 
>>> packed = p_uint32.append_last('foo', 32)
>>> packed
'foo\\x00\\x00\\x00\x20'
>>> p_uint32.extract_last(packed)
('foo', 32)
    
>>> packed = p_revision.append_last('foo', 32)
>>> packed
'foo\\xff\\xff\\xff\\xdf'
>>> p_revision.extract_last(packed)
('foo', 32)

>>> p_string.append_last('foo\\x00', 'bar') < p_string.append_last('fooa\\x00', 'bar')
True
'''

import struct
import msgpack

from . import NUL

class NumberPacker(object):
    
    typecode = 0
    
    def pack(self, value, index=False):
        return self.packer.pack(value)
    
    def unpack(self, value, index=False):
        return self.packer.unpack(value)[0]
    
    def extract_last(self, s):
        ' returns the unprocessed portion of the string, plus the extracted value'
        return s[:-self.size], self.unpack(s[-self.size:])
    
    def append_last(self, s, value):
        ' returns the string plus the packed value '
        return s + self.pack(value)
    
    def __repr_(self):
        return "<{0}>".format(self.__class__.__name__)
        
class UnsignedIntegerPacker(NumberPacker):
    
    def __init__(self, fmt):
        self.packer = struct.Struct(fmt)
        self.size = self.packer.size
        self.max = pow(256, self.size) - 1
        self.min = 0
        self.typecode = self.size # 1, 2, 4
        
    def __repr__(self):
        return "<{0}({1})>".format(self.__class__.__name__, self.size)
    
class SignedIntegerPacker(UnsignedIntegerPacker):
    '''
    Offset value into positive space
    '''
    
    def __init__(self, fmt):
        super(SignedIntegerPacker, self).__init__(fmt)
        self.max /= 2
        self.offset = self.max
        self.min = -self.max
        self.typecode += 4 # 5, 6, 8
        
    def pack(self, value, index=False):
        return self.packer.pack(value+self.offset)
    
    def unpack(self, value, index=False):
        return self.packer.unpack(value)[0] - self.offset

class FloatPacker(NumberPacker):
    '''
    Twiddle the sign bit, then floats become naturally sortable
    '''
    
    typecode = 9
    
    def __init__(self, fmt):
        self.packer = struct.Struct(fmt)
        self.size = self.packer.size
        
    def pack(self, value, index=False):
        b = self.packer.pack(value)
        return chr(ord(b[0]) ^ 0x80) + b[1:]
    
    def unpack(self, value, index=False):
        b = chr(ord(value[0]) ^ 0x80) + value[1:]
        return self.packer.unpack(b)[0] 

class StringPacker(object):
    
    typecode = 10
    
    def pack(self, value, index=False):
        if index: value += NUL
        return value
    
    def unpack(self, value, index=False):
        return value[:-1] if index else value
    
    def extract_last(self, s):
        start = p_uint8.unpack(s[-1]) + 1
        return s[:-start], self.unpack(s[-start:-1], index=True)
    
    def append_last(self, s, value):
        packed = self.pack(value, index=True)
        return s + packed  + p_uint8.pack(len(packed))
    
    def __repr__(self):
        return "<{0}>".format(self.__class__.__name__)
    
class MsgpackPacker(StringPacker):
    
    typecode = 11
    
    def pack(self, value, index=False):
        return msgpack.dumps(value)
    
    def unpack(self, value, index=False): 
        return msgpack.loads(value)
    
class RevisionPacker(UnsignedIntegerPacker):

    def pack(self, value, index=False):
        return self.packer.pack(self.max - value)
    
    def unpack(self, value, index=False):
        return self.max - self.packer.unpack(value)[0]

'''
Instances of the packers for use.
All prefixed with `p_` to prevent collisions with primitives
'''

p_uint32 = UnsignedIntegerPacker(">I")
p_uint16 = UnsignedIntegerPacker(">H")
p_uint8 = UnsignedIntegerPacker(">B")
p_int32 = SignedIntegerPacker(">I")
p_int16 = SignedIntegerPacker(">H")
p_float = FloatPacker(">f")
p_string = StringPacker()
p_dict = MsgpackPacker()
p_obj = MsgpackPacker()
p_revision = RevisionPacker(">I")

registry = {'uint32': p_uint32,
            'uint16': p_uint16,
            'uint8': p_uint8,
            'int32': p_int32,
            'int16': p_int16,
            'float': p_float,
            'string': p_string,
            'dict': p_dict,
            'obj': p_obj,
            'revision': p_revision}


if __name__ == '__main__':
    import doctest
    print doctest.testmod()