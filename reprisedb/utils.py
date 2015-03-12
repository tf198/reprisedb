

def dotted_accessor(d, accessor, default=None):
    if d is None:
        return default
    
    try:
        for p in accessor.split('.'):
            d = d[p]
        return d
    except KeyError:
        return default