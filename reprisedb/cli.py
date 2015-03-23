from reprisedb import database, drivers

import csv
import os.path
import re
import logging

logger = logging.getLogger(__name__)

r_num = re.compile('^([0-9]+)(\.[0-9]+)?$')

def parse_value(v):
    m = r_num.match(v)
    if m is None: return v
    
    if m.group(2) is None:
        return int(v)
    else:
        return float(v)

def import_csv(db, collection, filename):
    fullpath = os.path.realpath(filename)
    
    t = db.begin()
    
    if not collection in t.list_collections():
        t.create_collection(collection)
    
    headers = None
    
    pk = 1
    
    with open(fullpath, 'rb') as f:
        csvreader = csv.reader(f)
        for line in csvreader:
            
            if headers is None:
                headers = line
                continue
            
            data = dict(zip(headers, [ parse_value(v) for v in line ]))
            t.put(collection, pk, data)
            pk += 1
            
            #if pk > 500: break
            
    t.commit()
            

def import_data(options):
    db = database.RepriseDB(path=options.datadir)
    
    return import_csv(db, options.collection, options.filename)
    

def shell(options):
    db = database.RepriseDB(path=options.datadir)
    code.interact("RepriseDB Interactive Shell", local={'db': db, 't': db.begin()})
    

if __name__ == '__main__':
    import argparse
    import code
        
    parser = argparse.ArgumentParser(description="CLI for RepriseDB")
    parser.add_argument('--logging', '-l', default='info', help="Logging level")
    subparsers = parser.add_subparsers()
    
    p_cli = subparsers.add_parser('shell')
    p_cli.add_argument('datadir', help="Directory containing database files")
    p_cli.set_defaults(func=shell)
    
    p_import = subparsers.add_parser('import')
    p_import.add_argument('datadir', help="Directory containing database files")
    p_import.add_argument('collection', help='Collection to import into')
    p_import.add_argument('filename', help='File to import')
    p_import.add_argument('--format', default='csv', help='Import format')
    p_import.set_defaults(func=import_data)
    
    args = parser.parse_args()
    
    logging.basicConfig(level=getattr(logging, args.logging.upper(), logging.INFO))
    
    logger.debug("CLI ARGS: %r", args._get_kwargs())
    
    args.func(args)
    
    
    
    