import pymongo

from threading import Lock
import logging

import pkgutil
import inspect

import cache
import datatype as dt

metadata = MetaData()
base_types = ['sint', 'int', 'lint', 'string', 'text', 'float', 'blob', 'date', 'time', 'datetime']
table_space = {}

# Dummy initialization to prevent errors in some IDEs
obj_type = ''
field_def = ''
engine = ''
mc = ''

def synchronized(lock):
    """ Synchronization decorator. """
    def wrap(f):
        def new_function(*args, **kw):
            lock.acquire()
            try:
                return f(*args, **kw)
            finally:
                lock.release()
        return new_function
    return wrap

def composeTableName(*args):
    tn = u'_'.join(args)
    return u'mcms_' + tn
        
def init(config):
    global engine
 
    engine = engine_from_config(config, 'sqlalchemy.')

    
    metadata.bind = engine

    global obj_type
    obj_type_tname = composeTableName('obj_type')
    obj_type = Table(obj_type_tname, metadata,
                     Column('id', Integer, primary_key=True),
                     Column('name', String(30), unique=True),
                     Column('base_type_id', None, ForeignKey(obj_type_tname + '.id')))

    
    global field_def
    global field_def_tname
    field_def_tname = composeTableName('field_def')
    field_def = Table(field_def_tname, metadata,
                      Column('id', Integer, primary_key=True),
                      Column('belongs_to_id', None, ForeignKey(obj_type_tname + '.id')),
                      Column('name', String(30)),
                      Column('type_id', None, ForeignKey(obj_type_tname + '.id')),
                      Column('size', Integer),
                      Column('is_list', Boolean))
    
    metadata.create_all(engine)
    
    import dal
    package = dal
    path = package.__path__
    for (module_loader, name, ispkg) in pkgutil.iter_modules(path):
        module = __import__('dal.' + name, fromlist='dummy')
        module_sqlalchemy = __import__('dal_sqlalchemy.' + name, fromlist='dummy')
        for f in dir(module):
            obj = getattr(module, f)
            if inspect.isfunction(obj) or inspect.isclass(obj):
                obj_sqlalchemy = getattr(module_sqlalchemy, f)
                if not obj_sqlalchemy:
                    raise Exception("DAL incomplete")
                else:
                    setattr(module, f, obj_sqlalchemy)

    # init database tables
    init_basic_types()
    
    global mc
    mc = cache.Cache('127.0.0.1:11211')
    mc.clear()

    init_table_space()
    
    # init cache

def init_basic_types():
    for tn in base_types:
        try:
            obj_type.insert().execute(name=tn)
        except:
            pass
        
    try:
        obj_type.insert().execute(id=99, name="dummy", base_type_id=0)
    except:
        pass

def init_table_space():
    s = select([obj_type])
    rows = engine.connect().execute(s)
    types = []
    for t in rows:
        if t['id'] > 99:
            tname = composeTableName(t['name'])
            table = Table(tname, metadata, autoload=True)
            table_space[tname] = table
            mc.set_type_def(t['id'], t['name'])
            mc.set_type_def(t['name'], t['id'])
            dt.gen_class(t['name'])
            types.append((t['name'], t['id']))
    
    for t in types:
        type_id = t[1]
        type_name = t[0]
        rows2 = engine.connect().execute(select([field_def]).where(field_def.c.belongs_to_id==type_id))
        for f in rows2:
            tname = composeTableName(type_name, f['name']) 
            table = Table(tname, metadata, autoload=True)
            table_space[tname] = table
            mc.set_field_def({'id': f['id'],
                              'belongs_to_id': f['belongs_to_id'],
                              'name': f['name'],
                              'type_id': f['type_id'],
                              'size': f['size'],
                              'is_list': f['is_list']})
            dt.gen_field(type_name, mc.get_type_def(f['type_id']), f['name'])
            
def transaction(f, *args):
    conn = engine.connect()
    trans = conn.begin()
    r = f(*args)
    trans.commit()
