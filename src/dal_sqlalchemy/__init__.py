from threading import Lock
import logging

from sqlalchemy import engine_from_config, Table, Column, Integer, Boolean, String, MetaData, ForeignKey, select
from sqlalchemy.engine import reflection
from sqlalchemy import create_engine
from sqlalchemy.schema import (
    DropTable,
    ForeignKeyConstraint,
    DropConstraint,
    )
import pkgutil
import inspect

import dal

import dal.cache as cache
import datatype as dt

metadata = MetaData()
table_space = {}

# Dummy initialization to prevent errors in some IDEs
obj_type = ''
field_def = ''
index_acc = ''
engine = ''

class DALError(Exception):
    def __init__(self, message):
        self.message = message
    
    def _str_(self):
        return self.message

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

def clearDatabase(engine):
    conn = engine.connect()

    # the transaction only applies if the DB supports
    # transactional DDL, i.e. Postgresql, MS SQL Server
    trans = conn.begin()

    inspector = reflection.Inspector.from_engine(engine)

    # gather all data first before dropping anything.
    # some DBs lock after things have been dropped in 
    # a transaction.

    metadata = MetaData()

    tbs = []
    all_fks = []

    for table_name in inspector.get_table_names():
        fks = []
        for fk in inspector.get_foreign_keys(table_name):
            if not fk['name']:
                continue
            fks.append(
                       ForeignKeyConstraint((),(),name=fk['name'])
                       )
        t = Table(table_name,metadata,*fks)
        tbs.append(t)
        all_fks.extend(fks)

    for fkc in all_fks:
        conn.execute(DropConstraint(fkc))

    for table in tbs:
        conn.execute(DropTable(table))

    trans.commit()
    
        
def init(config):
    global engine
 
    engine = engine_from_config(config, 'sqlalchemy.')

    clearDatabase(engine)
    
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
                      Column('is_list', Boolean), 
                      Column('indexed', Boolean))
    
    global index_acc
    global index_acc_tname
    index_acc_tname = composeTableName('index_acc')
    index_acc = Table(index_acc_tname, metadata,
                      Column('id', Integer, primary_key=True),
                      Column('type_id', None, ForeignKey(obj_type_tname + '.id')),
                      Column('atrib_id', None, ForeignKey(field_def_tname + '.id')),
                      Column('type_id_ext', None, ForeignKey(obj_type_tname + '.id')))
    
    
    metadata.create_all(engine)
    
    # init database tables
    init_basic_types()

    init_table_space()
    
    # init cache

def init_basic_types():
    for tn in dal.base_types:
        try:
            obj_type.insert().execute(name=tn)
        except:
            pass
        
    try:
        obj_type.insert().execute(id=99, name="dummy")
    except Exception as e:
        print e;

def init_table_space():
    s = select([obj_type])
    rows = engine.connect().execute(s)
    types = []
    for t in rows:
        if t['id'] > 99:
            tname = composeTableName(t['name'])
            table = Table(tname, metadata, autoload=True)
            table_space[tname] = table
            dt.gen_class(t['name'])
            types.append((t['name'], t['id']))
        dal.mc.set_type_data(t['id'], t['name'])
        dal.mc.set_type_data(t['name'], t['id'])
    
    for t in types:
        type_id = t[1]
        type_name = t[0]
        rows2 = engine.connect().execute(select([field_def]).where(field_def.c.belongs_to_id==type_id))
        for f in rows2:
            tname = composeTableName(type_name, f['name']) 
            table = Table(tname, metadata, autoload=True)
            table_space[tname] = table
            dal.mc.set_field_data({'id': f['id'],
                              'belongs_to_id': f['belongs_to_id'],
                              'name': f['name'],
                              'type_id': f['type_id'],
                              'size': f['size'],
                              'is_list': f['is_list'],
                              'indexed': f['indexed']})
            if f['is_list']==0 : 
                dt.gen_field(type_name, dal.mc.get_type_data(f['type_id']), f['name'])
            else :
                dt.gen_field_list(type_name, dal.mc.get_type_data(f['type_id']), f['name'])

            
def transaction(f, *args):
    conn = engine.connect()
    trans = conn.begin()
    r = f(*args)
    trans.commit()
    conn.close()
    return r

def executeQuery(s):
    conn = engine.connect()
    result = conn.execute(s)
    conn.close()
    return result
