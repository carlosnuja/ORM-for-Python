'''
Created on 25/01/2011

@author: Carlos
'''

import sqlalchemy 
from sqlalchemy import Table, Column, Integer, ForeignKey, and_, select, Index
from sqlalchemy.sql import bindparam

import dal

import dal_sqlalchemy as dals

def getColumnTypeId(type_id, size):
    ''' Retorna el tipus de dada Elemental corresponent, segons el seu id i la seva mida '''
    if type_id == 0:
        return sqlalchemy.SmallInteger
    elif type_id == 1:
        return sqlalchemy.Integer
    elif type_id == 2:
        return sqlalchemy.BigInteger
    elif type_id == 3:
        if size > 0:
            return sqlalchemy.String(size)
        else:
            return sqlalchemy.String
    elif type_id == 4:
        return sqlalchemy.Text
    elif type_id == 5:
        return sqlalchemy.Float
    elif type_id == 6:
        return sqlalchemy.LargeBinary
    elif type_id == 7:
        return sqlalchemy.Date
    elif type_id == 8:
        return sqlalchemy.Time
    elif type_id == 9:
        return sqlalchemy.DateTime
    else:
        return None

def get_type_id(type_obj):
    ''' Retorna el tipus de dada passant un unicode cridant a la funcio type_obj '''
    if not type_obj:
        return -1
    if type(type_obj) is str:
        type_obj=unicode(type_obj)
    if type(type_obj) is unicode:
        try:
            return dal.mc.get_type_data(type_obj)
        except Exception as e:
            print e
            return None
    else:
        return type_obj


def getTypeName(type_obj):
    ''' Se li pasa un parametre y retorna el nom del tipus del objecte '''
    if type(type_obj) is int:
        try:
            return dal.mc.get('ot:%i' % type_obj)
        except Exception as e:
            print e
            return None
    else:
        return type_obj
    
def getTypeFields(type_obj):
    ''' mitjancant un int fem una crida al metode get_type_fields que retorna el resultat '''
    if type(type_obj) is str:
        unicode(type_obj)
    if type(type_obj) is unicode:
        type_obj=get_type_id(type_obj)
    if type(type_obj) is int:
        try:
            return dal.mc.get_type_fields(type_obj)
        except Exception as e:
            print e
        
        
def getWriteLock(table, id):
    q = table.update().where(table.c.id==id).values(lockw=table.c.lockw+1)
    conn = dals.engine.connect()
    trans = conn.begin()
    r = q.execute()
    p = r.last_updated_params()
    lock_aq = True
    if p['lockw_1'] > 1:
        q = table.update().where(table.c.id==id).values(lockw=table.c.lockw-1)
        q.execute()
        lock_aq = False
    trans.commit()
    return lock_aq
    
def releaseWriteLock(table, id):
    q = table.update().where(table.c.id==id).values(lockw=table.c.lockw-1)
    conn = dals.engine.connect()
    trans = conn.begin()
    r = q.execute()
    p = r.last_updated_params()
    failed = False
    if p['lockw_1'] != 0:
        q = table.update().where(table.c.id==id).values(lockw=0)
        q.execute()
        failed = True
    trans.commit()
    return failed

def create_type(name, base=None):
    name = unicode(name)
    tname = dals.composeTableName(name)
    if tname in dals.table_space:
        raise dal.TypeAlreadyExists(name)
    
    base_id = get_type_id(base) if base else -1
        
    id = 0
    try:
        result = dals.obj_type.insert().execute(name=name, base=base_id)
        id = result.lastrowid
    except Exception as e:
        print e
    
    if id != 0:
        table = Table(tname, dals.metadata,
                      Column('id', Integer, primary_key=True),
                      Column('lockw', Integer, default=0),
                      Column('lockr', Integer, default=0))

        table.create(dals.metadata.bind, checkfirst=True)
        dals.table_space[tname] = table
        dal.mc.set_type_data(name, id)
        dal.mc.set_type_data(id, name)
        
    cls = gen_class(name)
    setattr(cls, 'table', table)
        

def create_field(name, type_field, size, is_list, belongs_to, indexed):
    name = unicode(name)
    belongs_to = unicode(belongs_to)
    belongs_to_id = get_type_id(belongs_to)
    fd = dal.mc.get_field_data(belongs_to_id, name)
    if fd:
        return
    
    type_id = get_type_id(type_field)
        
    try:
        r = dals.field_def.insert().execute(name=name,
                                           type_id=type_id,
                                           belongs_to_id=belongs_to_id,
                                           size=size,
                                           is_list=is_list, 
                                           indexed=indexed)
        id = r.lastrowid
    except Exception as e:
        print e
        return 0
    
    if id != 0:
        if type_id < 100:
            table = Table(dals.composeTableName(belongs_to, name), dals.metadata,
                          Column('id', Integer, primary_key=True),
                          Column('value', getColumnTypeId(type_id, size)),
                          Column('obj_id', Integer, ForeignKey(dals.obj_type.c.id), unique=not is_list))
        else:
            table = Table(dals.composeTableName(belongs_to, name), dals.metadata,
                          Column('id', Integer, primary_key=True),
                          Column('value', Integer, ForeignKey(dals.obj_type.c.id)),
                          Column('obj_id', Integer, ForeignKey(dals.obj_type.c.id), unique=not is_list))
            
        table.create(dals.metadata.bind)
        dals.table_space[dals.composeTableName(belongs_to, name)] = table
        dal.mc.set_field_data({'belongs_to_id':belongs_to_id,
                               'name':name, 
                               'type_id':type_id,
                               'size':size,
                               'is_list': is_list,
                               'indexed':indexed})
    if is_list:
        gen_field_list(belongs_to, type_field, name)   
    else:   
        gen_field(belongs_to, type_field, name)    
    return id

def gen_class(name):
    class C(dal.object.Object):
        def __init__(self, id=-1):
            super(C, self).__init__(name, id)
    setattr(dal.object, name, C)
    return C
    
def gen_field(cname, type_field, name):
    cls = getattr(dal.object, cname)
    setattr(cls, name, dal.field.Field(cname, type_field, name))

def gen_field_list(cname, type_field, name):
    cls = getattr(dal.object, cname)
    setattr(cls, name, dal.fieldList.FieldList(cname, type_field, name))

def get_types():
    s = select([dals.obj_type])
    rows = dals.engine.connect().execute(s)
    return [(r[0], r[1]) for r in rows]

def get_type_data(type_obj):
    try:
        if type(type_obj) is unicode:
            r = select([dals.obj_type]).where(dals.obj_type.c.name==type_obj).execute().fetchone()
        else:
            r = select([dals.obj_type]).where(dals.obj_type.c.id==type_obj).execute().fetchone()
    except:
        pass
    
    return (r[0], r[1])

def get_fields(type_obj):
    if type(type_obj) is int:
        type_id = type_obj
    else:     
        type_id = get_type_id(type_obj)    
    fields={}
    try:
        r = dals.field_def.select(dals.field_def.c.belongs_to_id==type_id).execute().fetchall()
        for row in r:
            fields[row['name']]=row['is_list'];
        return fields
    except Exception as e: 
        print e

def get_field_data(type_obj, field_name):
    type_id= get_type_id(type_obj)
    try:
        r = dals.field_def.select(and_(dals.field_def.c.name==field_name,
                                       dals.field_def.c.belongs_to_id==type_id)).execute().fetchone()
    except:
        raise dal.FieldNotDefined(type_obj, field_name)
    
    if r:
        return {'belongs_to_id': r['belongs_to_id'],
                'name': r['name'], 
                'type_id': r['type_id'],
                'size': r['size'],
                'is_list': r['is_list'],
                'indexed': r['indexed']}
    else:
        return None
    
def create_simple_acc(belongs_to, field_name):
    ''' Es crea un index al valor del camp que li passem per parametre,  field_name es el atribut del objecte del objecte
    que li indiquem amb belongs_to. Els dos parametres han de contenir un string'''
    field_name = unicode(field_name)
    belongs_to = unicode(belongs_to)
    belongs_to_id = get_type_id(belongs_to)
    def create_field_index(belongs_to, field_name):
        i = Index(gen_index_name(belongs_to, field_name), dals.table_space[dals.composeTableName(belongs_to, field_name)].c.value)
        i.create(dals.metadata.bind)
        r = dals.field_def.update().where(and_(dals.field_def.c.name==field_name, dals.field_def.c.belongs_to_id==belongs_to_id)).values(indexed=True).execute()
    dals.transaction(create_field_index, belongs_to, field_name)
    dal.mc.update_field_data(belongs_to_id, field_name)
    
    
def create_compose_acc(field_name, belongs_to, type_obj): 
    field_name = unicode(field_name)
    belongs_to = unicode(belongs_to)
    belongs_to_id = get_type_id(belongs_to)
    type_id = get_type_id(type_obj)
    '''Es tracta de afegir a les taules d'instancies d'atributs dels objectes una columna extra amb la referencia directe al objecte
    append_column(column_name)'''
    '''  '''
    
def gen_index_name(belongs_to, field_name):
    return 'idx_%s_%s' % (str(belongs_to), str(field_name))