'''
Created on 04/02/2011

@author: imartin
'''

import dal

import dal_sqlalchemy as dals
import dal_sqlalchemy.datatype as dt
from dal_sqlalchemy.object import Object


class Value(object):
    def __init__(self, field, value):
        self.field = field
        self.value = value
        self.changed = False
        self.list = False

class Field(object):
    
    def __init__(self, belongs_to, type_field, name):
            self.name = name
            self.belongs_to = belongs_to
            tname = dals.composeTableName(belongs_to, name)
            if not tname in dals.table_space:
                raise dal.FieldNotDefined(belongs_to, name)
            
            self.table = dals.table_space[tname]
            self.belongs_to_id = dt.get_type_id(belongs_to)
            self.type_id = dt.get_type_id(type_field)
            fdata = dal.mc.get_field_data(self.belongs_to_id, name)
            self.size = fdata['size']
            self.is_list = fdata['is_list']
            self.vname = '_v_' + name
            if fdata['is_list'] == 1:
                raise dal.IsList(self.name)

        
    def _get_field(self, oid):
        r = self.table.select(self.table.c.obj_id==oid).execute().fetchall()
        return r
    
    def __get__(self, obj, cls=None):
        if obj == None :
            return self
        else:
            if self.vname in obj.__dict__:
                return obj.__dict__[self.vname].value
        
            r = dals.transaction(self._get_field, obj.id)
            if self.type_id < 100:
                result = r[0][1]
            else:
                result = Object(self.type_id, r[0][1])       
            v = Value(self, result)
            self._init_attr(obj, v)
            return v.value
    
    def __set__(self, obj, value):        
        
        if self.vname in obj.__dict__:
            v = getattr(obj, self.vname)
            v.value = value
        else:
            v = Value(self, value)
            self._init_attr(obj, v)

        v.changed = True
        
    def _init_attr(self, obj, v):
        setattr(obj, self.vname, v)
        obj.add_field(v)
        
    def __eq__(self, o):
        return self.table.c.value == o
    
    def __gt__(self, o):
            return self.table.c.value > o
