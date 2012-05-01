'''
Created on 23/02/2011

@author: Carlos
'''
import dal

import dal_sqlalchemy as dals
import dal_sqlalchemy.datatype as dt
from dal_sqlalchemy.object import Object
from dal_sqlalchemy.query import Query

class ValueList(object):
    def __init__(self, field, obj):
        self.obj=obj
        self.field = field
        self.valuelist = []
        self.totaledit = False
        self.list = True
        
    def append(self, value):
        if not type(value) is list:
            self.valuelist.append(value)
        else:
            raise dal.IsList("value")
        
    def clear(self):
        self.obj.delete_field_content(self.field)
        
    def all(self):
        q = Query()
        q.set_table(dals.table_space[dals.composeTableName(self.field.belongs_to,self.field.name)])
        q.set_id(self.obj.id);
        q.set_type(dt.getTypeName(self.field.type_id))
        return q
                

class FieldList(object):
    '''
    classdocs
    '''
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
        if self.is_list == 0:
                raise dal.IsList(self.name)
        
    def __get__(self, obj, cls=None):
        return self.__get_value_list(obj)
    
    def __set__(self, obj, value):                
        if type(value) is list:
            v=self.__get_value_list(obj)
            v.value=value;
            v.totaledit = True
        else:
            raise dal.IsNotList("value")
        
        
    def __get_value_list(self, obj):
        if self.vname in obj.__dict__:
                v = getattr(obj, self.vname)            
        else:
                v = ValueList(self, obj)
                self._init_attr(obj, v)
        return v
    

    def _init_attr(self, obj, v):
        setattr(obj, self.vname, v)
        obj.add_field(v)

                
        