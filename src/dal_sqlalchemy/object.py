'''
Created on 25/01/2011

@author: Carlos
'''

import dal_sqlalchemy as dals
import dal_sqlalchemy.datatype as dt
from dal_sqlalchemy.query import Query



class Object(object):
    
    table = None
     
    def __init__(self, type_name, id=-1):    
        self.fields=[]    
        if id >= 0:
            self.type_name = type_name
            self.id = id
            return
            
        self.type_id = dt.get_type_id(type_name) 
        self.attributes = dt.getTypeFields(self.type_id)
        for i in self.attributes:
            if not self.attributes[i]:
                setattr(self, i, None)
            else: 
                setattr(self, i, [])
        self.table = dals.table_space[dals.composeTableName(type_name)]
        self.__class__.table = dals.table_space[dals.composeTableName(type_name)]
        try:
            o = self.table.insert().execute()
        except Exception as e:
            print e
    
        self.id = o.lastrowid
        
    def equal_type(self, type_object):
        return self.type_name == type_object

    def add_field(self, value):
        self.fields.append(value)

    def save(self):
        for v in self.fields:
            if v.list:
                if v.totaledit:
                    """ borrar todo el contenido """
                id_list = [o.id for o in v.valuelist]
                for o in v.valuelist:
                    self._save_field(v.field, o)
            else:
                if v.changed:
                    self._save_field(v.field, v.value)
                v.changed = False

    
    def _save_field(self, field, value):
        val = value.id if isinstance(value, Object) else value

        if field.is_list:
            def set_field_not_unique(tf, oid, v):       
                tf.insert().values(value=v, obj_id=oid).execute()
            dals.transaction(set_field_not_unique, field.table, self.id, val)
        else:
            def set_field_unique(tf, oid, v):       
                r = tf.update().where(tf.c.obj_id==oid).values(value=v).execute()
                if r.rowcount == 0:
                    tf.insert().values(value=v, obj_id=oid).execute()
            dals.transaction(set_field_unique, field.table, self.id, val)
            
    def delete_field_content(self, field):
        def delete_fields(tf, oid):       
            tf.delete().where(tf.c.obj_id==oid).execute()
        dals.transaction(delete_fields, field.table, self.id) 
        
 
    @classmethod       
    def all(cls):
        q = Query()
        q.set_type(cls.title.belongs_to)
        q.set_table(cls.table)
        return q

