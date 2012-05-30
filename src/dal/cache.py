'''
Created on 21/12/2010

@author: imartin
'''
from sqlalchemy import select, Table
import memcache

import dal
import dal.datatype as dt

class Cache:
    def __init__(self, address):
        self.mc = memcache.Client([address], debug=0)
    
    def clear(self):
        self.mc.flush_all()
    def add(self, key, data):
        self.mc.add(key, data)
    def set(self, key, data):
        self.mc.set(key, data)
    def get(self, key):
        return self.mc.get(key)
    def delete(self, key, time):
        self.mc.delete(key, time)
    
    def get_type_data(self, type_obj):
        data = self.mc.get(gen_key_type(type_obj))
        if data:
            return data
        
        td = dt.get_type_data(type_obj)  
        
        self.mc.set(gen_key_type(td[0]), td[1])
        self.mc.set(gen_key_type(td[1]), td[0])
            
        return self.mc.get(gen_key_type(type_obj))
    
    def set_type_data(self, type_obj, value):
        self.mc.set(gen_key_type(type_obj), value)
    
    def set_field_data(self, data):
        self.set(gen_key_field(data['belongs_to_id'], data['name']), data)
        belongs_to = self.get_type_data(data['belongs_to_id'])
        self.set(gen_key_field(belongs_to, data['name']), data)
        return data
    
    def get_field_data(self, type_obj, name):
        key = gen_key_field(type_obj, name)
        fd = self.mc.get(key)
        if not fd:
            fd = dt.get_field_data(type_obj, name)
            self.mc.set(key, fd)
        return fd
    
    def update_field_data(self, type_obj, name):
        key = gen_key_field(type_obj, name)
        self.delete(key, 0)
        fd = dt.get_field_data(type_obj, name)
        self.mc.set(key, fd)
        return fd
    
    def set_type_fields(self, type_obj ,fields):
        self.set(gen_key_type_fields(type_obj), fields)
    
    def get_type_fields(self, type_obj):
        key = gen_key_type_fields(type_obj)
        fd = self.mc.get(key)
        if not fd:
            fd = dt.get_fields(type_obj)
            self.mc.set(key, fd)
        return fd

def gen_key_type(type_obj):
    return 'ot:%s' % str(type_obj)

def gen_key_field(type_obj, field_name):
    return 'fd:%s:%s' % (str(type_obj), str(field_name))

def gen_key_type_fields(type_obj):
    return 'otf:%s' % str(type_obj)