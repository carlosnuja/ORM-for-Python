'''
Created on 23/03/2011

@author: Carlos
'''

import dal

import dal_sqlalchemy as dals
import dal_sqlalchemy.datatype as dt
from sqlalchemy import engine_from_config, Table, Column, Integer, Boolean, String, MetaData, ForeignKey, select, bindparam, union

class Query(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.id = 0
        self.type = None
        self.table = None
        self.fields = {}
        self.limit = 0
        self.offset = 0
        self.order = None
  
    def set_table(self, table):
        self.table = table
        
    def set_type(self, type):
        self.type = type

    def set_id(self, id):
        self.id = id
                
    def set_fields(self, *args):
        pass    
        
    def offset(self, offset):
        if type(offset) is int:
            self.limit=offset
            return self
        else:
            raise TypeError()        
        
    def limit(self, limit):
        if type(limit) is int:
            self.limit=limit
            return self
        else:
            raise TypeError()
      
    def orderby(self, column):
        list=dt.getTypeFields(self.type)
        if column in list: 
            if list[column]==False:
                self.order=column
                return self
            else:
                raise dal.IsList(column)
        else:
            raise dal.FieldNotDefined(column, self.type)
                    
    
    def filter(self, *fields):
        list=dt.getTypeFields(self.type)
        for i in fields:
            if i in list:
                if not i in self.fields:
                    if list[i]==False :
                        self.fields[i]=False                   
                    else:
                        raise dal.IsList(i)
            else:
                raise dal.FieldNotDefined(i, self.type)
        return self
                
            
    
    def fetch(self):
        ''' El fetch obtindra primerament la llista dels ids que siguin necessaris
         limitara si fos el cas i fara diferents selects per els camps que li demanem.
        Fara una unio de les diferents files empilant tots els objectes amb els camps
        que pertoquin i retornara la llista d'objectes'''
        objs = []
        extrawork = False
        version = 1
        fieldName = str(dt.getTypeName(self.type))
        cls = getattr(dal.object, fieldName)
        if(len(self.fields) == 0):
            self.fields=dt.getTypeFields(self.type)
        if self.id != 0 :
            ''' La crida ve desde una instancia: recuperem nomes els objectes que pertanyen a aquesta instancia''' 
            if self.order == None :
                '''Si no es vol cap ordre especial sobtenen els registres utilitzan una crida en una de les taules '''
                if self.limit==0 and self.offset!=0:
                    query = select([self.table.c.value]).where(self.table.c.obj_id==self.id).offset(self.offset)
                elif self.limit!=0 and self.offset==0:
                    query = select([self.table.c.value]).where(self.table.c.obj_id==self.id).limit(self.limit)
                elif self.limit==0 and self.offset!=0:
                    query = select([self.table.c.value]).where(self.table.c.obj_id==self.id).offset(self.offset)
                else: 
                    query = select([self.table.c.value]).where(self.table.c.obj_id==self.id)
            else :
                extrawork = True
                query = select([self.table.c.value]).where(self.table.c.obj_id==self.id)
        else :
            '''La crida ve des dun objecte: '''
            if self.order != None :
                table=dals.composeTableName(self.type, self.order)
                if self.limit==0 and self.offset!=0 :
                    oQuery = select([table.c.value, table.c.obj_id]).order_by(table.c.value).limit(self.limit)
                elif self.limit!=0 and self.offset==0 :
                    oQuery = select([table.c.value, table.c.obj_id]).order_by(table.c.value).offset(self.offset).limit(self.limit)
                elif self.limit==0 and self.offset!=0 :
                    oQuery = select([table.c.value, table.c.obj_id]).order_by(table.c.value).offset(self.offset)
                else :
                    oQuery = select([table.c.value, table.c.obj_id]).order_by(table.c.value)
                "aqui creamos una tabla con el orden de los ids"
            else :
                if self.limit==0 and self.offset!=0 :
                    query = select([self.table.c.id]).order_by(self.table.c.id).limit(self.limit)
                elif self.limit!=0 and self.offset==0 :
                    query = select([self.table.c.id]).order_by(self.table.c.id).offset(self.offset).limit(self.limit)
                elif self.limit==0 and self.offset!=0 :
                    query = select([self.table.c.id]).order_by(self.table.c.id).offset(self.offset)
                else :
                    query = select([self.table.c.id]).order_by(self.table.c.id)
                objs = {}
                ids=dals.executeQuery(query).fetchall()
                for i in ids:
                    objs[i['id']]= cls() 
                for nom in self.fields:
                    if not self.fields[nom]:
                        table=dals.composeTableName(self.type,nom)
                        result = dals.executeQuery(select([dals.table_space[table].c.value, dals.table_space[table].c.obj_id]).order_by(dals.table_space[table].c.obj_id)).fetchall()
                        for row in result:
                            if row['obj_id'] in objs:
                                setattr(objs[row['obj_id']], nom, row['value']) 
                            else: 
                                break;
                return objs                                              
        result=dals.executeQuery(query).fetchall()
        if version == 1 :
            for row in result:
                id_obj = row['value']                
                absObj = cls(id_obj)
                for nom in self.fields:
                    if (self.fields[nom]==False):
                        table=dals.composeTableName(self.type,nom)
                        values=dals.executeQuery(select([dals.table_space[table].c.value]).where(dals.table_space[table].c.obj_id==id_obj)).fetchone()                    
                        if values != None:
                            setattr(absObj, nom, values['value'])
                objs.append(absObj)
                if extrawork : 
                    c = Compare(self.order)
                    objs.sort(c)
        if version == 2 :
            for nom in self.fields:
                if nom != self.order & self.fields[nom] == False :  
                    values = dals.executeQuery(select([dals.table_space[table].c.value])).fetchall()
                    for value in values:
                        pass           
        return objs
    
class Compare(object):
    def __init__(self, field):
        self.field=field
    
    def __call__(self, left, right):
        return getattr(left, self.field)<getattr(right, self.field)
        