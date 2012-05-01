import dal.cache as cache

base_types = ['sint', 'int', 'lint', 'string', 'text', 'float', 'blob', 'date', 'time', 'datetime']

# dummy types to avoid IDE syntax errors
mc = ''

class TypeAlreadyExists(Exception):
    def __init__(self, name):
        self.name = name
    
    def str(self):
        return self.name
    
class FieldNotDefined(Exception):
    def __init__(self, cname, name):
        self.cname = cname
        self.name = name
        super(FieldNotDefined, self).__init__()
        
class NotFieldsDefined(Exception):
    def __init__(self):
        pass
        
    def __str__(self):
        return "Error: No hay fields definidas"
    
        
class TypeNotEqual(Exception):
    def __init__(self):
        pass
        
    def __str__(self):
        return "Error: el tipo es diferente"
    
class TooParameters(Exception):
    def __init__(self):
        pass
        
    def __str__(self):
        return "Error: Mas parametros de los esperados"

class IsList(Exception):
    def __init__(self, name):
        self.name=name
        
    def __str__(self):
        return "Error: "+self.name+" es una llista"
    
class IsNotList(Exception):
    def __init__(self, name):
        self.name=name
        
    def __str__(self):
        return "Error: "+self.name+" no es una llista"
        
        
        
def init_cache(): 
    global mc
    mc = cache.Cache('127.0.0.1:11211')
    mc.clear()