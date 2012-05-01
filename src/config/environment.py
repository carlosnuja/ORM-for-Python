'''
Created on 16/11/2010

@author: imartin
'''

import inspect

dal_name = ''

def load_environment(config):
    global dal_name
    dal_name = 'dal_' + config['backend']
    dal_backend = __import__(dal_name)
    
    dal = __import__('dal')
    path = dal.__path__
    modules = ['datatype', 'object', 'field', 'fieldList']
    for name in modules:
        module_dal = __import__('dal.' + name, fromlist='dummy')
        module_backend = __import__(dal_name + '.' + name, fromlist='dummy')
        for f in dir(module_dal):
            obj = getattr(module_dal, f)
            if inspect.isfunction(obj) or inspect.isclass(obj):
                obj_backend = getattr(module_backend, f)
                if not obj_backend:
                    raise Exception("DAL incomplete")
                else:
                    setattr(module_dal, f, obj_backend)
    
    dal.init_cache()
    dal_backend.init(config)