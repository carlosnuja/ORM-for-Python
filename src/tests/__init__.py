from config.environment import load_environment
import dal
import dal.datatype as dt
import dal_sqlalchemy as dals
from sqlalchemy import engine_from_config, Table, Column, Integer, Boolean, String, MetaData, ForeignKey, select, bindparam, union

def do (from_scratch):
    ''' load_environment({'backend' : 'sqlalchemy', 'sqlalchemy.url' : 'sqlite:///development.sqlite'}) '''
    load_environment({'backend' : 'sqlalchemy', 'sqlalchemy.url' : 'mysql://root:perrodino18@localhost:3306/projecte', 'sqlalchemy.pool_size' : 10 , 'sqlalchemy.max_overflow' : 10 , 'sqlalchemy.pool_timeout' : 30})
    create_objects(from_scratch)
    init_data(from_scratch)     
    result = dt.get_field_data("Thread", "title" )
    dt.create_compose_acc("title", "Board", "Board")
    print 'Thread size: %s' % result['size']
    o=dal.object.Thread.all().fetch()
    '''o1=dal.object.Thread.all().fetch()'''
    keys=o.keys()
    for key in keys:
        print 'thread: %s' % o[key].title
        

def create_type_if_not_exists(name):
    try:
        dt.create_type(name)
    except dal.TypeAlreadyExists:
        return False
    return True

def create_objects(from_scratch):
    if from_scratch: 
        if create_type_if_not_exists('Post'):
            dt.create_field('title', 'string', 256, False, 'Post', False)
            dt.create_field('text', 'string', 256, False, 'Post', False)
            dt.create_field('subtitle', 'string', 256, False, 'Post', False)
  
        if create_type_if_not_exists('Thread'):
            dt.create_field('title', 'string', 256, False, 'Thread', False)
            dt.create_field('posts', 'Post', 0, True, 'Thread', False)
       
        if create_type_if_not_exists('Board'):
            dt.create_field('name', 'string', 256, False, 'Board', False)
            dt.create_field('threads', 'Thread', 0, True, 'Board', False)
            dt.create_field('sub_boards', 'Board', 0, True, 'Board', False)

        if create_type_if_not_exists('Forum'):
            dt.create_field('name', 'string', 256, False, 'Forum', False)
            dt.create_field('boards', 'Board', 0, True, 'Forum', False)

        if create_type_if_not_exists('Web'):
            dt.create_field('forums', 'Forum', 0, False, 'Web', False)
        

def init_data(from_scratch):
    if from_scratch:    
                
        
        f1  = dal.object.Forum()
        
        b1  = dal.object.Board()
        b11 = dal.object.Board()
        b12 = dal.object.Board()
        b2  = dal.object.Board()
        b3  = dal.object.Board()
        b4  = dal.object.Board()
        b5  = dal.object.Board()
        b51 = dal.object.Board()
        b52 = dal.object.Board()
        b6  = dal.object.Board()
        b61 = dal.object.Board()
        b62 = dal.object.Board()
        
        
        t11 = dal.object.Thread()
        t12 = dal.object.Thread()
        t2  = dal.object.Thread()
        t31 = dal.object.Thread()
        t32 = dal.object.Thread()
        t33 = dal.object.Thread()
        t34 = dal.object.Thread()
        t35 = dal.object.Thread()
        t41 = dal.object.Thread()
        t42 = dal.object.Thread()
        t43 = dal.object.Thread()
        t44 = dal.object.Thread()
        t45 = dal.object.Thread()        
        t51 = dal.object.Thread()
        t52 = dal.object.Thread()
        t53 = dal.object.Thread()
        t54 = dal.object.Thread()
        t55 = dal.object.Thread()
        t61 = dal.object.Thread()
        t62 = dal.object.Thread()
        t63 = dal.object.Thread()
        t64 = dal.object.Thread()
        t65 = dal.object.Thread()
        
        p1 = dal.object.Post()
        p2 = dal.object.Post()
        p3 = dal.object.Post()
        p4 = dal.object.Post()
        
        w = dal.object.Web()
        t = dal.object.Thread()
        
        '''CREEM EL FORUM'''
        w.forum = f1
        w.save()
        f1.name = 'Forum'
        f1.save()       
        
        '''CREEM ELS DIFERENTS BOARDS I ELS SEUS FILS'''
        
        '''BOARD 1'''
        b1.name = 'Dubtes'         
        b11.name = 'Usuaris premium'
        t11.title = "Problemes al fer login"  
        t11.save()
        b11.threads.append(t11)
        b11.save()      
        b12.name = 'Usuaris normals'
        t12.title = "Introduir cerques correctament"
        t12.posts.append(p1)
        t12.posts.append(p2)
        t12.save()
        b12.threads.append(t12)
        b12.save()    
        b1.sub_boards.append(b11)
        b1.sub_boards.append(b12)
        b1.save()

        '''BOARD 2'''        
        b2.name = 'Comentaris'
        t2.title = "Es poden fer agraiments?"
        t2.posts.append(p1)
        t2.posts.append(p2)
        t2.save()
        b2.threads.append(t2)
        b2.save()
        
        p1.title = "prova a"
        p1.text = "text a: lLaloa loa loasd loasdio askl"
        
        p2.title = "prova b" 
        p2.text = "text b: lLaloa loa loasd loasdio askl"
        
        p3.title = "prova c"
        p3.text = "text c: lLaloa loa loasd loasdio askl"
        
        p4.title = "prova d" 
        p4.text = "text d: lLaloa loa loasd loasdio askl"
        p1.save()
        p2.save()
        p3.save()
        p4.save()
        
        '''BOARD 3'''
        b3.name = 'Fruita'        
        t31.title = "Pomes"
        t31.save()
        t32.title = "Peres"
        t32.save()
        t33.title = "Taronges"
        t33.save()
        t34.title = "Platans"
        t34.save()
        t35.title = "Mandarines"
        t35.save()
        b3.threads.append(t31)
        b3.threads.append(t32)
        b3.threads.append(t33)
        b3.threads.append(t34)
        b3.threads.append(t35)
        b3.save()
        
        '''BOARD 4'''
        b4.name = 'Verdures'
        t41.title = "Patates"
        t41.save()
        t42.title = "Tomaquets"
        t42.save()
        t43.title = "Cols"
        t43.save()
        t44.title = "Pebrots"
        t44.save()
        t45.title = "Mongetes"
        t45.save()   
        b4.threads.append(t41)
        b4.threads.append(t42)
        b4.threads.append(t43)
        b4.threads.append(t44)
        b4.threads.append(t45)
        b4.save()  
        
        '''BOARD 5'''
        b5.name = 'Carns'   
        b51.name = 'Carns Blanques'
        t51.title = "Pollastres"
        t51.save()
        t52.title = "Galls"
        t52.save()
        b52.threads.append(t51)
        b52.threads.append(t52)
        b52.save()
        b5.sub_boards.append(b51)
        b52.name = 'Carns Vermelles'
        t53.title = "Porcs"        
        t53.save()
        t54.title = "Vedelles"
        t54.save()
        t55.title = "Xai"
        t55.save()
        b52.threads.append(t53)
        b52.threads.append(t54)
        b52.threads.append(t55)
        b52.save()
        b5.sub_boards.append(b52)
        b5.save()
        

        ''' BOARD 6 '''
        b6.name = 'Peixos'
        b61.name = 'Peix Blau'
        t61.title = "Tonyines"
        t61.save()
        t63.title = "Sardines"
        t63.save()
        t64.title = "Anxoves"
        t64.save()
        b61.threads.append(t61)
        b61.threads.append(t63)
        b61.threads.append(t64)
        b61.save()
        b6.sub_boards.append(b61)
        b62.name = 'Peix Blanc'
        t62.title = "Raps"
        t62.save()
        t65.title = "Bacalla"
        t65.save()
        b62.threads.append(t62)
        b62.threads.append(t65)
        b62.save()
        b6.sub_boards.append(b62)
        b6.save()
        
        f1.boards.append(b1)
        f1.save()
        f1.boards.append(b2)
        f1.save()
        f1.boards.append(b3)
        f1.save()
        f1.boards.append(b4)
        f1.save()
        f1.boards.append(b5)
        f1.save()
        f1.boards.append(b6)
        f1.save()
        
        
        

if __name__ == '__main__':
    do(True)


