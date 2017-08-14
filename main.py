from cast.analysers import CustomObject, create_link
import cast.analysers.jee
import cast.analysers.log as CAST
from MqParser import parser

class MyExtension(cast.analysers.jee.Extension):
    
    def __init__(self):
        self.object_names=set()                                                       # for storing object's names
        self.objects = set()                                                          # for storing objects
        self.bookmarks = {}                                                            # for storing Method's positions
        self.paths = []                                                               

    def start_analysis(self,options):
        CAST.debug(' Hello World!! ')
        
    def createObj(self,obj,name,parent,fullname,Type):                                  # function to create an object
        obj.set_name(name)
        obj.set_parent(parent)
        obj.set_fullname(fullname)
        obj.set_type(Type)
        obj.save()
        CAST.debug(str(obj.name))      
        
    def start_member(self, member):  
        if "'cast.analysers.Method'" in str(type(member)):
            self.bookmarks[member] = (member.get_positions())                           # getting positions of Method within a file
        
            
    def end_type(self, _type):
        class_bookmark = _type.get_positions()
        name=str(_type.get_name())                                                      # fetching name of class or interface
        path = _type. get_position().get_file().get_path()                              # fetching path of required file
        CAST.debug("-----"+str(path)+"-----")                                           
        self.object_names=[]
        self.objects = []
        method_range = [[x,y[0].get_begin_line(),y[0].get_end_line()] for x,y in self.bookmarks.items()]
        CAST.debug("Method_range: "+str(method_range)+"     Class Bookmark: "+str(class_bookmark[0]))
        Result= parser(path,method_range,class_bookmark[0])
        if Result:
            CAST.debug(" Creating Objects:    ")                                  # Creating objects for producer, consumer variables and queue name
            for key in Result.keys():
                if "MessageProducer" in Result[key][0] or "JMSProducer" in Result[key][0] or "QueueSender" in Result[key][0] or "TopicPublisher" in Result[key][0]:
                    o = CustomObject()
                    Object_created = True
                    parent_link = list(filter(lambda x: Result[key][1] in range(x[1],x[2]),method_range))
                    if parent_link: self.createObj(o,name+"_"+str(key),parent_link[0][0],"obj"+str(key),"MOM_Producer")  
                    elif Result[key][1] in range(class_bookmark[0].get_begin_line(),class_bookmark[0].get_end_line()):   self.createObj(o,name+"_"+str(key),_type,"obj"+str(key),"MOM_Producer")
                    else: Object_created = False
                    if Object_created:
                        self.object_names.append(key)
                        self.objects.append(o)
                elif "TopicSubscriber" in Result[key][0] or "JMSConsumer" in Result[key][0] or "MessageConsumer" in Result[key][0] or "QueueReceiver" in Result[key][0]:
                    o = CustomObject()
                    Object_created = True
                    parent_link = list(filter(lambda x: Result[key][1] in range(x[1],x[2]),method_range))
                    if parent_link: self.createObj(o,name+"_"+str(key),parent_link[0][0],"obj"+str(key),"MOM_Consumer") 
                    elif Result[key][1] in range(class_bookmark[0].get_begin_line(),class_bookmark[0].get_end_line()):   self.createObj(o,name+"_"+str(key),_type,"obj"+str(key),"MOM_Consumer")
                    else: Object_created = False
                    if Object_created:
                        self.object_names.append(key)
                        self.objects.append(o)
                if Result[key][-1] not in self.object_names:
                    p = CustomObject()
                    Object_created = True
                    parent_link = list(filter(lambda x: Result[key][1] in range(x[1],x[2]),method_range)) 
                    if parent_link: self.createObj(p,name+"_"+str(Result[key][-1]),parent_link[0][0],"obj"+str(key),"MOM_Queue")
                    elif Result[key][1] in range(class_bookmark[0].get_begin_line(),class_bookmark[0].get_end_line()):   self.createObj(p,name+"_"+str(Result[key][-1]),_type,"obj"+str(key),"MOM_Queue")
                    else: Object_created = False
                    if Object_created:
                        self.object_names.append(str(Result[key][-1]))
                        self.objects.append(p)   
            CAST.debug(" Creating Links:    ")
            if self.objects:
                for key in Result.keys():
                    class_variable = True
                    for bk in self.bookmarks.keys():                                                # for linking Method objects with producer and consumer objects
                        x,y = self.bookmarks[bk][0].get_begin_line() , self.bookmarks[bk][0].get_end_line()
                        if Result[key][1] in range(x,y+1):
                            obj1 = bk
                            obj2 = self.objects[self.object_names.index(key)]
                            create_link('useLink',obj1,obj2)
                            class_variable = False
                            CAST.debug(str(obj1)+" --links with-- "+obj2.name)
                    if class_variable == True:
                        obj1 = _type
                        obj2 = self.objects[self.object_names.index(key)]
                        create_link('useLink',obj1,obj2)
                        CAST.debug(str(obj1)+" --links with-- "+obj2.name)
                for key in Result.keys():                                                                        # for linking producer or consumer objects with queue object
                    obj1 = self.objects[self.object_names.index(key)]
                    obj2 = self.objects[self.object_names.index(Result[key][-1])]
                    if "MessageProducer" in Result[key][0] or "QueueSender" in Result[key][0] or "JMSProducer" in Result[key][0] or "TopicPublisher" in Result[key][0]:
                        create_link('useLink', obj1, obj2)
                        CAST.debug(obj1.name+" --linked with-- "+obj2.name)
                    else:
                        create_link('useLink', obj2, obj1)
                        CAST.debug(obj2.name+" --linked with-- "+obj1.name)   
                self.bookmarks={}    
    
if __name__ == '__main__':
    pass
