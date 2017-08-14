import re
import cast.analysers.log
def parser(filename,method_range,class_bookmark):
    fin = open(filename,'r')
    file_data = []                                                                              # list containing file data without comments
    parse_condition = 0                                                                            
    cmt = False                                       
    for child in fin:                                                                          # Removing single line and multiple line comments from java file
        stripped = child.strip()
        if ("ibm.mq" in stripped or "ibm.websphere") and ("import" in stripped): parse_condition = 1
        if cmt:
            file_data.append('*')
            if stripped.endswith('*/'): cmt = False
        elif stripped.startswith('/*'):
            file_data.append('*')
            cmt = True
        elif stripped.startswith('//') or "print" in stripped or "import" in stripped or "package" in stripped:
            file_data.append('*')
        else:  file_data.append(child)
    if parse_condition == 1:
        cast.analysers.log.debug(" Parse Condition - True")
        return(internalparse(file_data,method_range,class_bookmark))
    else:
        cast.analysers.log.debug(" Parse Condition - False")
        return

def findString(var,content,dictP,p):                                                        # to get string (queue name) eliminating variables in between
    while("'" not in var and '"' not in var and var):
        count_i=0
        count_n = len(content)
        for ln in content[::-1]:                                                            
            count_i += 1
            if re.search("\w*\s+"+var+"\s*=",ln):                                           # searching and extracting variables
                ''' MessageProducer   producer   =    
                    consumer  =     '''
                if "create" in ln and "null" not in ln : 
                    var = ln[ln.index('(')+1:ln.index(')')].strip()
                    '''     Topic stockIndex = sn.createTopic("topic://STOCKINDEX/FTSE");    '''
                elif "new" in ln:
                    var = ln[ln.index('(')+1:ln.index(';')-1]
                    '''     Queue queue = new MQQueue(getQueueName());    '''
                elif "lookup" in ln :
                    print("lookup   "+ln)
                    ln = ln.replace('(',' ')
                    ln = ln .replace(')',' ')
                    var =  ln.split()[-2]
                    '''    topic = (Topic) iniCtx.lookup("topic/testTopic");     '''
                elif "create" not in ln and "null" not in ln:
                    var = ln[ln.index('=')+1:ln.index(';')].strip()
                    '''     Dest = destination ;       '''
                else:
                    var = None
                    return 0
                dictP[p].append(var)                                                              # appending variables to the list               
            else:
                if count_i == count_n: 
                    return 0                                                                            
    if '"' in var or "'" in var:
        var = var.replace("'",'"')
        var = var[var.index('"')+1:len(var) - var[::-1].index('"')]
        dictP[p].append('"'+var)                                                                 # appending queue name to the list
        '''     String destinationName = "PRO.QDX.QDX.MSG.QDC";      '''   
        return 1                       
        
def internalparse(file_data,method_range,class_bookmark):    
    list1=[]                                                                                     # list of producers and consumers
    dictP={}                                                                                     # dictionary with producers & consumers as keys and variables and queue name as values in a list
    method_search = []                                                                               
    tags = "\w*MessageProducer\w*|\w*MessageConsumer\w*|\w*createProducer\w*|\w*createConsumer\w*|\\w*\.send\s*\(\w*,*\s*|\w*JMSProducer\w*|\w*JMSConsumer\w*|\w*QueueSender\w*|\w*QueueReceiver\w*|\w*QueueBrowser\w*|\w*createSender\w*|\w*createReceiver\w*|\w*createBrowser\w*|\w*createDurableSubscriber\w*|\w*TopicPublisher\w*|\w*TopicSubscriber\w*|\w*createSubscriber\w*|\w*createPublisher\w*|\\w*\.publish\s*\(\w*,*\s*"
    content =[]                                                                 
    content.extend(file_data)
    lineC = 0
    for line in file_data:
        lineC += 1                                                              
        if re.search(tags,line):
            j = line.strip().split()
            key = re.search(tags,line).group()                                                   # searching for patterns defined in tags list            
            if ("MessageProducer" in line or "MessageConsumer" in line or "JMSProducer" in line or "JMSConsumer" in line or "QueueSender" in line or "QueueReceiver" in line or "QueueBrowser" in line or "TopicPublisher" in line or "TopicSubscriber" in line) and "create" in line:
                '''   QueueSender queueSender = queueSession.createSender(queueSession.createQueue("QUEUE.REQEST"));    '''
                p = j[j.index(key)+1]
                list1.append(p)
                dictP[p] = []
                dictP[p].extend([line,lineC])                                                                                                           # for linking with Method
                k = list(filter(lambda x : "createProducer" in x or "createConsumer" in x or "createSender" in x or "createReceiver" in x or "createBrowser" in x or "createSubscriber" in x or "createPublisher" in x or "createDurableSubscriber" in x ,j))
                l = k[0]
                for i in method_range:                                                                 # i[0] : member , i[1] : start line , i[2] : end line
                    if lineC in range(i[1],i[2]+1):
                        method_search = [i[1],lineC]
                    else:   method_search = [class_bookmark.get_begin_line(),class_bookmark.get_end_line()]
                if "(" not in l: l = line[line.index('='):]
                if "'" in l or '"' in l: 
                    k = l[l.index('(')+1:l.index(')')]                                                   # extracting value within brackets
                    if "(" in k and "create" in k:  k = k[k.index('(')+1:] 
                    if k:
                        dictP[p].append(k)
                        list1.remove(p)
                else:
                    var = l[l.index('(')+1:l.index(')')].strip()
                    if "," in var:  var = var.split(",")[0]
                    if not var and "=" in l :   var = l[l.index('=')+1:].strip()                  # extracting assigned value
                    if not var :  pass
                    if "(" in var and "create" in var: var = var[var.index('(')+1:] 
                    if var:
                        dictP[p].append(var)
                        findString(var,content[method_search[0]:],dictP,p)
                        list1.remove(p)
            elif key == "MessageProducer" or key == "MessageConsumer" or key == "JMSProducer" or key == "JMSConsumer" or key == "QueueSender" or key == "QueueReceiver" or key == "QueueBrowser" or key == "TopicSubscriber" or key == "TopicPublisher":
                '''    MessageProducer producer = null;     '''
                if ',' in line:
                    z = line[line.index(j[j.index(key)+1]):line.index(';')]
                    z = z.strip().split(',')
                    for q in z:
                        if ";" in q: q = q[:q.index(';')]
                        if "=" in q: q = q[:q.index("=")]
                        q=q.strip()
                        list1.append(q)
                        dictP[q] = []
                        dictP[q].extend([line,lineC])
                else:
                    p = j[j.index(key)+1]
                    if ";" in p: p = p[:p.index(';')]
                    if "=" in p: p = p[:q.index("=")]
                    list1.append(p)
                    dictP[p] = []
                    dictP[p].extend([line,lineC])
            elif key == "createProducer" or key == "createConsumer" or key == "createSender" or key == "createReceiver" or key == "createBrowser" or key == "createPublisher" or key == "createSubscriber" or key == "createDurableSubscriber":
                '''     producer = session.createProducer(dest);     '''
                for p in list1:
                    if p in j:
                        k = list(filter(lambda x : key in x,j))
                        l = k[0]
                        if "(" not in l:  l = line[line.index('=')+1:line.index(';')]
                        if "'" in l or '"' in l:                                                 # extracting queue name withing quotes
                            k = l[l.index('(')+1:l.index(')')] 
                            if "(" in k and "create" in k: k = k[k.index('(')+1:]
                            dictP[p].append(k)
                            list1.remove(p)
                        else:
                            var = l[l.index('(')+1:l.index(')')].strip()
                            if "(" in var and "create" in var: var = var[var.index('(')+1:] 
                            if not var :  var = l[l.index('=')+1:].strip()
                            elif not var : continue 
                            if 'null' in var: continue
                            if var: dictP[p].append(var)
                            for i in method_range:                                                # i[0] : member , i[1] : start line , i[2] : end line
                                if lineC in range(i[1],i[2]+1):
                                    method_search = [i[1],lineC]
                                else:   method_search = [class_bookmark.get_begin_line(),class_bookmark.get_end_line()]
                            findString(var,content[method_search[0]:],dictP,p)
                            list1.remove(p)
            elif (".send" in key or ".publish" in key) and p in line:
                '''     producer.send(message,cl);
                        msgPrd.send(Dest,msg);    '''
                if "," in key: var = key[key.index('(')+1:key.index(',') ]                       # extracting queue name
                else: var = key[key.index('(')+1: ][0]
                if len(dictP[key[:key.index('.')]]) == 2 and var:
                    if list1:
                        dictP[key[:key.index('.')]].extend([lineC,str(var)])
                        list1.remove(key[:key.index('.')])
                        for i in method_range:
                            if lineC in range(i[1],i[2]+1):
                                method_search = [i[1],lineC]
                            else:  method_search = [class_bookmark.get_begin_line(),class_bookmark.get_end_line()]  
                        findString(var,content[method_search[0]:],dictP,p)            
    return(dictP)

#print(parser('C:\\Users\\GSO\\eclipse-workspace2\\com.castsoftware.ibmmq\\mqTests\\Tests\\TopicSendRecvClient.java',[['Method(org.jboss.book.jms.ex1.TopicSendRecvClient.main)', 87, 97], ['Method(org.jboss.book.jms.ex1.TopicSendRecvClient.sendRecvAsync)', 61, 78], ['Method(org.jboss.book.jms.ex1.TopicSendRecvClient.stop)', 80, 85], ['Method(org.jboss.book.jms.ex1.TopicSendRecvClient.setupPubSub)', 48, 59]]))
if __name__ == '__main__':
    pass