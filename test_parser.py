'''Created on Aug 7, 2017   @author: GSO'''
import unittest
import tempfile
from MqParser import parser
from cast.analysers import Bookmark
class TestParser(unittest.TestCase):    
    def test_parse_java(self):
        
        java_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.py', delete=False)
        print(java_file.name)
        #java_file.close()
        #java_file.open("w+")
        java_file.write("""
public class TopicRecvClient
{
    TopicConnection conn = null;
    TopicSession session = null;
    Topic topic = null;
    
    public void setupPubSub()
        throws JMSException, NamingException
    {
        InitialContext iniCtx = new InitialContext();
        Object tmp = iniCtx.lookup("ConnectionFactory");
        TopicConnectionFactory tcf = (TopicConnectionFactory) tmp;
        conn = tcf.createTopicConnection();
        topic = (Topic) iniCtx.lookup("topic/testTopic");
        session = conn.createTopicSession(false,
                                          TopicSession.AUTO_ACKNOWLEDGE);
        conn.start();
    }
    
    public void recvSync()
        throws JMSException, NamingException
    {
        System.out.println("Begin recvSync");
        // Setup the pub/sub connection, session
        setupPubSub();

        // Wait upto 5 seconds for the message
        TopicSubscriber recv = session.createSubscriber(topic);
        Message msg = recv.receive(5000);
        if (msg == null) {
            System.out.println("Timed out waiting for msg");
        } else {
            System.out.println("TopicSubscriber.recv, msgt="+msg);
        }
    }
    
}        """)
        java_file.close()
        
        #result = parser(java_file.name)
        result = parser('C:\\Users\\GSO\\eclipse-workspace2\\com.castsoftware.ibmmq\\mqTests\\Tests\\TopicRecvClient.java', [['1Method(org.jboss.book.jms.ex1.TopicRecvClient.recvSync)', 42, 57], ['Method(org.jboss.book.jms.ex1.TopicRecvClient.main)', 67, 77], ['Method(org.jboss.book.jms.ex1.TopicRecvClient.setupPubSub)', 29, 40], ['Method(org.jboss.book.jms.ex1.TopicRecvClient.stop)', 59, 65]],Bookmark('C:\\Users\\GSO\\eclipse-workspace2\\com.castsoftware.ibmmq\\mqTests\\Tests\\TopicRecvClient.java',17,1,79,2) )
        self.assertTrue('recv' in result)
        
        value = result['recv']
        #declaration = value[0]
        declaration_line = value[0]
        declaration_line_Count = value[1]
        
        #variable_assignement = value[4]
        variable_value = value[-1]
        
        self.assertEqual('"topic/testTopic"', variable_value)

    @unittest.skip
    def test_infinite_loop_correct_me(self):

        java_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.py', delete=False)
        java_file.write("""
public class TopicRecvClient
{
    TopicConnection conn = null;
    TopicSession session = null;
    Topic topic = null;
    

    public void recvSync()
        throws JMSException, NamingException
    {
        System.out.println("Begin recvSync");
        // Setup the pub/sub connection, session
        setupPubSub();

        // Wait upto 5 seconds for the message
        TopicSubscriber recv = session.createSubscriber(topic);
        Message msg = recv.receive(5000);
        if (msg == null) {
            System.out.println("Timed out waiting for msg");
        } else {
            System.out.println("TopicSubscriber.recv, msgt="+msg);
        }
    }
    
}        """)
        java_file.close()
        
        #result = parser(java_file.name)
        result = parser('C:\\Users\\GSO\\eclipse-workspace2\\com.castsoftware.ibmmq\\mqTests\\Tests\\TopicRecvClient.java', [['2Method(org.jboss.book.jms.ex1.TopicRecvClient.recvSync)', 42, 57], ['Method(org.jboss.book.jms.ex1.TopicRecvClient.main)', 67, 77], ['Method(org.jboss.book.jms.ex1.TopicRecvClient.setupPubSub)', 29, 40], ['Method(org.jboss.book.jms.ex1.TopicRecvClient.stop)', 59, 65]],Bookmark('C:\\Users\\GSO\\eclipse-workspace2\\com.castsoftware.ibmmq\\mqTests\\Tests\\TopicRecvClient.java',17,1,79,2) )
        self.assertTrue('recv' in result)
        #self.assertEqual('"topic/testTopic"', variable_value)


if __name__ == "__main__":
    unittest.main()
    
    