import ibm.mq;
public class ClassicAPI{
	/*
     * Sends message asynchronously using JMS 2.0 classic API
     */
   public void classicAsyncSend(){
        JmsConnectionFactory cf = null;
        
        try {
            JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
            
            // Create connection factory
            cf = ff.createConnectionFactory();
            
            // Send connection parameters
            cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, "QM2");
            cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_BINDINGS);
            
            // Create connection
            Connection cn = cf.createConnection();
            
            // Create a non-transacted automatic message acknowledge session
            Session sn = cn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            //Create a topic destination
            Topic stockIndex = sn.createTopic("topic:///STOCKINDEX/FTSE");
            
            // Create a producer for sending message
            MessageProducer msgPrd = sn.createProducer(stockIndex);
           
            TextMessage txtMsg = sn.createTextMessage("FTSE100: 6663");
            
            // Create a callback object instance
            CompletionListener cl = new SendCompleteCallback();
            
            // Send message with a callback
            msgPrd.send(txtMsg,cl);

        }catch(JMSException jmsEx){
            System.out.println(jmsEx);
        }
    }
}