import ibm.mq;
/*
     * A class that implements CompletionListener interface
     *
     */
    public class SendCompleteCallback implements CompletionListener {
        /*
         * (non-Javadoc)
         * @see javax.jms.CompletionListener#onCompletion(javax.jms.Message)
         */
        @Override
        public void onCompletion(Message msg) {
            // Message was successfully sent. Let's just print it for confirmation
            try{
            System.out.print(msg.getBody(String.class));
            }
            catch(Exception ex){
            }
        }
    
        /*
         * (non-Javadoc)
         * @see javax.jms.CompletionListener#onException(javax.jms.Message, java.lang.Exception)
         */
        @Override
        public void onException(Message msg, Exception e) {
            /*
             * Asynchronous send has failed. Just print exception
             */
          e.printStackTrace();
        }        
       
    /*
     * Sends message asynchronously using JMS 2.0 Simplified API
     */
   public void simplifiedAsyncSend() {
        JmsConnectionFactory cf = null;
        JMSContext msgContext = null;
        CompletionListener cl = new SendCompleteCallback();
        
        try {
            // Set up connection factory and message context.
            JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
            cf = ff.createConnectionFactory();
            cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, "QM2");
            cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_BINDINGS);
            msgContext = cf.createContext();
            
            // Create a topic for sending message.
            Topic stockIndex = msgContext.createTopic("topic:///STOCKINDEX/FTSE");
            
            // Create a JMS producer for sending message
            JMSProducer msgProducer = msgContext.createProducer();
    
            // Set completion listener and publish a message
            msgProducer.setAsync(cl);
            msgProducer.send(stockIndex, "FTSE100: 6663");
            
            // Remove callback. Publish message synchronously
            msgProducer.setAsync(null);
            msgProducer.send(stockIndex, "FTSE100: 6649");

        }catch(JMSException jmsEx){
            System.out.println(jmsEx);
        }
    
   }
}