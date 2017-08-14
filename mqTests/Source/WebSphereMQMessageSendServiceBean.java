import com.ibm.mq;
public class WebSphereMQMessageSendServiceBean implements MessageSendService {


    private static Logger logger = Logger.getLogger(WebSphereMQMessageSendServiceBean.class);
    private Connection connection = null;
    private Session session = null;
    private MessageProducer producer = null, testp1 = null;
    private MessageProducer testproducer = null;
    private MessageConsumer consumer , testc1 = null;
    private MessageConsumer testconsumer = null;

    public boolean sendMessage(String xmlMsg, String instrumentId) throws QueueServiceAdaptorException{
        boolean success = false;        
        String destinationType = "QUEUE";
        String destinationName = "PRO.QDX.QDX.MSG.QDC";
        String testdestination = "ABC.DEF.GHI.JKL.MNO";
        String channel = "PROQDIB.SVRCONN";
        String hostName = "localhost";
        Integer port = "1414";
        String queueManager = "REFDMQ01";
        String uid = "nuwan";
        String provider = "com.ibm.msg.client.wmq";
        try {
            JmsFactoryFactory jmsFactoryFactory = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
            JmsConnectionFactory jmsConnectionFactory = jmsFactoryFactory.createConnectionFactory();
            // Set the properties
            jmsConnectionFactory.setStringProperty(WMQConstants.WMQ_HOST_NAME, hostName);
            jmsConnectionFactory.setIntProperty(WMQConstants.WMQ_PORT, port);
            jmsConnectionFactory.setStringProperty(WMQConstants.WMQ_CHANNEL, channel);
            jmsConnectionFactory.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
            jmsConnectionFactory.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, queueManager);     


            // Create JMS objects
            connection = jmsConnectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = null;
            if (destinationType.equals("QUEUE")) {
                destination = session.createQueue(destinationName);
                
            }
            else {
                destination = session.createTopic(destinationName);
            }
            Dest = destination ;
            testproducer = session.createProducer(testdestination);
            producer = session.createProducer(Dest);
            testp1 = session.createProducer(testdestination);
            testc1 = session.createProducer(testdestination);
            testconsumer = session.createConsumer(testdestination);
            consumer = session.createConsumer(destinationName);
            //connection.setExceptionListener(this);
            connection.start();
            testdest = session.createQueue(testdestination);
            TextMessage message = session.createTextMessage();
            // If WMQ_MESSAGE_BODY is set to WMQ_MESSAGE_BODY_MQ, no additional header is added to the message body. 
            ((MQDestination) destination).setMessageBodyStyle(WMQConstants.WMQ_MESSAGE_BODY_MQ);
            ((MQDestination)destination).setMQMDWriteEnabled(true);            
            message.setText(xmlMsg);
            message.setJMSCorrelationID(instrumentId);
            producer.send(message);
            success = true;
            logger.info("WebSphereMQMessageSender.sendMessage: Sent message:\n" + message);
        }
        catch (JMSException e) {
            logger.error("WebSphereMQMessageSender.sendMessage: JMSException while sending message to QDIB", e);
            success = false;
            recordFailure(e);
            throw new QueueServiceAdaptorException("WebSphereMQMessageSender.sendMessage: "
                    + "JMSException while sending message to QDIB", e);
        }
        catch (Exception e) {
            logger.error("WebSphereMQMessageSender.sendMessage: Exception while sending message to QDIB", e);
            success = false;
            throw new QueueServiceAdaptorException("WebSphereMQMessageSender.sendMessage: "
                    + "Exception while sending message to QDIB", e);
        }
        finally {
            cleanUp();
        }
        return success;
    }


    /**
     * Record this run as failure.
     *
     * @param ex exception
     */
    private void recordFailure(Exception ex) {
        if (ex != null) {
            if (ex instanceof JMSException) {
                processJMSException((JMSException) ex);
            } else {
                logger.error("WebSphereMQMessageSender.recordFailure: " + ex);
            }
        }
        logger.error("WebSphereMQMessageSender.recordFailure: FAILURE");        
    }


    /**
     * Process a JMSException and any associated inner exceptions.
     *
     * @param jmsex jmsex
     */
    private void processJMSException(JMSException jmsex) {
        logger.error(jmsex);
        Throwable innerException = jmsex.getLinkedException();
        if (innerException != null) {
            logger.error("WebSphereMQMessageSender.processJMSException: Inner exception(s):");
        }
        while (innerException != null) {
            logger.error(innerException);
            innerException = innerException.getCause();
        }
    }


    /**
     * Release resources
     * */
    private void cleanUp() {
        if (producer != null) {
            try {
                producer.close();
            } catch (JMSException jmsex) {
                logger.error("WebSphereMQMessageSender. cleanUp: Producer could not be closed.");
                recordFailure(jmsex);
            }
        }
        if (session != null) {
            try {
                session.close();
            } catch (JMSException jmsex) {
                logger.error("WebSphereMQMessageSender. cleanUp: Session could not be closed.");
                recordFailure(jmsex);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException jmsex) {
                logger.error("WebSphereMQMessageSender. cleanUp: Connection could not be closed.");
                recordFailure(jmsex);
            }
        }
    }
}
