import javax.jms.DeliveryMode;
import ibm.mq.pcf;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQQueueConnectionFactory;

public class JMSApplicationStandAlone {
    public static void main(String[] args) {
        try {
            /*MQ Configuration*/
            MQQueueConnectionFactory mqQueueConnectionFactory = new MQQueueConnectionFactory();
            mqQueueConnectionFactory.setHostName("localhost");
            mqQueueConnectionFactory.setChannel("MQ.CHANNEL");//communications link
            mqQueueConnectionFactory.setPort(1416);
            mqQueueConnectionFactory.setQueueManager("QUEUE.MGR");//service provider 
            mqQueueConnectionFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);

            /*Create Connection */
            QueueConnection queueConnection = mqQueueConnectionFactory.createQueueConnection();
            queueConnection.start();

            /*Create session */
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            /*Create response queue */
            Queue queue = queueSession.createQueue("QUEUE.RESPONSE");


            /*Create text message */
            TextMessage textMessage = queueSession.createTextMessage("put some message here");
            textMessage.setJMSReplyTo(queue);
            textMessage.setJMSType("mcd://xmlns");//message type
            textMessage.setJMSExpiration(2*1000);//message expiration
            textMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT); //message delivery mode either persistent or non-persistemnt

            /*Create sender queue */
            QueueSender queueSender = queueSession.createSender(queueSession.createQueue("QUEUE.REQEST"));
            queueSender.setTimeToLive(2*1000);
            queueSender.send(textMessage);

            /*After sending a message we get message id */
            System.out.println("after sending a message we get message id "+ textMessage.getJMSMessageID());
            String jmsCorrelationID = " JMSCorrelationID = '" + textMessage.getJMSMessageID() + "'";


            /*Within the session we have to create queue reciver */
            QueueReceiver queueReceiver = queueSession.createReceiver(queue,jmsCorrelationID);


            /*Receive the message from*/
            Message message = queueReceiver.receive(60*1000);
            String responseMsg = ((TextMessage) message).getText();

            queueSender.close();
            queueReceiver.close();
            queueSession.close();
            queueConnection.close();


        } catch (JMSException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
