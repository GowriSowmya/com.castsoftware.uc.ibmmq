import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.ibm.websphere.sib.api.jms.JmsConnectionFactory;
import com.ibm.websphere.sib.api.jms.JmsFactoryFactory;
import com.ibm.websphere.sib.api.jms.JmsQueue;
import com.ibm.websphere.sib.api.jms.JmsTopic;

/**
 * Sample code to programmatically create a connection to a bus and
 * send a text message.
 * 
 * Example command lines:
 *   SIBusSender topic://my/topic?topicSpace=Default.Topic.Space MyBus localhost:7276
 *   SIBusSender queue://myQueue MyBus localhost:7286:BootstrapSecureMessaging InboundSecureMessaging 
 */
public class SIBusSender {
  
  /**
   * @param args DEST_URL,BUS_NAME,PROVIDER_ENDPOINTS,[TRANSPORT_CHAIN]
   */
  public static void main(String[] args) throws JMSException, IOException {
    
    // Parse the arguments
    if (args.length < 3) {
      throw new IllegalArgumentException(
          "Usage: SIBusSender <DEST_URL> <BUS_NAME> <PROVIDER_ENDPOINTS> [TARGET_TRANSPORT_CHAIN]");
    }    
    String destUrl = "TestTopic";
    String busName = args[1];
    String providerEndpoints = args[2];    
    String targetTransportChain = "InboundBasicMessaging";
    if (args.length >= 4) targetTransportChain = args[3];
    
    // Obtain the factory factory
    JmsFactoryFactory jmsFact = JmsFactoryFactory.getInstance();

    // Create a JMS destination
    Destination dest;
    if (destUrl.startsWith("\\topic:")) {
      JmsTopic topic = jmsFact.createTopic(destUrl);
      // Setter methods could be called here to configure the topic
      dest = topic ;
    }
    else {
      JmsQueue queue = jmsFact.createQueue(destUrl);
      // Setter methods could be called here to configure the queue
      dest = queue;
    }
        
    // Create a unified JMS connection factory
    JmsConnectionFactory connFact = jmsFact.createConnectionFactory();
    
    // Configure the connection factory
    connFact.setBusName(busName);
    connFact.setProviderEndpoints(providerEndpoints);
    connFact.setTargetTransportChain(targetTransportChain);
    
    // Create the connection
    Connection conn = connFact.createConnection();
    
    Session session = null;
    MessageProducer producer = null;
    try {
      
      // Create a session
      session = conn.createSession(false, // Not transactional 
                                   Session.AUTO_ACKNOWLEDGE);
      
      // Create a message producer
      producer = session.createProducer(dest);
      
      // Loop reading lines of text from the console to send
      System.out.println("Ready to send to " + dest + " on bus " + busName);
      BufferedReader lineInput = new BufferedReader(new InputStreamReader(System.in));      
      String line = lineInput.readLine();
      while (line != null && line.length() > 0) {
        
        // Create a text message containing the line
        TextMessage message = session.createTextMessage();
        message.setText(line);
        
        // Send the message
        producer.send(message,
                      Message.DEFAULT_DELIVERY_MODE,
                      Message.DEFAULT_PRIORITY,
                      Message.DEFAULT_TIME_TO_LIVE);        
        
        // Read the next line
        line = lineInput.readLine();
      }
      
    }
    // Finally block to ensure we close our JMS objects 
    finally {
      
      // Close the message producer
      try {
        if (producer != null) producer.close();
      }
      catch (JMSException e) {
        System.err.println("Failed to close message producer: " + e);
      }
      
      // Close the session
      try {
        if (session != null) session.close();
      }
      catch (JMSException e) {
        System.err.println("Failed to close session: " + e);
      }
      
      // Close the connection
      try {
        conn.close();
      }
      catch (JMSException e) {
        System.err.println("Failed to close connection: " + e);
      }
      
    }
  }

}
