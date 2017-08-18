# com.castsoftware.uc.ibmmq

Supports of IBM MQ 7.0

IBM MQ Extension

Create objects of producer, consumer and queue instances by parsing the source code. Create links between Producers and Consumers based on the Queue through which they are connected with and also a link from the calling method. If Queue name is declared as a variable then resolve it within the method. If not present within the method then search within the class.

Limitations:

String resolution for the Queue name happens only when present within the method or as a class. Else an object is created for the variable in which queue name would be stored.
