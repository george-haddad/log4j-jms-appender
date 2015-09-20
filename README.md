# log4j-jms-appender
Log4J JMS Asynchronous Appender

A JMS Appender modified from Ceki G&uuml;lc&uuml;'s implementation. The modification made were to make the appender asynchronous so that the rate of logging from the application would not affect the rate of sending the logs over the wire to a JMS server. This also has the added bonus of serializing the log messages to disk if the JMS connection is down and then resending everything once the connection is back online.
