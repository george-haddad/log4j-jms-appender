package test;

import java.io.File;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class TestJMSAppender implements MessageListener {

        private static Logger logger = null;

        public TestJMSAppender() {
                try {
                        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://myJmsServer.net:61616");
                        TopicConnection conn = factory.createTopicConnection();
                        TopicSession ses = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
                        conn.start();

                        for (int i = 0; i < 10; i++) {
                                logger.info("Testing ....");
                                Thread.sleep(4000);
                                logger.info("Hello World!");
                        }

                        Thread.sleep(1000);
                        ses.close();
                        conn.close();

                        logger.removeAllAppenders();
                }
                catch (Exception ex) {
                        ex.printStackTrace();
                }
        }

        public void onMessage(Message msg) {

        }

        public static void main(String... args) {
                File configfile = new File(System.getProperty("user.dir") + File.separator + "src" + File.separator + "test" + File.separator + "log4j.properties");
                PropertyConfigurator.configure(configfile.getAbsolutePath());
                logger = Logger.getLogger("test");

                new TestJMSAppender();
        }
}
