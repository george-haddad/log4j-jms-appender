/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package george.log4j.jmsappender;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.jms.JMSException;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A simple appender that publishes events to a JMS Topic. The events are
 * serialized and transmitted as JMS message type {@link ObjectMessage}.
 * 
 * <p>
 * JMS {@link Topic topics} and {@link TopicConnectionFactory topic connection
 * factories} are administered objects that are retrieved using JNDI messaging
 * which in turn requires the retrieval of a JNDI {@link Context}.
 * 
 * <p>
 * There are two common methods for retrieving a JNDI {@link Context}. If a file
 * resource named <em>jndi.properties</em> is available to the JNDI API, it will
 * use the information found therein to retrieve an initial JNDI context. To
 * obtain an initial context, your code will simply call:
 * 
 * <pre>
 * InitialContext jndiContext = new InitialContext();
 * </pre>
 * 
 * <p>
 * Calling the no-argument <code>InitialContext()</code> method will also work
 * from within Enterprise Java Beans (EJBs) because it is part of the EJB
 * contract for application servers to provide each bean an environment naming
 * context (ENC).
 * 
 * <p>
 * In the second approach, several predetermined properties are set and these
 * properties are passed to the <code>InitialContext</code> constructor to
 * connect to the naming service provider. For example, to connect to JBoss
 * naming service one would write:
 * 
 * <pre>
 * Properties env = new Properties();
 * env.put(Context.INITIAL_CONTEXT_FACTORY, &quot;org.jnp.interfaces.NamingContextFactory&quot;);
 * env.put(Context.PROVIDER_URL, &quot;jnp://hostname:1099&quot;);
 * env.put(Context.URL_PKG_PREFIXES, &quot;org.jboss.naming:org.jnp.interfaces&quot;);
 * InitialContext jndiContext = new InitialContext(env);
 * </pre>
 * 
 * where <em>hostname</em> is the host where the JBoss application server is
 * running.
 * 
 * <p>
 * To connect to the the naming service of Weblogic application server one would
 * write:
 * 
 * <pre>
 * Properties env = new Properties();
 * env.put(Context.INITIAL_CONTEXT_FACTORY, &quot;weblogic.jndi.WLInitialContextFactory&quot;);
 * env.put(Context.PROVIDER_URL, &quot;t3://localhost:7001&quot;);
 * InitialContext jndiContext = new InitialContext(env);
 * </pre>
 * 
 * <p>
 * Other JMS providers will obviously require different values.
 * 
 * The initial JNDI context can be obtained by calling the no-argument
 * <code>InitialContext()</code> method in EJBs. Only clients running in a
 * separate JVM need to be concerned about the <em>jndi.properties</em> file and
 * calling {@link InitialContext#InitialContext()} or alternatively correctly
 * setting the different properties before calling
 * {@link InitialContext#InitialContext(java.util.Hashtable)} method.
 * 
 * <p>
 * Made the appender Asynchronous with a linked blocking queue. - George El-Haddad
 * 
 * @author Ceki G&uuml;lc&uuml;
 * @author Georges El-Haddad - george.dma@gmail.com
 */
public class JMSAppender extends AppenderSkeleton {
        // This is hard coded for testing purposes
        private static String OBJECT_STORE_PATH = "/opt/backup_store";

        private static final BlockingQueue<LoggingEvent> loggingEventQueue = new LinkedBlockingQueue<LoggingEvent>();
        private static JMSAppender instance;

        private String securityPrincipalName;
        private String securityCredentials;
        private String initialContextFactoryName;
        private String urlPkgPrefixes;
        private String providerURL;
        private String topicBindingName;
        private String tcfBindingName;
        private String userName;
        private String password;
        private String jmsTopic;
        private boolean locationInfo;

        private int fileCounter = 0;
        private boolean connected = true;
        private long disconnectTime = 0;
        private long reconnectDelay = 300000;

        private TopicConnection topicConnection;
        private TopicSession topicSession;
        private TopicPublisher topicPublisher;
        private static Thread thread = null;

        static {
                thread = new Thread(new Runnable() {
                        public void run() {
                                processQueue();
                        }
                });

                thread.setDaemon(true);
                thread.start();
        }

        public JMSAppender() {
                super();
                instance = this;
        }

        /**
         * The <b>TopicConnectionFactoryBindingName</b> option takes a string
         * value. Its value will be used to lookup the appropriate
         * <code>TopicConnectionFactory</code> from the JNDI context.
         */
        public void setTopicConnectionFactoryBindingName(String tcfBindingName) {
                this.tcfBindingName = tcfBindingName;
        }

        /**
         * Returns the value of the <b>TopicConnectionFactoryBindingName</b>
         * option.
         */
        public String getTopicConnectionFactoryBindingName() {
                return tcfBindingName;
        }

        /**
         * The <b>TopicBindingName</b> option takes a string value. Its value
         * will be used to lookup the appropriate <code>Topic</code> from the
         * JNDI context.
         */
        public void setTopicBindingName(String topicBindingName) {
                this.topicBindingName = topicBindingName;
        }

        /**
         * Returns the value of the <b>TopicBindingName</b> option.
         */
        public String getTopicBindingName() {
                return topicBindingName;
        }

        /**
         * Returns value of the <b>LocationInfo</b> property which determines
         * whether location (stack) info is sent to the remote subscriber.
         */
        public boolean getLocationInfo() {
                return locationInfo;
        }

        /**
         * Options are activated and become effective only after
         * calling this method.
         */
        @Override
        public void activateOptions() {
                TopicConnectionFactory topicConnectionFactory;

                try {
                        Context jndi;

                        LogLog.debug("Getting initial context.");
                        if (initialContextFactoryName != null) {
                                Properties env = new Properties();
                                env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactoryName);

                                if (providerURL != null) {
                                        env.put(Context.PROVIDER_URL, providerURL);
                                }
                                else {
                                        LogLog.warn("You have set InitialContextFactoryName option but not the " + "ProviderURL. This is likely to cause problems.");
                                }

                                if (urlPkgPrefixes != null) {
                                        env.put(Context.URL_PKG_PREFIXES, urlPkgPrefixes);
                                }

                                if (securityPrincipalName != null) {
                                        env.put(Context.SECURITY_PRINCIPAL, securityPrincipalName);

                                        if (securityCredentials != null) {
                                                env.put(Context.SECURITY_CREDENTIALS, securityCredentials);
                                        }
                                        else {
                                                LogLog.warn("You have set SecurityPrincipalName option but not the " + "SecurityCredentials. This is likely to cause problems.");
                                        }
                                }

                                if (jmsTopic != null) {
                                        env.put("topic." + jmsTopic, jmsTopic);
                                }
                                else {
                                        LogLog.warn("You have not set jmsTopic option. This is likely to cause problems.");
                                }

                                jndi = new InitialContext(env);
                        }
                        else {
                                jndi = new InitialContext();
                        }

                        LogLog.debug("Looking up [" + tcfBindingName + "]");
                        topicConnectionFactory = (TopicConnectionFactory) lookup(jndi, tcfBindingName);
                        LogLog.debug("About to create TopicConnection.");

                        if (userName != null) {
                                topicConnection = topicConnectionFactory.createTopicConnection(userName, password);
                        }
                        else {
                                topicConnection = topicConnectionFactory.createTopicConnection();
                        }

                        LogLog.debug("Creating TopicSession, non-transactional, " + "in AUTO_ACKNOWLEDGE mode.");
                        topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

                        LogLog.debug("Looking up topic name [" + topicBindingName + "].");
                        Topic topic = (Topic) lookup(jndi, topicBindingName);

                        LogLog.debug("Creating TopicPublisher.");
                        topicPublisher = topicSession.createPublisher(topic);

                        LogLog.debug("Starting TopicConnection.");
                        topicConnection.start();

                        jndi.close();

                        connected = true;
                }
                catch (JMSException e) {
                        errorHandler.error("Error while activating options for appender named [" + name + "].", e, ErrorCode.GENERIC_FAILURE);
                        connected = false;
                }
                catch (NamingException e) {
                        errorHandler.error("Error while activating options for appender named [" + name + "].", e, ErrorCode.GENERIC_FAILURE);
                        connected = false;
                }
                catch (RuntimeException e) {
                        errorHandler.error("Error while activating options for appender named [" + name + "].", e, ErrorCode.GENERIC_FAILURE);
                        connected = false;
                }
        }

        protected Object lookup(Context ctx, String name) throws NamingException {
                try {
                        return ctx.lookup(name);
                }
                catch (NameNotFoundException e) {
                        LogLog.error("Could not find name [" + name + "].");
                        throw e;
                }
        }

        protected boolean checkEntryConditions() {
                String fail = null;

                if (this.topicConnection == null) {
                        fail = "No TopicConnection";
                }
                else if (this.topicSession == null) {
                        fail = "No TopicSession";
                }
                else if (this.topicPublisher == null) {
                        fail = "No TopicPublisher";
                }

                if (fail != null) {
                        errorHandler.error(fail + " for JMSAppender named [" + name + "].");
                        return false;
                }
                else {
                        return true;
                }
        }

        /**
         * Close this JMSAppender. Closing releases all resources used by the
         * appender. A closed appender cannot be re-opened.
         */
        public synchronized void close() {
                // The synchronized modifier avoids concurrent append and close operations

                if (this.closed) {
                        return;
                }

                closeJms();
                thread.interrupt();

                LogLog.debug("Closing appender [" + name + "].");
                this.closed = true;
        }

        private final void closeJms() {
                try {
                        if (topicPublisher != null) {
                                topicPublisher.close();
                        }

                        if (topicSession != null) {
                                topicSession.close();
                        }

                        if (topicConnection != null) {
                                topicConnection.close();
                        }
                }
                catch (JMSException e) {
                        LogLog.error("Error while closing JMSAppender [" + name + "].", e);
                }
                catch (RuntimeException e) {
                        LogLog.error("Error while closing JMSAppender [" + name + "].", e);
                }
                finally {
                        // Help garbage collection
                        topicPublisher = null;
                        topicSession = null;
                        topicConnection = null;
                }
        }

        /**
         * This method called by {@link AppenderSkeleton#doAppend} method to do
         * most of the real appending work.
         */
        @Override
        public void append(LoggingEvent event) {
                loggingEventQueue.add(event);
        }

        private static void processQueue() {
                while (true) {
                        try {
                                LoggingEvent event = loggingEventQueue.poll(1L, TimeUnit.SECONDS);
                                if (event != null) {
                                        instance.processEvent(event);
                                }
                        }
                        catch (InterruptedException e) {
                                // No operations.
                        }
                }
        }

        private final void processEvent(LoggingEvent loggingEvent) {
                if (loggingEvent != null) {
                        Object obj = loggingEvent.getMessage();
                        if (obj instanceof String) {
                                // It can be an instance of anything depending on what is being logged
                                String msg = (String) obj;
                                if (connected) {
                                        sendObject(msg);
                                }
                                else {
                                        saveToDisk(msg);
                                        if (checkReconnect()) {
                                                activateOptions();

                                                if (connected) {
                                                        sendSavedObjects();
                                                        disconnectTime = 0;
                                                }
                                                else {
                                                        disconnectTime = System.currentTimeMillis();
                                                }
                                        }
                                }
                        }
                }
        }

        private final void sendObject(String obj) {
                try {
                        ObjectMessage msg = topicSession.createObjectMessage();
                        msg.setObject(obj);
                        topicPublisher.publish(msg);
                }
                catch (JMSException e) {
                        saveToDisk(obj);
                        disconnectTime = System.currentTimeMillis();
                        connected = false;
                        closeJms();
                }
                catch (RuntimeException e) {
                        errorHandler.error("Could not publish message in JMSAppender [" + name + "].", e, ErrorCode.GENERIC_FAILURE);
                }
                catch (Exception e) {
                        errorHandler.error("Could not publish message in JMSAppender [" + name + "].", e, ErrorCode.GENERIC_FAILURE);
                }
                finally {
                        obj = null;
                }
        }

        private final void saveToDisk(String obj) {
                BufferedOutputStream bout = null;
                byte[] bytes = null;
                String fileName = null;

                try {
                        fileCounter++;
                        fileName = OBJECT_STORE_PATH + File.separator + "OBJ_" + fileCounter;

                        bout = new BufferedOutputStream(new FileOutputStream(fileName));
                        bytes = Serializer.serializeObject(obj);
                        bout.write(bytes);
                        bout.flush();
                        bout.close();
                }
                catch (IOException ioe) {
                        errorHandler.error("Could not serialize file [" + fileName + "] to bytes.", ioe, ErrorCode.WRITE_FAILURE);
                }
                finally {
                        bout = null;
                        bytes = null;
                        fileName = null;
                        obj = null;
                }
        }

        private final void sendSavedObjects() {
                BufferedInputStream bis = null;
                File objDir = new File(OBJECT_STORE_PATH);
                File[] files = objDir.listFiles();
                int numSent = 0;

                for (int i = 0; i < files.length; i++) {
                        File fName = files[i];
                        if (fName.getName().matches("OBJ_\\d+")) {
                                try {
                                        bis = new BufferedInputStream(new FileInputStream(fName));
                                        byte[] bytes = new byte[(int) fName.length()];
                                        int b = bis.read(bytes);

                                        bis.close();
                                        if (!fName.delete()) {
                                                fName.deleteOnExit();
                                        }

                                        if (b == bytes.length) {
                                                String obj = (String) Serializer.deserializeBytes(bytes);
                                                sendObject(obj);
                                                numSent++;
                                        }
                                        else {
                                                errorHandler.error("Could not read file [" + fName + "] fully, " + b + " bytes out of " + bytes.length + ".");
                                        }
                                }
                                catch (FileNotFoundException fnfe) {
                                        errorHandler.error("Could not find file [" + fName + "]", fnfe, ErrorCode.FILE_OPEN_FAILURE);
                                }
                                catch (IOException ioe) {
                                        errorHandler.error("Could not read file [" + fName + "]", ioe, ErrorCode.FILE_OPEN_FAILURE);
                                }
                                catch (ClassNotFoundException cnfe) {
                                        errorHandler.error("Could not deserialize file [" + fName + "]", cnfe, ErrorCode.GENERIC_FAILURE);
                                }
                        }
                }

                if (numSent < files.length) {
                        errorHandler.error("Not all saved data objects were sent, only " + numSent + " out of " + files.length);
                }

                if (numSent == files.length) {
                        // Reset to zero, there should be no more files in the
                        // dir as they should have been deleted. This will not be
                        // reset if some files could not be sent. So more objects that
                        // get saved will have a number starting from the last point
                        // so that there will be no conflicts.
                        fileCounter = 0;
                }

                bis = null;
                files = null;
        }

        private final boolean checkReconnect() {
                long now = System.currentTimeMillis();
                long diff = now - disconnectTime;
                if (diff >= reconnectDelay) {
                        return true;
                }
                else {
                        return false;
                }
        }

        /**
         * Returns the value of the <b>InitialContextFactoryName</b> option. See
         * {@link #setInitialContextFactoryName} for more details on the meaning
         * of this option.
         * */
        public String getInitialContextFactoryName() {
                return initialContextFactoryName;
        }

        /**
         * Setting the <b>InitialContextFactoryName</b> method will cause this
         * <code>JMSAppender</code> instance to use the
         * {@link InitialContext#InitialContext(Hashtable)} method instead of
         * the no-argument constructor. If you set this option, you should also
         * at least set the <b>ProviderURL</b> option.
         * 
         * <p>
         * See also {@link #setProviderURL(String)}.
         * */
        public void setInitialContextFactoryName(String initialContextFactoryName) {
                this.initialContextFactoryName = initialContextFactoryName;
        }

        public String getProviderURL() {
                return providerURL;
        }

        public void setProviderURL(String providerURL) {
                this.providerURL = providerURL;
        }

        String getURLPkgPrefixes() {
                return urlPkgPrefixes;
        }

        public void setURLPkgPrefixes(String urlPkgPrefixes) {
                this.urlPkgPrefixes = urlPkgPrefixes;
        }

        public String getSecurityCredentials() {
                return securityCredentials;
        }

        public void setSecurityCredentials(String securityCredentials) {
                this.securityCredentials = securityCredentials;
        }

        public String getSecurityPrincipalName() {
                return securityPrincipalName;
        }

        public void setSecurityPrincipalName(String securityPrincipalName) {
                this.securityPrincipalName = securityPrincipalName;
        }

        public void setJmsTopic(String jmsTopic) {
                this.jmsTopic = jmsTopic;
        }

        public String getJmsTopic() {
                return jmsTopic;
        }

        public String getUserName() {
                return userName;
        }

        /**
         * The user name to use when
         * {@link TopicConnectionFactory#createTopicConnection(String, String)
         * creating a topic session}. If you set this option, you should also
         * set the <b>Password</b> option. See {@link #setPassword(String)}.
         * */
        public void setUserName(String userName) {
                this.userName = userName;
        }

        public String getPassword() {
                return password;
        }

        /**
         * The password to use when creating a topic session.
         */
        public void setPassword(String password) {
                this.password = password;
        }

        /**
         * If true, the information sent to the remote subscriber will include
         * caller's location information. By default no location information is
         * sent to the subscriber.
         */
        public void setLocationInfo(boolean locationInfo) {
                this.locationInfo = locationInfo;
        }

        /**
         * Returns the TopicConnection used for this appender. Only valid after
         * activateOptions() method has been invoked.
         */
        protected TopicConnection getTopicConnection() {
                return topicConnection;
        }

        /**
         * Returns the TopicSession used for this appender. Only valid after
         * activateOptions() method has been invoked.
         */
        protected TopicSession getTopicSession() {
                return topicSession;
        }

        /**
         * Returns the TopicPublisher used for this appender. Only valid after
         * activateOptions() method has been invoked.
         */
        protected TopicPublisher getTopicPublisher() {
                return topicPublisher;
        }

        /**
         * The JMSAppender sends serialized events and consequently does not
         * require a layout.
         */
        public boolean requiresLayout() {
                return false;
        }

        @Override
        public void finalize() {
                close();
                super.finalize();
        }
}
