package george.log4j.jmsappender;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public final class Serializer {

        private Serializer() {

        }

        /**
         * De-serializes an array of bytes into a java object.
         * 
         * @param bytes
         * @return Object
         * @throws IOException
         * @throws ClassNotFoundException
         */
        public static final Object deserializeBytes(byte[] bytes) throws IOException, ClassNotFoundException {
                ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bytesIn);
                Object obj = ois.readObject();
                ois.close();
                return obj;
        }

        /**
         * Serializes a java object into an array of bytes.
         * 
         * @param obj
         * @return byte[]
         * @throws IOException
         */
        public static final byte[] serializeObject(Object obj) throws IOException {
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
                oos.writeObject(obj);
                oos.flush();
                byte[] bytes = bytesOut.toByteArray();
                bytesOut.close();
                oos.close();
                return bytes;
        }
}
