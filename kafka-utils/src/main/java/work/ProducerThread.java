package work;

import commons.Config;
import commons.Utils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by sponge on 2017/3/25 0025.
 */
public class ProducerThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(ProducerThread.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final int numEvents;

    private int msgSize = 512;

    public ProducerThread(String topic, int numEvents) {
        this.topic = topic;
        this.numEvents = numEvents;

        Properties props = Config.getProducerProperties();

        producer = new KafkaProducer<String, String>(props);
    }

    public void run() {
        int messageNo = 1;
        log.info("Begin send message!");
        String messageStr = Utils.randString(msgSize);
        while (messageNo ++ <= this.numEvents) {

            long startTime = System.currentTimeMillis();

            producer.send(new ProducerRecord<String, String>(topic,
                    Integer.toString(messageNo),
                    messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            log.debug("Send message messageNo[" + messageNo + "] Message: " + messageStr);

//            try {
//                Thread.sleep(randomInt(500, 1000));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    private static class DemoCallBack implements Callback {

        private long startTime;
        private int key;
        private String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
         * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                log.debug(
                        "topic [" + metadata.topic() + "] " + "message(" + key + ", " + message + ") sent to partition("
                                + metadata.partition() + "), " +
                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
                if (key % 50000 == 0) {
                    log.info("Commit msg " + Integer.toString(key));
                }
            } else {
                exception.printStackTrace();
            }
        }
    }
}