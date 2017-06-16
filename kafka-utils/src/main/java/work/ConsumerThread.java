package work;


import commons.Config;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by sponge on 2017/3/24 0024.
 */
public class ConsumerThread extends ShutdownableThread
{
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    public ConsumerThread(String topic, String threadName)
    {
        super(threadName, false);
        Properties props = Config.getConsumerProperties();

        consumer = new KafkaConsumer<String, String>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Received message : (" + record.key() + ", " + record.value() + ") at offset "
                    + record.offset() + " from thread :" + this.getName());
        }
    }


    @Override
    public boolean isInterruptible() {
        return false;
    }
}