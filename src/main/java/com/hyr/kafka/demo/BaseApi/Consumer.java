package com.hyr.kafka.demo.BaseApi;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;


/*******************************************************************************
 * @date 2017-12-25 19:51
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: Consumer 消费者
 ******************************************************************************/
public class Consumer {

    public static String topic = "my-output-topic";

    public static void main(String[] args) throws IOException {
        runConsumer();
    }

    public static void runConsumer() {
        String group = "0";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        Long unixTime;
        Long totalLatency = 0L;
        Long count = 0L;
        Long minCreationTime = Long.MAX_VALUE;

        while (true) {
            ConsumerRecords<String, Long> records = consumer.poll(100);
            for (ConsumerRecord<String, Long> record : records) {
                // For benchmarking tests
                Long ts = record.timestamp();
                if (ts < minCreationTime) {
                    minCreationTime = ts;
                }
                //TimestampType tp = record.timestampType();
                unixTime = System.currentTimeMillis();
                Long latency = unixTime - ts;
                totalLatency += latency;
                count += 1;
                System.out.println("region(key): " + record.key() + "  clicks(value): " + record.value() + "   outputTime: " + unixTime + " minCreationTime : " + minCreationTime + "  totallatency: " + totalLatency + "  count: " + count);
            }

        }
    }
}