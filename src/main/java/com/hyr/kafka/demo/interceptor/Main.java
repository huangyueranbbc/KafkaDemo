package com.hyr.kafka.demo.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/*******************************************************************************
 * @date 2019-02-20 上午 11:19
 * @author: <a href=mailto:>黄跃然</a>
 * @Description:
 ******************************************************************************/
public class Main {
    private static String KAFKA_ADDRESS="192.168.0.133:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_ADDRESS);
        props.put("client.id", "Producer.1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 拦截器
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.hyr.kafka.demo.interceptor.CustomInterceptor"); // interceptor 1
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        String topic = "test-topic";
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            RecordMetadata recordMetadata = producer.send(record).get();
        }

        // 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }

}
