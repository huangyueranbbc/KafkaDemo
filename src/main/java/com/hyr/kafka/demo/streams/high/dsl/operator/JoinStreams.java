package com.hyr.kafka.demo.streams.high.dsl.operator;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/*******************************************************************************
 * @date 2017-12-26 下午 2:59
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: Join 连接多个流
 ******************************************************************************/
public class JoinStreams {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream left = builder.stream("my-input-topic1");
        KStream right = builder.stream("my-input-topic2");

        KeyValueMapper<String, String, String> leftSelect = new KeyValueMapper<String, String, String>() {
            public String apply(String key, String line) {
                System.out.println(key + ":" + line);
                return line;
            }
        };

        KeyValueMapper<String, String, String> rightSelect = new KeyValueMapper<String, String, String>() {
            public String apply(String key, String line) {
                System.out.println(key + ":" + line);
                return line;
            }
        };

        KStream<String, String> all = left.selectKey(leftSelect)
                // join 连接多个stream join multiple stream
                .join(right.selectKey(rightSelect), new ValueJoiner<String, String, String>() {
                    public String apply(String value1, String value2) {
                        System.out.println(value1 + "--" + value2);
                        return value1 + "--" + value2;
                    }
                }, JoinWindows.of(TimeUnit.SECONDS.toMillis(2)));


        all.to("my-output-topic"); // 将join合并后的流发送到"my-output-topic"中

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // 发送数据
        com.hyr.kafka.demo.BaseApi.Producer producer=new com.hyr.kafka.demo.BaseApi.Producer();
        producer.producerTopic1="my-input-topic1";
        producer.producerTopic2="my-input-topic2";
        producer.runProducer();
    }

}
