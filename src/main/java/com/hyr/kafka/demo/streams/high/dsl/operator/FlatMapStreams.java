package com.hyr.kafka.demo.streams.high.dsl.operator;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.*;

/*******************************************************************************
 * @date 2017-12-28 下午 5:19
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: FlatMap 扁平化
 ******************************************************************************/
public class FlatMapStreams {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> kStream = builder.stream("my-input-topic");

        // flatMap 将一行数据分割后，逐个输出。
        KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> keyValueMapper = new KeyValueMapper<String, String, Iterable<KeyValue<String, String>>>() {
            @Override
            public Iterable<KeyValue<String, String>> apply(String key, String line) {
                String[] split = line.split(" ");
                List<KeyValue<String, String>> list = new ArrayList<KeyValue<String, String>>();
                for (String s : split) {
                    KeyValue<String, String> keyValue = KeyValue.pair(s, s);
                    list.add(keyValue);
                }
                return list;
            }
        };

        KStream<String, String> stream = kStream.flatMap(keyValueMapper);

        stream.to("my-output-topic");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();

    }

}
