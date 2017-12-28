package com.hyr.kafka.demo.streams.high.dsl.operator;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.*;

/*******************************************************************************
 * @date 2017-12-28 下午 5:19
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: FlatMap 扁平化
 ******************************************************************************/
public class FlatMapValuesStreams {

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

        // Takes one record and produces zero, one, or more records,
        // while retaining the key of the original record. You can modify the record values and the value type.
        ValueMapper<? super String, ? extends Iterable<String>> valueMapper = new ValueMapper<String, Iterable<String>>() {
            @Override
            public Iterable<String> apply(String s) {
                return Arrays.asList(s.split(" "));
            }
        };

        KStream<String, String> stream = kStream.flatMapValues(valueMapper);


        stream.to("my-output-topic");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();

    }

}
