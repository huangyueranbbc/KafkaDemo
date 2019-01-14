package com.hyr.kafka.demo.streams.high.dsl.operator;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-29 下午 4:22
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: SelectKey 为每个记录分配一个新的KEY，可能是一个新的KEY类型。
 ******************************************************************************/
public class SelectKeyStreams {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> kStream = builder.stream("testoffsetp5");

        // 返回值,就是新分配的key
        KeyValueMapper<? super String, ? super String, ?> keyValueMapper = new KeyValueMapper<String, String, Object>() {
            @Override
            public Object apply(String key, String value) {
                System.out.println("key:" + key + " value:" + value);
                return "time:" + System.currentTimeMillis() + "_" + value;
            }
        };
        KStream<Object, String> selectKey = kStream.selectKey(keyValueMapper);
        selectKey.print();

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
    }

}
