package com.hyr.kafka.demo.streams.high.dsl.operator;

import com.hyr.kafka.demo.BaseApi.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-28 下午 5:19
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: GroupBy分组
 ******************************************************************************/
public class GroupByStreams {

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

        KeyValueMapper<? super String, ? super String, String> keyValueMapper = new KeyValueMapper<String, String, String>() {
            @Override
            public String apply(String key, String value) {
                return "[" + value + "," + key + "]";
            }
        };

        KGroupedStream<String, String> stream = kStream.groupBy(keyValueMapper);

        Reducer<String> reducer = new Reducer<String>() {
            @Override
            public String apply(String key, String value) {
                return value;
            }
        };

        KTable<String, String> table = stream.reduce(reducer);

        table.to("my-output-topic");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();

        // 发送数据
        Producer producer = new Producer();
        producer.PRODUCERTOPIC1 = "my-input-topic";
        producer.PRODUCERTOPIC2 = "my-input-topic";
        producer.runProducer();
    }

}
