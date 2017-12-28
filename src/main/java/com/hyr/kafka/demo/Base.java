package com.hyr.kafka.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-26 下午 3:00
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description:
 ******************************************************************************/
public class Base {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream wordStream = builder.stream("my-input-topic");

        wordStream.to("my-output-topic"); // 将"my-input-topic"处理后的数据发送到"my-output-topic"的流中

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
