package com.hyr.kafka.demo.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-25 19:51
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: 入门DEMO 了解KafkaStream的创建和执行 By 高级StreamDSL
 ******************************************************************************/
public class HelloKafkaStream {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 指定K-V 格式 Specify K-V format
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        Serde<String> keySerde = new Serdes.StringSerde(); // 用于制定Key的格式
        Serde<String> valueSerde = new Serdes.StringSerde(); // 用于制定Value的格式
        //KStream source = builder.stream(keySerde,valueSerde,"my-input-topic");
        KStream source = builder.stream("my-input-topic"); // 创建"my-input-topic"Topic的流

        // 分隔字符串 split word
        ValueMapper<String, Iterable<String>> word = new ValueMapper<String, Iterable<String>>() {
            public Iterable<String> apply(String s) {
                System.out.println("==========================");
                List<String> list = Arrays.asList(s.split(" "));
                for (String str : list) {
                    System.out.println(str);
                }
                return list;
            }
        };

        KStream wordStream = source.flatMapValues(word);

        wordStream.to("my-output-topic"); // 将"my-input-topic"处理后的数据发送到"my-output-topic"的流中

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

}
