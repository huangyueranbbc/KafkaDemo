package com.hyr.kafka.demo.streams.high.dsl.operator;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-28 下午 5:19
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: filter 拦截器
 ******************************************************************************/
public class FilterStreams {

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

        // Evaluates a boolean function for each element and retains those for which the function returns true.
        // 计算每个元素的布尔函数，并保留函数返回true的函数。
        Predicate<? super String, ? super String> predicate = new Predicate<String, String>() {
            @Override
            public boolean test(String key, String value) {
                System.out.println(value.equals("5") + ":" + value);
                if (value.equals("5")) {
                    return true;
                }
                return false;
            }
        };


        KStream<String, String> filterStream = kStream.filter(predicate); // kStream.filterNot(predicate);
        filterStream.to("my-output-topic");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();

    }

}
