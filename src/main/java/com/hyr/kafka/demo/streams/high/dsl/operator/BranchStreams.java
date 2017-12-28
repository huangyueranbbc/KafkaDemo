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
 * @date 2017-12-28 下午 5:28
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: Branch 将stream按照规则进行切分多个stream。
 ******************************************************************************/
public class BranchStreams {

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


        // Branch (or split) a KStream based on the supplied predicates into one or more KStream instances.
        // 根据规则将Stream进行切分
        Predicate<? super String, ? super String> predicate = new Predicate<String, String>() {
            @Override
            public boolean test(String key, String value) {
                switch (value) {
                    case "a":
                        return true;
                    case "b":
                        return true;
                    case "c":
                        return true;
                    case "d":
                        return true;
                    default:
                        return false;
                }
            }
        };

        KStream<String, String>[] kStreams = kStream.branch(predicate);

        // TODO 根据规则,切分stream,分发到不通的topic中。
        for (KStream stream : kStreams) {
            stream.to("my-output-topic"); // stream.to("a"); stream.to("b"); stream.to("c"); stream.to("d");
        }

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
    }

}
