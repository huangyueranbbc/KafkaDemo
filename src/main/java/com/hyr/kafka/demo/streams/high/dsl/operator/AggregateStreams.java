package com.hyr.kafka.demo.streams.high.dsl.operator;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-26 下午 4:59
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: aggregate 聚合
 ******************************************************************************/
public class AggregateStreams {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream stream1 = builder.stream("my-input-topic1");


        Initializer initializer = new Initializer() {
            public Object apply() {
                return "";
            }
        };

        // 聚合算子
        Aggregator<String, String, String> aggregator = new Aggregator<String, String, String>() {
            //key - the key of the record
            //value - the value of the record
            //aggregate - the current aggregate value
            public String apply(String key, String value, String aggregate) {
                System.out.println(key + "#######" + value + "######" + aggregate);
                return value+","+aggregate;
            }
        };

        // 对相同key的数据进行聚合
        KTable table = stream1.groupByKey().aggregate(initializer, aggregator, Serdes.String(), "aggregated-demo");

        table.toStream().to("my-output-topic");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // 发送数据
        com.hyr.kafka.demo.BaseApi.Producer producer = new com.hyr.kafka.demo.BaseApi.Producer();
        producer.PRODUCERTOPIC1 = "my-input-topic1";
        producer.PRODUCERTOPIC2 = "my-input-topic2";
        producer.runProducer();
    }

}
