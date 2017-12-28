package com.hyr.kafka.demo.streams.high.dsl.operator.Join;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-26 下午 2:59
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: KStream Join GlobalKTable
 ******************************************************************************/
public class KStreamJoinGlobalKTable {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, Long> leftStream = builder.stream(Serdes.String(), Serdes.Long(), "user-clicks");
        GlobalKTable<String, String> rightRlobalKTable = builder.globalTable("user-location", "user-location");

        // 根据返回的Key,取出GlobalKTable中Key相同的记录进行join。
        KeyValueMapper<? super String, ? super Long, ? extends String> keyValueMapper = new KeyValueMapper<String, Long, String>() {
            public String apply(String key, Long value) {
                System.out.println(key + "==" + value);
                return key; //return "user995";
            }
        };

        ValueJoiner<? super Long, ? super String, ? extends String> valueJoiner = new ValueJoiner<Long, String, String>() {
            public String apply(Long leftValue, String rightValue) {
                System.out.println("left stream =" + leftValue + ", right globalKTable =" + rightValue);
                return "left=" + leftValue + ", right=" + rightValue;
            }
        };

        // KStream-GlobalKTable joins are always non-windowed joins. They allow you to perform table lookups against a GlobalKTable (entire changelog stream) upon receiving a new record from the KStream (record stream). An example use case would be “star queries” or “star joins”, where you would enrich a stream of user activities (KStream) with the latest user profile information (GlobalKTable) and further context information (further GlobalKTables).

        // At a high-level, KStream-GlobalKTable joins are very similar to KStream-KTable joins. However, global tables provide you with much more flexibility at the some expense when compared to partitioned tables:
        // They do not require data co-partitioning.
        // They allow for efficient “star joins”; i.e., joining a large-scale “facts” stream against “dimension” tables
        // They allow for joining against foreign keys; i.e., you can lookup data in the table not just by the keys of records in the stream, but also by data in the record values.
        // They make many use cases feasible where you must work on heavily skewed data and thus suffer from hot partitions.
        // They are often more efficient than their partitioned KTable counterpart when you need to perform multiple joins in succession.
        KStream<String, String> allStream = leftStream.join(rightRlobalKTable, keyValueMapper, valueJoiner);

        allStream.to("my-output-topic");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // 发送数据
        com.hyr.kafka.demo.BaseApi.Producer producer = new com.hyr.kafka.demo.BaseApi.Producer();
        producer.producerTopic1 = "user-clicks";
        producer.producerTopic2 = "user-location";
        producer.runProducer();
    }

}
