package com.hyr.kafka.demo.streams.high.dsl.operator.Join;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-26 下午 2:59
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: KTable Join KTable
 ******************************************************************************/
public class KTableJoinKTable {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KTable<String, Long> leftTable = builder.table(Serdes.String(), Serdes.Long(), "user-clicks", "query_table_1");
        KTable<String, String> rightTable = builder.table(Serdes.String(), Serdes.String(), "user-location", "query_table_2");

        ValueJoiner<Long, String, String> valueJoiner = new ValueJoiner<Long, String, String>() {
            @Override
            public String apply(Long leftValue, String rightValue) {
                System.out.println("left=" + leftValue + ", right=" + rightValue);
                return "left=" + leftValue + ", right=" + rightValue;
            }
        };

        // KTable-KTable joins are always non-windowed joins. They are designed to be
        // consistent with their counterparts in relational databases.
        // The changelog streams of both KTables are materialized into local state stores to
        // represent the latest snapshot of their table duals. The join result is a new KTable
        // that represents the changelog stream of the join operation.
        KTable<String, String> allTable = leftTable.join(rightTable, valueJoiner);
        allTable.to("my-output-topic");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // 发送数据
        com.hyr.kafka.demo.BaseApi.Producer producer = new com.hyr.kafka.demo.BaseApi.Producer();
        producer.PRODUCERTOPIC1 = "user-clicks";
        producer.PRODUCERTOPIC2 = "user-location";
        producer.runProducer();
    }

}
