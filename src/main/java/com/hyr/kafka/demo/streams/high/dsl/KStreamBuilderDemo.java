package com.hyr.kafka.demo.streams.high.dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-26 下午 2:46
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: KStreamBuilder 高级DSL接口
 ******************************************************************************/
public class KStreamBuilderDemo {

    public static void main(String[] args) {
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 制定K-V 格式
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // Serdes : Data Types and Serialization
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        // KStream是一个消息流抽象，其中每个数据记录代表在无界数据集里的自包含数据。
        // KStream is a message flow abstraction in which each data record represents the self contained data in an unbounded set of data.
        KStream<String, String> kStream = builder.stream("my-input-topic1");

        // 分区表
        // KTable是一个变更日志流的抽象，其中每个数据记录代表一个更新。更确切的说，数据记录中的value是相同记录key的最后一条的更新（如果key还不存在，则更新将被认为是创建）。
        // KTable is an abstraction of a change log stream, in which each data record represents an update.
        // To be more precise, the value in the data record is the last update of the same record key (if the key does not exist,
        // the update will be considered to be created).
        KTable<String, String> kTable = builder.table("my-input-topic2", "query_my_input_topic");

        // 全局表
        // GlobalKTable也是一个变更日志流的抽象。其中每个数据记录代表一个更新。但是，不同于KTable，它是完全的复制每个KafkaStreams实例。
        // GlobalKTable is also an abstraction of a change log stream. Each of the data records represents an update.
        // However, unlike KTable, it is completely replicating every KafkaStreams instance.
        GlobalKTable<String, String> globalKTable = builder.globalTable("my-input-topic3", "my_input_topic_global_store");

        ForeachAction<? super String, ? super String> foreachAction = new ForeachAction<String, String>() {
            public void apply(String key, String value) {
                System.out.println("key:" + key + "\t value:" + value);
            }
        };
        kStream.foreach(foreachAction);

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();
    }

}
