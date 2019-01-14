package com.hyr.kafka.demo.streams.high.dsl.operator;

import com.hyr.kafka.demo.BaseApi.Producer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Map;

/*******************************************************************************
 * @date 2017-12-28 下午 6:10
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: ForeachAction 对每个记录执行无状态操作
 ******************************************************************************/
public class ForeachStreams {

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


        // Terminal operation. Performs a stateless action on each record.
        // 终端操作。对每个记录执行无状态操作。
        ForeachAction<? super String, ? super String> foreachAction = new ForeachAction<String, String>() {
            @Override
            public void apply(String key, String value) {
                System.out.println("key:" + key + " value:" + value);
            }
        };

        // 打印topic队列中的数据
        kStream.foreach(foreachAction);

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        //发送数据
        Producer producer = new Producer();
        producer.PRODUCERTOPIC1 = "my-input-topic";
        producer.PRODUCERTOPIC2 = "my-input-topic";
        producer.runProducer();
    }

}
