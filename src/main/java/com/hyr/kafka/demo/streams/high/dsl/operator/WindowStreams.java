package com.hyr.kafka.demo.streams.high.dsl.operator;

import com.hyr.kafka.demo.BaseApi.Producer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/*******************************************************************************
 * @date 2017-12-26 下午 6:32
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: 滑动窗口 window Join
 ******************************************************************************/
public class WindowStreams {

    // Bean
    static public class RegionClicks {
        public long clicks;
        public String region;
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka.streams.1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-windowedjoin");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KStreamBuilder builder = new KStreamBuilder();


        KStream<String, Long> userclicks = builder.stream(Serdes.String(), Serdes.Long(), "user-clicks");

        KStream<String, String> userregion = builder.stream(Serdes.String(), Serdes.String(), "user-location");


        //<V1,R> KStream<K,R> join(KStream<K,V1> otherStream, ValueJoiner<V,V1,R> joiner, JoinWindows windows, Serde<K> keySerde, Serde<V> thisValueSerde, Serde<V1> otherValueSerde)
        //Combine element values of this stream with another KStream's elements of the same key using windowed Inner Join.
        KStream<String, Long> regionclicks = userclicks.join(userregion, new ValueJoiner<Long, String, RegionClicks>() {
            public RegionClicks apply(Long clicks, String region) {
                RegionClicks rc = new RegionClicks();
                if (region != null) {
                    rc.region = region;
                } else rc.region = "UNKNOWN";
                rc.clicks = clicks;
                if (clicks % 25 == 0) {
                    System.out.println("\n \n ----- Performing windowed join ----------");
                }
                return rc;
            }
        }, JoinWindows.of(TimeUnit.SECONDS.toMillis(3)), Serdes.String(), Serdes.Long(), Serdes.String())


                //map(KeyValueMapper<K,V,KeyValue<K1,V1>> mapper)
                //R apply(K key, V value)  Map a record with the given key and value to a new value.
                .map(new KeyValueMapper<String, RegionClicks, KeyValue<String, Long>>() {
                    public KeyValue<String, Long> apply(String user, RegionClicks rclicks) {
                        return new KeyValue<String, Long>(rclicks.region, rclicks.clicks);
                    }
                });

        // 打印数据
        ForeachAction<? super String, ? super Long> foreachAction = new ForeachAction<String, Long>() {
            public void apply(String region, Long clicks) {
                System.out.println("region:" + region + "\t click:" + clicks);
            }
        };
        regionclicks.foreach(foreachAction);

        regionclicks.to(Serdes.String(), Serdes.Long(), "my-output-topic"); // 将数据发送到指定的topic

        System.out.println("Performed join.");


        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        //发送数据
        Producer producer = new Producer();
        producer.producerTopic1 = "user-clicks";
        producer.producerTopic2 = "user-location";
        producer.runProducer();
    }

}
