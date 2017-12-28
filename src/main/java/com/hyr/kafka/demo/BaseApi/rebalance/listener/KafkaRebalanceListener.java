package com.hyr.kafka.demo.BaseApi.rebalance.listener;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/*******************************************************************************
 * @date 2017-12-28 上午 9:40
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: kafka rebalance监听 存储、消费指定位置的offset的数据
 ******************************************************************************/
public class KafkaRebalanceListener {

    static ConcurrentHashMap<TopicPartition, Long> consumed; // 每个partition已消费的标记 通常会保存到其他的文件系统中,避免随kafka程序销毁而同时销毁。
    public static String topic = "testoffset";

    public static void main(String[] args) throws IOException {
        runConsumer();
    }

    public static void runConsumer() {
        consumed = new ConcurrentHashMap<TopicPartition, Long>();

        String group = "0,1,2";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", group);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "6000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);

        ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {

            // 保存偏移量 如果成功，进行offset提交,保存offset。
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println(" onPartitionsRevoked partitions.size:" + partitions.size());
                for (TopicPartition partition : partitions) {
                    OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
                    System.out.println(getNowDate() + " now offset:" + offsetAndMetadata.offset() + " partition" + partition);
                    consumed.put(partition, offsetAndMetadata.offset());
                }
            }

            // 提取偏移量
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println(" onPartitionsAssigned partitions.size:" + partitions.size());
                for (TopicPartition partition : partitions) {
                    long lastOffset = consumed.get(partition); // 该partition当前需要消费(还没有消费)的offset
                    System.out.println(getNowDate() + " lastOffset:" + lastOffset + "\t partition:" + partition);
                    consumer.seek(partition, lastOffset);

                }
            }
        };
        consumer.subscribe(Arrays.asList(topic), consumerRebalanceListener); // 添加

        System.out.println("Subscribed to topic " + topic);
        Long unixTime;
        Long totalLatency = 0L;
        Long count = 0L;
        Long minCreationTime = Long.MAX_VALUE;

        while (true) {
            ConsumerRecords<String, Long> records = consumer.poll(100);

            // 迭代每一个partition
            for (TopicPartition partition : records.partitions()) {

                // 每一个partition的数据
                List<ConsumerRecord<String, Long>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, Long> record : partitionRecords) {
                    // For benchmarking tests
                    Long ts = record.timestamp();
                    if (ts < minCreationTime) {
                        minCreationTime = ts;
                    }
                    //TimestampType tp = record.timestampType();
                    unixTime = System.currentTimeMillis();
                    Long latency = unixTime - ts;
                    totalLatency += latency;
                    count += 1;
                    System.out.println(getNowDate() + " partition:" + record.partition() + " region(key): " + record.key() + "  clicks(value): " + record.value() + "   outputTime: " + unixTime + " minCreationTime : " + minCreationTime + "  totallatency: " + totalLatency + "  count: " + count + " offset" + record.offset());
                    // poll 消费每一条数据后,自动提交offset到当前的partition。
                    long offset = record.offset(); // 当前已经消费过的offset
                    Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = Collections.singletonMap(
                            partition, new OffsetAndMetadata(offset + 1)); // 由于手动提交,offset需要+1,指向下一个还没有消费的offset。
                    // TODO 保存offset到自定义的存储系统中
                    consumed.put(partition, record.offset());

                    // 系统自身的提交offset
                    consumer.commitSync(offsetAndMetadataMap);

                }

            }

            // 使用完poll从本地缓存拉取到数据之后,需要client调用commitSync方法（或者commitAsync方法）去commit 下一次该去读取 哪一个offset的message。
            // TODO 系统自动提交,会自动加offset指向下一个还没有被消费的位置上。
            // consumer.commitSync();

        }
    }

    /**
     * 获取现在时间
     *
     * @return 返回时间类型 yyyy-MM-dd HH:mm:ss
     */
    public static String getNowDate() {
        Date currentTime = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = formatter.format(currentTime);
        return dateString;
    }

}
