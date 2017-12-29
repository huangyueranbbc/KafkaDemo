package com.hyr.kafka.demo.offset.atomic;

import com.hyr.kafka.demo.utils.RedisUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

/*******************************************************************************
 * @date 2017-12-28 上午 9:40
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: kafka 保证消息处理和offset提交的原子性。 解决数据重复消费或数据丢失的问题。
 * Ensure the atomicity of message processing and offset submission. Solve the problem of data duplication or data loss.
 ******************************************************************************/
public class KafkaOffsetAtomic {

    /*
    避免重复消费和数据丢失必须保证消息处理和offset提交的原子性。
    官方建议:
    1.如果是关系型数据库,使用事务保证其原子性。
    2.如果是搜索引擎,将offset和索引存放在一起。

    针对于分布式集群环境下,使用redis存储offset是最佳选择。保存后,每次rebalance或Kafka重启,通过seek去redis中读取指定topic中partition的offset

    Offset保存格式:K-V
    使用partion+topic作为Key,offset作为Value


    Avoiding repeated consumption and data loss must guarantee the atomicity of message processing and offset submission.
    The official proposal:
    1. if it is a relational database, use transactions to ensure its atomicity.
    2. if it is a search engine, store the offset and the index together.

    In a distributed cluster environment, using redis to store offset is the best choice.
    After saving, every time rebalance or Kafka is restarted, the offset of partition in the specified topic is read through seek to redis

    Offset save format: K-V
    Use partion+topic as Key, offset as Value
     */

    // 使用redis存储offset
    static Jedis redis = RedisUtil.getJedis();
    //static ConcurrentHashMap<TopicPartition, Long> consumed; // 每个partition已消费的标记 通常会保存到其他的文件系统中,避免随kafka程序销毁而同时销毁。

    public static String topic = "testoffsetp5";

    public static void main(String[] args) throws IOException {
        runConsumer();
    }

    public static void runConsumer() {
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

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {

            // 保存偏移量 保存每一个partition已经提交消费的offset。 Save the offset to save offset for each partition that has already been submitted to the consumption.
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println(" onPartitionsRevoked partitions.size:" + partitions.size());
                for (TopicPartition partition : partitions) {
                    if (consumer != null) {
                        OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
                        // 如果当前partition是首次消费,当前partition的offset设为0。  If the current partition is the first consumption, the current partition's offset is set to 0。
                        if (null == offsetAndMetadata || offsetAndMetadata.offset() < 0) {
                            redis.set(partition + "_" + topic, String.valueOf(0));
                            continue;
                        }
                        System.out.println(getNowDate() + " now offset:" + offsetAndMetadata.offset() + " partition" + partition);
                        redis.set(partition + "_" + topic, String.valueOf(offsetAndMetadata.offset()));
                    }
                }
            }

            // 提取偏移量 Extraction of offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println(" onPartitionsAssigned partitions.size:" + partitions.size());
                for (TopicPartition partition : partitions) {
                    long lastOffset = Long.parseLong(redis.get(partition + "_" + topic)); // 该partition当前需要消费(还没有消费)的offset。  The partition currently needs to consume (not yet consumed) offset
                    System.out.println(getNowDate() + " lastOffset:" + lastOffset + "\t partition:" + partition);
                    if (consumer != null) {
                        consumer.seek(partition, lastOffset); // 指定当前partition消费的位置。  Specify the location of the current partition consumption.
                    }

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
            // TODO 如果还是担心会消费到错误offset的数据,可以在每次poll之前,再对每个partition进行一次seek。但这会影响性能。 If you still worry about consuming the wrong offset data, you can make a seek for each partition before each poll. But this will affect performance
            //            Set<TopicPartition> topicPartitions = consumer.assignment();
            //            for (TopicPartition partition : topicPartitions) {
            //                // redis取出offset getOffsetFromDB
            //                long offset = Long.parseLong(redis.get(partition + "_" + topic));
            //                consumer.seek(partition, offset);
            //                System.out.println("before poll seek offser:" + offset + " partition:" + partition + " topic:" + topic);
            //            }

            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records != null && !records.isEmpty()) {

                // 迭代每一个partition
                for (TopicPartition partition : records.partitions()) {

                    // 每一个partition的数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
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
                        // poll 消费每一条数据后,自动提交offset到当前的partition。  After each data is consumed, offset is automatically submitted to the current partition.
                        long offset = record.offset(); // 当前已经消费过的offset。  Offset, which is currently consumed。
                        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = Collections.singletonMap(
                                partition, new OffsetAndMetadata(offset + 1)); // 由于手动提交,offset需要+1,指向下一个还没有消费的offset。 Due to manual submission, offset needs +1, pointing to the next offset that has not been consumed yet.

                        // 保证消息处理和offset提交的原子性。解决数据丢失或数据重复消费。 Ensure the atomicity of message processing and offset submission. Solve data loss or data repeated consumption.
                        // Buffer buffer=new Buffer(); // 使用buffer缓冲
                        insertAtomicDB(record.partition() + "00000000" + record.offset(), record.value(), redis, consumer, offsetAndMetadataMap, record, partition);
                    }

                }
                // 使用完poll从本地缓存拉取到数据之后,需要client调用commitSync方法（或者commitAsync方法）去commit 下一次该去读取 哪一个offset的message。
                // consumer.commitSync();
            }

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

    /**
     * 入库,消息处理。 Warehousing, message processing.
     * 同时提交offset,保证原子性。如果失败异常则一同回滚
     * At the same time, the offset is submitted to ensure the atomicity. If the failure is unsuccessful, it rolls back together
     */
    public static void insertAtomicDB(String v1, String v2, Jedis redis, KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap, ConsumerRecord<String, String> record, TopicPartition partition) {
        Statement statement = null;
        Connection connection = null;
        // 上一次消费的offset 用于回滚RollBack getOffsetFromDB。     The last consumption of offset was used to roll back
        long lastOffset;
        if (redis.exists(partition + "_" + topic)) {
            lastOffset = Long.parseLong(redis.get(partition + "_" + topic));
        } else {// 如果没有消费记录  If there is no record of consumption
            lastOffset = 0;
        }

        try {
            String URL = "jdbc:mysql://localhost:3306/kafkatest";
            String USER = "root";
            String PASSWORD = "666666";
            //1.加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
            //2.获得数据库链接
            connection = DriverManager.getConnection(URL, USER, PASSWORD);

            // 关闭自动提交  Close automatic submission
            connection.setAutoCommit(false);

            // 4.执行插入
            // 4.1 获取操作SQL语句的Statement对象：
            // 调用Connection的createStatement()方法来创建Statement的对象

            // 3.准备插入的SQL语句
            String sql = "INSERT INTO ttt(id,text) "
                    + "VALUES (" + v1 + ",'" + v2 + "')";
            System.out.println(sql);
            statement = connection.createStatement();

            // TODO 针对关系型数据库,使用事务保证消息处理和offset提交的原子性。 For relational databases, transactional message processing and offset submitted atomicity

            // 4.2 调用Statement对象的executeUpdate(sql) 执行SQL 语句的插入
            statement.executeUpdate(sql);

            // 手动提交offset。  Manual submission of offset
            redis.set(partition + "_" + topic, String.valueOf(record.offset() + 1));
            // 系统自身的提交offset
            //consumer.commitSync(offsetAndMetadataMap);

            // 提交
            connection.commit();

        } catch (SQLException e) {
            // RollBack
            try {
                if (null != connection && null != redis) {
                    connection.rollback();
                    redis.set(partition + "_" + topic, String.valueOf(lastOffset));
                } else {
                    System.err.println("connection:" + connection + " redis:" + redis);
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            if (e.getErrorCode() == 1062) {
                System.out.println("主键重复 重复消费");
                System.exit(-1);
            }
            e.printStackTrace();
        } catch (Exception e) {
            // RollBack
            try {
                if (null != connection && null != redis) {
                    connection.rollback();
                    redis.set(partition + "_" + topic, String.valueOf(lastOffset));
                } else {
                    System.err.println("connection:" + connection + " redis:" + redis);
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            e.printStackTrace();
        } finally {
            // 5.关闭Statement对象
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                // 2.关闭连接
                try {
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void initRedis() {

    }

}
