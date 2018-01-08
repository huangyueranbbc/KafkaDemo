package com.hyr.kafka.demo.multithread.consumser;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/*******************************************************************************
 * @date 2018-01-08 上午 15:15
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: 多线程消费实例
 ******************************************************************************/
public enum MultiThreadConsumer {

    //kafka Consumer实例
    instance;

    private KafkaConsumer<String, String> consumer;
    private String topic;
    // Threadpool of consumers
    private ExecutorService executor;

    private AtomicBoolean isShutdown;
    private CountDownLatch countDownLatch;


    public void init(String brokers, String groupId, String topic1) {
        isShutdown = new AtomicBoolean(false);
        countDownLatch = new CountDownLatch(1);

        Properties properties = buildKafkaProperty(brokers, groupId);
        this.consumer = new KafkaConsumer<>(properties);
        this.topic = topic1;

        ConsumerRebalanceListener consumerRebalanceListener = new ConsumerRebalanceListener() {

            // 保存偏移量 保存每一个partition已经提交消费的offset。 Save the offset to save offset for each partition that has already been submitted to the consumption.
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(); // 保存offset
            }

            // 提取偏移量 Extraction of offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println(" onPartitionsAssigned partitions.size:" + partitions.size());
                for (TopicPartition partition : partitions) {
                    OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
                    long lastOffset = offsetAndMetadata.offset();
                    if (consumer != null) {
                        consumer.seek(partition, lastOffset); // 指定当前partition消费的位置。  Specify the location of the current partition consumption.
                    }

                }
            }
        };
        this.consumer.subscribe(Arrays.asList(this.topic), consumerRebalanceListener); // 订阅主题
    }

    public void start(int threadNumber) {
        Set<TopicPartition> topicPartitions = consumer.assignment();
        for (TopicPartition partition : topicPartitions) {
            // redis取出offset getOffsetFromDB
            long offset = consumer.committed(partition).offset();
            consumer.seek(partition, offset);
            System.out.println("before poll seek offser:" + offset + " partition:" + partition + " topic:" + topic);
        }

        executor = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        System.out.println("Subscribed to topic " + topic);

        while (!isShutdown.get()) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Long unixTime;
                    Long totalLatency = 0L;
                    Long count = 0L;
                    Long minCreationTime = Long.MAX_VALUE;

                    ConsumerRecords<String, String> records = consumer.poll(100);

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

                                System.out.println(getNowDate() + " thread:" + Thread.currentThread().getName() + " partition:" + record.partition() + " region(key): " + record.key() + "  clicks(value): " + record.value() + "   outputTime: " + unixTime + " minCreationTime : " + minCreationTime + "  totallatency: " + totalLatency + "  count: " + count + " offset" + record.offset());
                                // poll 消费每一条数据后,自动提交offset到当前的partition。  After each data is consumed, offset is automatically submitted to the current partition.
                                long offset = record.offset(); // 当前已经消费过的offset。  Offset, which is currently consumed。
                                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = Collections.singletonMap(
                                        partition, new OffsetAndMetadata(offset + 1)); // 由于手动提交,offset需要+1,指向下一个还没有消费的offset。 Due to manual submission, offset needs +1, pointing to the next offset that has not been consumed yet.

                                insertAtomicDB(record.partition() + "00000000" + record.offset(), record.value(), consumer, offsetAndMetadataMap, record, partition);
                                // 系统自身的提交offset
                                consumer.commitSync();
                            }

                        }
                        // 使用完poll从本地缓存拉取到数据之后,需要client调用commitSync方法（或者commitAsync方法）去commit 下一次该去读取 哪一个offset的message。
                        // consumer.commitSync();
                    }
                }
            });

        }
    }

    private static Properties buildKafkaProperty(String brokers, String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return properties;
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

    public void insertAtomicDB(String v1, String v2, KafkaConsumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap, ConsumerRecord<String, String> record, TopicPartition partition) {
        Statement statement = null;
        Connection connection = null;
        // 上一次消费的offset 用于回滚RollBack getOffsetFromDB。     The last consumption of offset was used to roll back

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
            String str1 = " currentThread:" + Thread.currentThread() + " partition:" + partition + " start insert!!!:";
            System.out.println(str1 + "\n" + sql);
            statement = connection.createStatement();

            // TODO 针对关系型数据库,使用事务保证消息处理和offset提交的原子性。 For relational databases, transactional message processing and offset submitted atomicity

            // 4.2 调用Statement对象的executeUpdate(sql) 执行SQL 语句的插入
            statement.executeUpdate(sql);

            // 提交
            connection.commit();

        } catch (SQLException e) {
            if (e.getErrorCode() == 1062) {
                System.out.println("主键重复 重复消费:" + offsetAndMetadataMap.get(partition).offset());
                System.exit(-1);
            }
            e.printStackTrace();
        } catch (Exception e) {
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

    public void stop() {


    }
}