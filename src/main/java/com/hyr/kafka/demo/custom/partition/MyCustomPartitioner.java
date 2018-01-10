package com.hyr.kafka.demo.custom.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/*******************************************************************************
 * @date 2018-01-10 上午 15:15
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: 自定义分区器
 ******************************************************************************/
public class MyCustomPartitioner implements Partitioner {
    private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());
    private Random random = new Random();

    /**
     * A cheap way to deterministically convert a number to a positive value. When the input is
     * positive, the original value is returned. When the input number is negative, the returned
     * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
     * value.
     * <p>
     * Note: changing this method in the future will possibly cause partition selection not to be
     * compatible with the existing messages already placed on a partition.
     *
     * @param number a given number
     * @return a positive number.
     */
    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    // 重写partition方法
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println("run my custom partition!");
        int numPartitions = 2;
        int res = 1;
        if (value == null) {
            System.out.println("value is null");
            res = random.nextInt(numPartitions);
        } else {
            System.out.println("value is " + value + "\n hashcode is " + value.hashCode());
            res = Math.abs(value.hashCode()) % numPartitions;
        }
        System.out.println("data partitions is " + res);
        return res;
    }

    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}