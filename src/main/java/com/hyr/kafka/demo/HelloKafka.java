package com.hyr.kafka.demo;

import com.hyr.kafka.demo.BaseApi.Consumer;
import com.hyr.kafka.demo.BaseApi.Producer;

/*******************************************************************************
 * @date 2017-12-26 下午 4:26
 * @author: <a href=mailto:>黄跃然</a>
 * @Description: HelloWorld
 ******************************************************************************/
public class HelloKafka {

    public static void main(String[] args) throws InterruptedException {

        // 创建生产者 Create a producer
        Producer producer = new Producer();
        producer.PRODUCERTOPIC1 = "my-output-topic"; // 指定生产者的topic Topic of a specified producer
        producer.PRODUCERTOPIC2 = "my-output-topic";
        producer.KAFKA_ADDRESS="192.168.0.133:9092";

        // 创建消费者 Create consumers
        Consumer consumer = new Consumer();
        consumer.TOPIC = "my-output-topic"; // 指定消费者的topic Topic of the specified consumer
        consumer.KAFKA_ADDRESS="192.168.0.133:9092";
        consumer.ZOOKEEPER_ADDRESS="192.168.0.133:2181";

        // 启动消费者
        Thread con = new Thread(consumer);
        con.start();

        // 启动生产者
        Thread pro = new Thread(producer);
        pro.start();

    }

}
