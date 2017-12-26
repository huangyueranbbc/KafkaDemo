package com.hyr.kafka.demo;

import com.hyr.kafka.demo.BaseApi.Consumer;
import com.hyr.kafka.demo.BaseApi.Producer;

/*******************************************************************************
 * @date 2017-12-26 下午 4:26
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: HelloWorld
 ******************************************************************************/
public class HelloKafka {

    public static void main(String[] args) throws InterruptedException {

        // 创建生产者 Create a producer
        Producer producer = new Producer();
        producer.producerTopic1 = "my-output-topic"; // 指定生产者的topic Topic of a specified producer
        producer.producerTopic2 = "my-output-topic";

        // 创建消费者 Create consumers
        Consumer consumer = new Consumer();
        consumer.topic = "my-output-topic"; // 指定消费者的topic Topic of the specified consumer

        consumer.runConsumer(); // 启动消费者

        producer.runProducer(); // 启动生产者
    }

}
