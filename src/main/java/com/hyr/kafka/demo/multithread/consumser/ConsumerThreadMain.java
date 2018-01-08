package com.hyr.kafka.demo.multithread.consumser;

/*******************************************************************************
 * @date 2018-01-08 上午 15:15
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: 多线程消费DEMO
 ******************************************************************************/
public class ConsumerThreadMain {

    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String groupId = "0";
        String topic = "testoffsetp5";
        int consumerNumber = 5;

        // 安全关闭Consumer
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                MultiThreadConsumer.instance.stop();
            }
        }));

        MultiThreadConsumer.instance.init(brokers, groupId, topic);
        MultiThreadConsumer.instance.start(consumerNumber);
    }
}