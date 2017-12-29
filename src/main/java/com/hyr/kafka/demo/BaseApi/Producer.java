package com.hyr.kafka.demo.BaseApi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/*******************************************************************************
 * @date 2017-12-25 19:51
 * @author: <a href=mailto:huangyr@bonree.com>黄跃然</a>
 * @Description: Producer 生产者
 ******************************************************************************/
public class Producer {

    public static String producerTopic1 = "testoffsetp5";
    public static String producerTopic2 = "testoffsetp5";

    public static void main(String[] args) throws IOException {
        runProducer();
    }

    public static void runProducer() {
        KafkaProducer<String, Long> producer1; //生产者1
        Properties props1 = new Properties();
        props1.put("bootstrap.servers", "localhost:9092");
        props1.put("client.id", "Producer.1");
        props1.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props1.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        producer1 = new KafkaProducer<String, Long>(props1);

        KafkaProducer<String, String> producer2; //生产者1
        Properties props2 = new Properties();
        props2.put("bootstrap.servers", "localhost:9092");
        props2.put("client.id", "DemoProducer2");
        props2.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props2.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer2 = new KafkaProducer<String, String>(props2);
        String[] states = {"California", "Alabama", "Arkansas", "Arizona", "Alaska", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"};


        try {

            for (int i = 0; i < 10000; i++) {
                Random rn = new Random();

                int rnd = rn.nextInt(500);
                String user = (String) ("user" + Integer.toString(i));

                long range = 100L;
                long clicks = (long) (rn.nextDouble() * range);

                ProducerRecord record = new ProducerRecord(producerTopic1, user, clicks); // key:user value:clicks
                producer1.send(record);

                int rand = rn.nextInt(50);
                String location = states[rand];


                ProducerRecord rec = new ProducerRecord(producerTopic2, user, location);  // key:user value:location
                producer2.send(rec);

                if (clicks % 7 == 0) {
                    System.out.println("\n ----- Writing records to topics ----------  \n");
                }

            }
        } finally {

            producer1.close();
            producer2.close();
        }
    }
}