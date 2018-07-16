package com.xinho.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.Properties;

/**
 * @author lhf
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @date 2018/7/1318:06
 */
public class KafkaConsumerDemo extends Thread {
    private final KafkaConsumer kafkaConsumer;


    public KafkaConsumerDemo(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "47.96.119.178:9092,47.96.119.178:9093,47.96.119.178:9094");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerDemo1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.gupaoedu.kafka.MyPartition");

        kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Collections.singleton(topic));

    }
        @Override
        public void run() {
            while(true){
                ConsumerRecords<Integer,String> consumerRecord=kafkaConsumer.poll(1000);
                for(ConsumerRecord record:consumerRecord){
                    System.out.println("message receive:"+record.value());
                    kafkaConsumer.commitAsync();
                }
            }
        }

        public static void main(String[] args) {
            new KafkaConsumerDemo("test").start();
            //第一步，找到当前的consumer group的offset维护在哪个分区中
//        System.out.println(("KafkaConsumerDemo2".hashCode())%50);

        }
}
