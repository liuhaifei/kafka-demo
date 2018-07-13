package com.xinho.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author lhf
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @date 2018/7/1317:52
 */
public class KafkaProducerDemo  extends  Thread{
    //定义一个生产者
    private final KafkaProducer<Integer,String> produce;

    //topic
    private final String topic;
    private final boolean isAysnc;

    public KafkaProducerDemo(String topic,Boolean isAysnc){
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerDemo");
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerialize");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        produce=new KafkaProducer<Integer, String>(properties);
        this.topic=topic;
        this.isAysnc=isAysnc;
    }

    @Override
    public void run() {
        int num=0;
        while(num<50){
            String message="message_"+num;
            System.out.println("begin send message:"+message);
            if(isAysnc){//异步发送
                produce.send(new ProducerRecord<Integer, String>(topic, message), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(recordMetadata!=null){
                            System.out.println("async-offset:"+recordMetadata.offset()+
                                    "->partition"+recordMetadata.partition());
                        }
                    }
                });
            }else{//同步发送  future/callable
                try {
                    RecordMetadata recordMetadata=produce.
                            send(new ProducerRecord<Integer, String>(topic,message)).get();
                    System.out.println("sync-offset:"+recordMetadata.offset()+
                            "->partition"+recordMetadata.partition());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            }
            num++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new KafkaProducerDemo("test",true).start();
    }

}
