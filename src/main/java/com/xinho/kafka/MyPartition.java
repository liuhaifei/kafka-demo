package com.xinho.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author lhf
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @date 2018/7/1616:23
 */
public class MyPartition implements Partitioner {

    private Random random=new Random();
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionInfos=cluster.partitionsForTopic(topic);
        int partitionNum=0;
        if(key!=null){
            partitionNum= random.nextInt(partitionInfos.size());//随机分区
        }else {
            partitionNum=Math.abs((key.hashCode())%partitionInfos.size());
        }
        System.out.println("key ->"+key+"->value->"+value+"->"+partitionNum);
        return partitionNum;//指定发送的分区值
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
