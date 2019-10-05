package com.kafka.gaoji_old_consumeapi;


import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


import java.util.*;

public class OldConsume {
    //这个注解带不带无所谓，不知道什么用
    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put("zookeeper.connect", "hadoop102:2181");
        properties.put("group.id", "lisi");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");


        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        HashMap<String, Integer> topicCount  = new HashMap<String, Integer>();
        topicCount.put("first",1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap  = consumer.createMessageStreams(topicCount);
        KafkaStream<byte[], byte[]> stream  = consumerMap.get("first").get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()){
            System.out.println(new String(it.next().message()));
        }


    }
}
