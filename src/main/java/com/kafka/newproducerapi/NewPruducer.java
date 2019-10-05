package com.kafka.newproducerapi;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class NewPruducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop102:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class","com.kafka.newproducerapi.CustomerPartitoner");

        /**
         *第一个泛型指定用于分区的key的类型，第二个泛型指message的类型
         * topic只能为String类型  所以不用指定
         *
         */
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        /**
         *第一个泛型指定用于分区的key的类型，第二个泛型指message的类型
         * topic只能为String类型  所以不用指定
         *ProducerRecord里的泛型必须和producer里的泛型一致
         */
        producer.send(new ProducerRecord<String, String>("first","hello keaijiang"));
        /**
         *第一个泛型指定用于分区的key的类型，第二个泛型指message的类型
         * topic只能为String类型  所以不用指定
         *和上一个Producer相比，多了一个用于分区的key
         * ProducerRecord里的泛型必须和producer里的泛型一致
         */
        producer.send(new ProducerRecord<String, String>("first","1","wode jiang"));

        /**
         * 带回调函数
         */
        producer.send(new ProducerRecord<String, String>("first", "1", "wode jiang2"), new
                Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (recordMetadata!=null){
                            System.out.println("分区:"+recordMetadata.partition()+",topic:"+recordMetadata.topic()+"offset:"+recordMetadata.offset());

                        }
                    }
                });
        producer.close();
    }
}
