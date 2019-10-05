package com.kafka.oldproducerapi;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class Oldproducer {
    public static void main(String[] args) {
        Properties prop = new Properties();
        //设置生产者的broker地址
        prop.setProperty("metadata.broker.list","hadoop102:9092");
        //等待的所有副本的应答数目
        prop.setProperty("request.required.acks","1");
        //key序列化
        prop.setProperty("serializer.class","kafka.serializer.StringEncoder");
        //里面的参数是什么含义
        /**
         *第一个泛型指定用于分区的key的类型，第二个泛型指message的类型
         * topic只能为String类型  所以不用指定
         *
         */
        Producer<Integer, String> producer = new Producer<Integer, String>(new ProducerConfig(prop));
        /**
         *第一个泛型指定用于分区的key的类型，第二个泛型指message的类型
         * topic只能为String类型  所以不用指定
         *message里的泛型必须和producer里的泛型一致
         */
        KeyedMessage<Integer, String> message = new KeyedMessage<Integer, String>("first", "hello world");
        producer.send(message);
    }
}
