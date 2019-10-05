package com.kafka.Interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.Properties;

public class InterceptorProducer {
    public static void main(String[] args) {
        // 1 设置配置信息
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "hadoop102:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class","com.kafka.newproducerapi.CustomerPartitoner");
        //2构建拦截链
        ArrayList<String> interceptors = new ArrayList<String>();
        //拦截器执行顺序，在于下面add添加的顺序，先添加的先执行
        interceptors.add("com.kafka.Interceptor.CountIntetceptor");
        interceptors.add("com.kafka.Interceptor.TimeInterceptor");

        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,interceptors);

        // 3 发送消息
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


        // 4 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }
}
