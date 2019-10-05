package com.kafka.kafkastream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class kafkaStreamApplication {
    public static void main(String[] args) {

        // 定义输入的topic
        String from = "first";
        // 定义输出的topic
        String to = "sec";

        // 1.设置参数

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        StreamsConfig config = new StreamsConfig(settings);
        //2. 构建拓扑
        TopologyBuilder builder=new TopologyBuilder();
        builder.addSource("SOURCE",from).addProcessor("PROCESS", new ProcessorSupplier<byte[], byte[]>() {

            public Processor<byte[], byte[]> get() {
                //具体分析业务,自定义方法
                return new LogProcessor();
            }
        },"SOURCE").addSink("SINK",to,"PROCESS");

        //1.创建KafkaStreams
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

    }
}
