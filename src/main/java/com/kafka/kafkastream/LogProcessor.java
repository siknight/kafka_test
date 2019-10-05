package com.kafka.kafkastream;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext context) {

        this.context=context;
    }
    @Override
    public void process(byte[] key, byte[] value) {
        String input = new String(value);
        // 如果包含“>>>”则只保留该标记后面的内容
        System.out.println("input="+input.toString());
        if (input.contains(">>>")){
            input=input.split(">>>")[1].trim();
            //输出到下一个topic,这里的logProcessor名字是不是无所谓
            context.forward("logProcessor".getBytes(), input.getBytes());

        }else{
            //输出到下一个topic

            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }
    @Override
    public void punctuate(long timestamp) {

    }
    @Override
    public void close() {

    }
}
