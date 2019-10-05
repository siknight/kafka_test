package com.kafka.Interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CountIntetceptor implements ProducerInterceptor {
    //错误数量
    private int errorCounter = 0;
    //正确数量
    private int successCounter = 0;
    /**
     * 该方法封装进KafkaProducer.send方法中，即它运行在用户主线程中。Producer确保在
     * 消息被序列化以及计算分区前调用该方法。用户可以在该方法中对消息做任何操作，但最好
     * 保证不要修改消息所属的topic和分区，否则会影响目标分区的计算
     * @param record
     * @return
     */
    public ProducerRecord<String,String> onSend(ProducerRecord record) {

        return record;
    }

    /**
     * 该方法会在消息被应答或消息发送失败时调用，并且通常都是在producer回调逻辑触发
     * 之前。onAcknowledgement运行在producer的IO线程中，因此不要在该方法中放入很重的
     * 逻辑，否则会拖慢producer的消息发送效率
     * @param recordMetadata
     * @param e
     */
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if (e==null){
            successCounter++;
        }else {
            errorCounter++;
        }
    }

    /**
     * 关闭interceptor，主要用于执行一些资源清理工作
     */
    public void close() {
        System.out.println("successful sent:"+successCounter);
        System.out.println("failed sent:"+errorCounter);
    }

    /**
     * 获取配置信息和初始化数据时调用。
     * @param map
     */
    public void configure(Map<String, ?> map) {

    }
}
