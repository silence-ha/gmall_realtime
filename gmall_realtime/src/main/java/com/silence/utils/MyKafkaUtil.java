package com.silence.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    public static String kafkaServer="hadoop202:9092,hadoop203:9092,hadoop204:9092";
    private static final String DEFAULT_TOPIC="DEFAULT_DATA";

    public static FlinkKafkaConsumer<String> getConsumer(String topic,String groupId){
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer(topic,new SimpleStringSchema(),prop);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(kafkaServer,topic,new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkwithSer(KafkaSerializationSchema<T> serializationSchema){
        Properties prop =new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        //如果 15 分钟没有更新状态，则超时 默认 1 分钟
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,1000*60*15+"");

        return new FlinkKafkaProducer(DEFAULT_TOPIC,serializationSchema,prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka'," +
                "'topic'    = '"+topic+"'," +
                "'properties.bootstrap.servers' = 'hadoop202:9092'," +
                "'format'    = 'json',"+
                " 'properties.group.id'='"+groupId+"',"+
                " 'scan.startup.mode' = 'latest-offset'";

        return ddl;
    }
}
