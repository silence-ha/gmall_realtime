package com.silence.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.silence.app.func.DimSink;
import com.silence.app.func.TableProcessFunction;
import com.silence.utils.MyKafkaUtil;

import com.silence.utils.MySqlUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/base_db"));

        String groupId="ods_base_db_m_groupid";
        String topic="ods_base_db_m";
        FlinkKafkaConsumer<String> consumer = MyKafkaUtil.getConsumer(topic, groupId);
        DataStreamSource<String> kafkaDs = env.addSource(consumer);

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        });

        SingleOutputStreamOperator<JSONObject> filterDs = jsonDs.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                if (jsonObject.getString("table") != null
                        && jsonObject.getJSONObject("data") != null
                        && jsonObject.getString("data").length() > 3) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        OutputTag<JSONObject> hbaseOut=new OutputTag<JSONObject>("hbase_out"){};

        SingleOutputStreamOperator kafkaProDs = filterDs.process(new TableProcessFunction<JSONObject, JSONObject>(hbaseOut) );

        DataStream<JSONObject> hbaseDs = kafkaProDs.getSideOutput(hbaseOut);

        hbaseDs.addSink(new DimSink());
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkwithSer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                JSONObject dataJson = jsonObject.getJSONObject("data");
                return new ProducerRecord<>(jsonObject.getString("sink_table"), dataJson.toJSONString().getBytes());
            }
        });
        kafkaProDs.addSink(kafkaSink);
        env.execute();
    }


}
