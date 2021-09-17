package com.silence.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/unique_visit"));

        String groupId="dwd_page_log_groupid";
        String topic="dwd_page_log";
        FlinkKafkaConsumer<String> consumer = MyKafkaUtil.getConsumer(topic, groupId);
        DataStreamSource<String> kafkaDs = env.addSource(consumer);

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                return JSON.parseObject(s);
            }
        });

        KeyedStream<JSONObject, String> keyedDs = jsonDs.keyBy(t -> t.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filterDs = keyedDs.filter(new RichFilterFunction<JSONObject>() {
            SimpleDateFormat sdf = null;
            ValueState<String> firstState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> first_enter = new ValueStateDescriptor<String>("first_enter", String.class);
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();

                first_enter.enableTimeToLive(ttlConfig);
                firstState = getRuntimeContext().getState(first_enter);
                sdf = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                if (jsonObject.getJSONObject("page").getString("last_page_id") == null || jsonObject.getJSONObject("page").getString("last_page_id").length() == 0) {
                    String lastDate = firstState.value();
                    String logDate = sdf.format(new Date(jsonObject.getLong("ts")));
                    if (lastDate != null && lastDate.length() > 0 && lastDate.equals(logDate)) {
                        System.out.println("已访问：lastVisit:" + lastDate + "|| logDate：" + logDate);
                        return false;
                    } else {
                        System.out.println("未访问：lastVisit:" + lastDate + "|| logDate：" + logDate);
                        firstState.update(logDate);
                        return true;
                    }
                } else {
                    return false;
                }

            }
        });
        SingleOutputStreamOperator<String> stringMap = filterDs.map(t -> JSON.toJSONString(t));
        stringMap.addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        env.execute();
    }
}
