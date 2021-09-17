package com.silence.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/user_jump_detail"));



        String groupId="user_jump_detail_groupid";
        String topic="dwd_page_log";
        FlinkKafkaConsumer<String> consumer = MyKafkaUtil.getConsumer(topic, groupId);
        DataStreamSource<String> kafkaDs = env.addSource(consumer);
//        DataStream<String> kafkaDs = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":15000} ",
//
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":30000} "
//                );
        kafkaDs.print("in_json");
        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaDs.map(t -> JSON.parseObject(t));
        SingleOutputStreamOperator<JSONObject> watermarkDs = jsonDs.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                           @Override
                                           public long extractTimestamp(JSONObject jsonObject, long l) {
                                               return jsonObject.getLong("ts");
                                           }
                                       }
                ));
        KeyedStream<JSONObject, String> keyedDs = watermarkDs.keyBy(t -> t.getJSONObject("common").getString("mid"));

        Pattern<JSONObject, JSONObject> patter = Pattern.<JSONObject>begin("start").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        if (jsonObject.getJSONObject("page").getString("last_page_id") == null
                                || jsonObject.getJSONObject("page").getString("last_page_id").length() == 0) {
                            System.out.println(">>>>>>>>>");
                            return true;
                        } else {
                            return false;
                        }

                    }
                }
        ).next("next").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        if (jsonObject.getJSONObject("page").getString("page_id") != null
                                && jsonObject.getJSONObject("page").getString("page_id").length() > 0) {
                            System.out.println("next:" + jsonObject.getJSONObject("page").getString("page_id"));

                            return true;
                        } else {
                            return false;
                        }

                    }
                }
        ).within(Time.milliseconds(10000));
        PatternStream<JSONObject> cepDs = CEP.pattern(watermarkDs, patter);
        OutputTag<String> timeOut=new OutputTag<String>("timeOut"){};
        SingleOutputStreamOperator<Object> outputDs = cepDs.flatSelect(timeOut, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                List<JSONObject> outList = map.get("start");
                for (JSONObject jsonObject : outList) {
                    System.out.println("============"+jsonObject);
                    collector.collect(JSON.toJSONString(jsonObject));
                }
            }
        }, new PatternFlatSelectFunction<JSONObject, Object>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {

            }
        });

        DataStream<String> jumpDstream = outputDs.getSideOutput(timeOut);

        jumpDstream.addSink(MyKafkaUtil.getKafkaSink( "dwm_user_jump_detail"));
        env.execute();
    }
}
