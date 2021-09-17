package com.silence.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaseLogApp {
    private static final String TOPIC_START ="dwd_start_log";
    private static final String TOPIC_PAGE ="dwd_page_log";
    private static final String TOPIC_DISPLAY ="dwd_display_log";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/base_log"));

        String groupId="ods_base_log_groupid";
        String topic="ods_base_log";
        FlinkKafkaConsumer<String> consumer = MyKafkaUtil.getConsumer(topic, groupId);
        DataStreamSource<String> kafkaDs = env.addSource(consumer);

        SingleOutputStreamOperator<JSONObject> jsonDs = kafkaDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                return jsonObject;
            }
        });
        KeyedStream<JSONObject, String> keyDs = jsonDs.keyBy(t -> t.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> newMidDs = keyDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            SimpleDateFormat sdf = null;
            ValueState<String> midstate = null;

            @Override
            public void open(Configuration context) throws Exception {
                sdf = new SimpleDateFormat("yyyyMMdd");
                ValueStateDescriptor<String> middesc = new ValueStateDescriptor<String>("mid_state", String.class);
                midstate = getRuntimeContext().getState(middesc);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String newDate = sdf.format(new Date(ts));
                if ("1".equals(isNew)) {
                    String mid = midstate.value();
                    if (mid != null && mid.length() != 0) {
                        if (!mid.equals(newDate)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    midstate.update(newDate);
                }

                return jsonObject;
            }
        });
        OutputTag<String> startLog=new OutputTag<String>("startLog"){};
        OutputTag<String> displayLog=new OutputTag<String>("displayLog"){};
        SingleOutputStreamOperator<String> pageDs = newMidDs.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {

                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null && start.size() != 0) {
                    context.output(startLog, jsonObject.toString());
                } else {
                    collector.collect(jsonObject.toString());
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject dis = displays.getJSONObject(i);
                            String pageId = jsonObject.getJSONObject("page").getString("page_id");
                            dis.put("page_id", pageId);
                            context.output(displayLog, dis.toString());
                        }
                    }

                }
            }
        });
        DataStream<String> startOutput = pageDs.getSideOutput(startLog);
        DataStream<String> displayOutput = pageDs.getSideOutput(displayLog);
        pageDs.print("page>>>>");
        startOutput.print("startOutput>>>>>");
        displayOutput.print("displayOutput>>>>>>>");

        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);

        pageDs.addSink(pageSink);
        startOutput.addSink(startSink);
        displayOutput.addSink(displaySink);


        env.execute();
    }
}
