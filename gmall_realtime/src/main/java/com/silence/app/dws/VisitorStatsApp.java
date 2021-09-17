package com.silence.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.silence.bean.VisitorStats;
import com.silence.utils.ClickHouseUtil;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/visitor_stats"));

        String pvTopic="dwd_page_log";
        String uvTopic="dwm_unique_visit";
        String ujpTopic="dwm_user_jump_detail";
        String groupId="visitorstatsapp_group";
        FlinkKafkaConsumer<String> pvCon = MyKafkaUtil.getConsumer(pvTopic,groupId);
        FlinkKafkaConsumer<String> uvCon = MyKafkaUtil.getConsumer(uvTopic,groupId);
        FlinkKafkaConsumer<String> ujpCon = MyKafkaUtil.getConsumer(ujpTopic,groupId);
        DataStreamSource<String> pvDs = env.addSource(pvCon);
        DataStreamSource<String> uvDs = env.addSource(uvCon);
        DataStreamSource<String> ujpDs = env.addSource(ujpCon);

        //pv,dur
        SingleOutputStreamOperator<VisitorStats> pvVistorStatsDs = pvDs.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject json = JSON.parseObject(s);
                JSONObject commonObj = json.getJSONObject("common");
                VisitorStats vs = new VisitorStats(
                        "",
                        "",
                        commonObj.getString("vc"),
                        commonObj.getString("ch"),
                        commonObj.getString("ar"),
                        commonObj.getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        json.getJSONObject("page").getLong("during_time"),
                        json.getLong("ts")
                );
                return vs;
            }
        });

        //uv
        SingleOutputStreamOperator<VisitorStats> uvVistorStatsDs = uvDs.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject json = JSON.parseObject(s);
                JSONObject commonObj = json.getJSONObject("common");
                VisitorStats vs = new VisitorStats(
                        "",
                        "",
                        commonObj.getString("vc"),
                        commonObj.getString("ch"),
                        commonObj.getString("ar"),
                        commonObj.getString("is_new"),
                        1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        json.getLong("ts")
                );
                return vs;
            }
        });

        //sv
        SingleOutputStreamOperator<VisitorStats> svVistorStatsDs = pvDs.process(new ProcessFunction<String, VisitorStats>() {

            @Override
            public void processElement(String s, Context context, Collector<VisitorStats> collector) throws Exception {
                JSONObject json = JSON.parseObject(s);
                String last_page_id = json.getJSONObject("page").getString("last_page_id");
                if (last_page_id == null || last_page_id.length() == 0) {
                    JSONObject commonObj = json.getJSONObject("common");
                    VisitorStats vs = new VisitorStats(
                            "",
                            "",
                            commonObj.getString("vc"),
                            commonObj.getString("ch"),
                            commonObj.getString("ar"),
                            commonObj.getString("is_new"),
                            0L,
                            0L,
                            1L,
                            0L,
                            0L,
                            json.getLong("ts")
                    );
                    collector.collect(vs);
                }

            }
        });

        //uj
        final SingleOutputStreamOperator<VisitorStats> ujVistorStatsDs = ujpDs.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String s) throws Exception {
                JSONObject json = JSON.parseObject(s);
                JSONObject commonObj = json.getJSONObject("common");
                VisitorStats vs = new VisitorStats(
                        "",
                        "",
                        commonObj.getString("vc"),
                        commonObj.getString("ch"),
                        commonObj.getString("ar"),
                        commonObj.getString("is_new"),
                        0L,
                        0L,
                        0L,
                        1L,
                        0L,
                        json.getLong("ts")
                );
                return vs;
            }
        });

        DataStream<VisitorStats> unionDs = pvVistorStatsDs.union(
                uvVistorStatsDs,
                svVistorStatsDs,
                ujVistorStatsDs
        );


        SingleOutputStreamOperator<VisitorStats> withWaterMarkDs = unionDs.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long l) {
                        return visitorStats.getTs();
                    }
                }));

        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDs = withWaterMarkDs.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(visitorStats.getVc(), visitorStats.getCh(), visitorStats.getAr(), visitorStats.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> reduceDs = windowDs.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats vs1, VisitorStats vs2) throws Exception {
                vs1.setPv_ct(vs1.getPv_ct() + vs2.getPv_ct());
                vs1.setUv_ct(vs1.getUv_ct() + vs2.getUv_ct());
                vs1.setUj_ct(vs1.getUj_ct() + vs2.getUj_ct());
                vs1.setDur_sum(vs1.getDur_sum() + vs2.getDur_sum());
                return vs1;
            }
        }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String, String, String, String> key, Context context, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long start_dt = context.window().getStart();
                long end_dt = context.window().getEnd();
                for(VisitorStats vs : iterable){
                    vs.setStt(sdf.format(new Date(start_dt)));
                    vs.setEdt(sdf.format(new Date(end_dt)));
                    collector.collect(vs);
                }
            }
        });


        reduceDs.print(">>>>>>");
        reduceDs.addSink(ClickHouseUtil.getJdbcSink("insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
