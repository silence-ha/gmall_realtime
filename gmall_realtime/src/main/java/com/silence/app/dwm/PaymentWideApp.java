package com.silence.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.silence.bean.OrderWide;
import com.silence.bean.PaymentInfo;
import com.silence.bean.PaymentWide;
import com.silence.utils.DateTimeUtil;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/payment_wide"));

        String orderWideTopic="dwm_order_wide";
        String paymentInfoTopic="dwd_payment_info";
        String groupId="paymentwidegroup";

        FlinkKafkaConsumer<String> orderWideSourceDs = MyKafkaUtil.getConsumer(orderWideTopic, groupId);
        FlinkKafkaConsumer<String> paymentInfoSourceDs = MyKafkaUtil.getConsumer(paymentInfoTopic, groupId);
        DataStreamSource<String> orderWideDs = env.addSource(orderWideSourceDs);
        DataStreamSource<String> paymentInfoDs = env.addSource(paymentInfoSourceDs);


        SingleOutputStreamOperator<OrderWide> owDs = orderWideDs.map(new MapFunction<String, OrderWide>() {
            @Override
            public OrderWide map(String s) throws Exception {
                return JSON.parseObject(s, OrderWide.class);
            }
        });

        SingleOutputStreamOperator<PaymentInfo> pmDs = paymentInfoDs.map(new MapFunction<String, PaymentInfo>() {
            @Override
            public PaymentInfo map(String s) throws Exception {
                return JSON.parseObject(s, PaymentInfo.class);
            }
        });

        SingleOutputStreamOperator<OrderWide> orderWideWithWmDs = owDs.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide o, long l) {
                        return o.getTm_id();
                    }
                }));

        SingleOutputStreamOperator<PaymentInfo> paymentWithWmDs = pmDs.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo paymentInfo, long l) {
                        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        Long ts=0L;
                        try {
                            ts=sdf.parse(paymentInfo.getCallback_time()).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        return ts;
                    }
                }) );


        KeyedStream<OrderWide, Long> orderWideKeyDs = orderWideWithWmDs.keyBy(t -> t.getOrder_id());
        KeyedStream<PaymentInfo, Long> paymentInfoKeyDs = paymentWithWmDs.keyBy(t -> t.getOrder_id());

        SingleOutputStreamOperator<PaymentWide> processDs = paymentInfoKeyDs
                .intervalJoin(orderWideKeyDs)
                .between(Time.seconds(-1800), Time.milliseconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<PaymentWide> collector) throws Exception {
                        collector.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                }).uid("payment_wide_join");

        SingleOutputStreamOperator<String> resDs = processDs.map(t -> JSON.toJSONString(t));
        resDs.addSink(MyKafkaUtil.getKafkaSink("dwm_payment_wide"));

        env.execute();

    }
}
