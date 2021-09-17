package com.silence.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import com.silence.app.func.DimAsyncFunction;
import com.silence.bean.OrderDetail;
import com.silence.bean.OrderInfo;
import com.silence.bean.OrderWide;
import com.silence.utils.ClickHouseUtil;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint/order_wide"));

        String orderInfoTopic="dwd_order_info";
        String orderDetainTopic="dwd_order_detail";
        String groupId="orderwidegroup";
        FlinkKafkaConsumer<String> orderInfoSourceDs = MyKafkaUtil.getConsumer(orderInfoTopic, groupId);
        FlinkKafkaConsumer<String> orderDetailSourceDs = MyKafkaUtil.getConsumer(orderDetainTopic, groupId);
        DataStreamSource<String> orderInfoDs = env.addSource(orderInfoSourceDs);
        DataStreamSource<String> orderDetailDs = env.addSource(orderDetailSourceDs);

        SingleOutputStreamOperator<OrderInfo> orderInfoMapDs = orderInfoDs.map(new RichMapFunction<String, OrderInfo>() {
            SimpleDateFormat sdf=null;
            @Override
            public void open(Configuration parameters) throws Exception {
                sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderInfo map(String s) throws Exception {
                OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                orderInfo.setCreate_ts( sdf.parse(orderInfo.getCreate_time()).getTime());

                return orderInfo;
            }
        });

        SingleOutputStreamOperator<OrderDetail> orderDetailMapDs = orderDetailDs.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat sdf=null;
            @Override
            public void open(Configuration parameters) throws Exception {
                sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String s) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(s, OrderDetail.class);
                orderDetail.setCreate_ts( sdf.parse(orderDetail.getCreate_time()).getTime());

                return orderDetail;

            }
        });

        SingleOutputStreamOperator<OrderInfo> orderInfoWaterDs = orderInfoMapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofMillis(5000))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));

        SingleOutputStreamOperator<OrderDetail> orderDetailWaterDs = orderDetailMapDs.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofMillis(5000))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail orderDetail, long l) {
                        return orderDetail.getCreate_ts();
                    }
                }));


        KeyedStream<OrderInfo, Long> orderInfoKeyedDs = orderInfoWaterDs.keyBy(t -> t.getId());
        KeyedStream<OrderDetail, Long> orderDetailKeyedDs = orderDetailWaterDs.keyBy(t -> t.getOrder_id());

        SingleOutputStreamOperator<OrderWide> processDs = orderInfoKeyedDs
                .intervalJoin(orderDetailKeyedDs)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {

                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

       // processDs.print(">>>>>>");
        //关联用户
        SingleOutputStreamOperator<OrderWide> orderWithUser = AsyncDataStream.unorderedWait(processDs,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void joinStream(OrderWide orderWide, JSONObject jsonObj) throws ParseException {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        String birthday = jsonObj.getString("BIRTHDAY");
                        if(birthday !=null && birthday.length()>0){
                            Long bir = sdf.parse(birthday).getTime();
                            Long curd = System.currentTimeMillis();
                            Long agec = curd - bir;
                            Long age = agec / 1000L / 3600L / 24L / 365L;
                            orderWide.setUser_age(age.intValue());
                        }else{
                            orderWide.setUser_age(0);
                        }

                        orderWide.setUser_gender(jsonObj.getString("GENDER"));
                    }
                }, 60, TimeUnit.SECONDS);

        //关联省市
        SingleOutputStreamOperator<OrderWide> orderWideWithPro = AsyncDataStream.unorderedWait(orderWithUser,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void joinStream(OrderWide orderWide, JSONObject jsonObj) throws ParseException {
                        orderWide.setProvince_name(jsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(jsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_3166_2_code(jsonObj.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(jsonObj.getString("ISO_CODE"));
                    }
                }, 60, TimeUnit.SECONDS);
        //关联sku
        SingleOutputStreamOperator<OrderWide> orderWideWithSku = AsyncDataStream.unorderedWait(orderWideWithPro,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void joinStream(OrderWide orderWide, JSONObject jsonObject) {
                         orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                         orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                         orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                         orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                 }, 60, TimeUnit.SECONDS);
        //关联spu
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDstream = AsyncDataStream.unorderedWait(
                orderWideWithSku, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void joinStream(OrderWide orderWide, JSONObject jsonObject)  {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);
        //关联品类
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3Dstream =
                AsyncDataStream.unorderedWait(
                        orderWideWithSpuDstream, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void joinStream(OrderWide orderWide, JSONObject jsonObject) {
                                orderWide.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //关联品牌
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDstream = AsyncDataStream.unorderedWait(
                orderWideWithCategory3Dstream, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void joinStream(OrderWide orderWide, JSONObject jsonObject)  {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<String> resDs= orderWideWithTmDstream.map(t -> JSON.toJSONString(t));

         resDs.addSink(MyKafkaUtil.getKafkaSink("dwm_order_wide"));
        env.execute();
    }
}
