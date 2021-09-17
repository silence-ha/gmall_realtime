package com.silence.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.silence.app.func.DimAsyncFunction;
import com.silence.bean.OrderWide;
import com.silence.bean.PaymentWide;
import com.silence.bean.ProductStats;
import com.silence.common.GmallConstant;
import com.silence.utils.ClickHouseUtil;
import com.silence.utils.DateTimeUtil;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

         //检查点 CK 相关设置
         env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
         env.getCheckpointConfig().setCheckpointTimeout(60000);
         StateBackend fsStateBackend = new FsStateBackend(
         "hdfs://hadoop202:8020/gmall/flink/checkpoint/product_stats");
         env.setStateBackend(fsStateBackend);


        //TODO 1.从 Kafka 中获取数据流
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource =
        MyKafkaUtil.getConsumer(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> orderWideSource =
        MyKafkaUtil.getConsumer(orderWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> paymentWideSource =
        MyKafkaUtil.getConsumer(paymentWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce =
        MyKafkaUtil.getConsumer(favorInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> cartInfoSource =
        MyKafkaUtil.getConsumer(cartInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> refundInfoSource =
        MyKafkaUtil.getConsumer(refundInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> commentInfoSource =
        MyKafkaUtil.getConsumer(commentInfoSourceTopic,groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream= env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream= env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream= env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream= env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream= env.addSource(commentInfoSource);

        SingleOutputStreamOperator<ProductStats> pageAndDispStatsDstream = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String s, Context context, Collector<ProductStats> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                if ("good_detail".equals(jsonObject.getJSONObject("page").getString("page_id"))) {
                    ProductStats build = ProductStats.builder().sku_id(jsonObject.getJSONObject("page").getLong("item"))
                            .click_ct(1L)
                            .ts(jsonObject.getLong("ts"))
                            .build();
                    collector.collect(build);
                }
                JSONArray display = jsonObject.getJSONArray("displays");
                if (display != null && display.size() > 0) {
                    for (int i = 0; i < display.size(); i++) {
                        JSONObject json = display.getJSONObject(i);
                        if(json.getString("item_type").equals("sku_id")){
                            ProductStats item = ProductStats.builder().sku_id(json.getLong("item"))
                                    .display_ct(1L)
                                    .ts(jsonObject.getLong("ts"))
                                    .build();
                            collector.collect(item);
                        }

                    }
                }
            }
        });


        //2.2 转换下单流数据
         SingleOutputStreamOperator<ProductStats> orderWideStatsDstream = orderWideDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String s, Context context, Collector<ProductStats> collector) throws Exception {
                OrderWide orderWide = JSON.parseObject(s, OrderWide.class);
                String create_time = orderWide.getCreate_time();
                Long ts = DateTimeUtil.toTs(create_time);

                ProductStats build = ProductStats.builder().sku_id(orderWide.getSku_id())
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(ts)
                        .build();
                collector.collect(build);

            }
        });
        //2.3 转换收藏流数据
        final SingleOutputStreamOperator<ProductStats> favorStatsDstream = favorInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                Long ts= DateTimeUtil.toTs(jsonObject.getString("create_time"));
                ProductStats build = ProductStats.builder().sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(ts)
                        .build();
                return build;
            }
        });

        //2.4 转换购物车流数据
         SingleOutputStreamOperator<ProductStats> cartStatsDstream = cartInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                ProductStats sku_id = ProductStats.builder().sku_id(jsonObject.getLong("sku_id"))
                        .cart_ct(1L)
                        .ts(ts)
                        .build();
                return sku_id;
            }
        });


        //2.5 转换支付流数据
         SingleOutputStreamOperator<ProductStats> paymentStatsDstream = paymentWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String s) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(s, PaymentWide.class);
                Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                ProductStats build = ProductStats.builder().sku_id(paymentWide.getSku_id())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .ts(ts)
                        .build();
                return build;
            }
        });
        //2.6 转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundStatsDstream = refundInfoDStream.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts).build();
                    return productStats;
                });
        //2.7 转换评价流数据
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDstream = commentInfoDStream.map(
                json -> {
                    JSONObject commonJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ?
                            1L : 0L;
                    return ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                });

        DataStream<ProductStats> productStatDetailDStream = pageAndDispStatsDstream.union(
                orderWideStatsDstream, cartStatsDstream,
                paymentStatsDstream, refundStatsDstream,favorStatsDstream,
                commonInfoStatsDstream);

        SingleOutputStreamOperator<ProductStats> productStatsWithTsStream =
                productStatDetailDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductStats>forMonotonousTimestamps().withTimestampAssigner(
                                (productStats, recordTimestamp) -> {
                                    return productStats.getTs();
                                })
                );
        SingleOutputStreamOperator<ProductStats> productStatsDstream = productStatsWithTsStream
                //5.1 按照商品 id 进行分组
                .keyBy(
                        new KeySelector<ProductStats, Long>() {
                            @Override
                            public Long getKey(ProductStats productStats) throws Exception {
                                return productStats.getSku_id();
                            }
                        })
                //5.2 开窗 窗口长度为 10s
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //5.3 对窗口中的数据进行聚合
                .reduce(new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats t1, ProductStats t2) throws Exception {
                                t1.setCart_ct(t1.getCart_ct() + t2.getCart_ct());
                                t1.setDisplay_ct(t1.getDisplay_ct() + t2.getDisplay_ct());
                                t1.setClick_ct(t1.getClick_ct() + t2.getClick_ct());
                                t1.setFavor_ct(t1.getFavor_ct() + t2.getFavor_ct());
                                t1.setOrder_amount(t1.getOrder_amount().add(t2.getOrder_amount()));
                                t1.getOrderIdSet().addAll(t2.getOrderIdSet());
                                t1.setOrder_ct(t1.getOrderIdSet().size() + 0L);
                                t1.setOrder_sku_num(t1.getOrder_sku_num() + t2.getOrder_sku_num());
                                t1.getPaidOrderIdSet().addAll(t2.getPaidOrderIdSet());
                                t1.setPaid_order_ct(t1.getPaidOrderIdSet().size() + 0L);
                                t1.setPayment_amount(t1.getPayment_amount().add(t2.getPayment_amount()));
                                t1.getRefundOrderIdSet().addAll(t2.getRefundOrderIdSet());
                                t1.setRefund_order_ct(t1.getRefundOrderIdSet().size() + 0L);
                                t1.setRefund_amount(t1.getRefund_amount().add(t2.getRefund_amount()));
                                t1.setComment_ct(t1.getComment_ct() + t2.getComment_ct());
                                t1.setGood_comment_ct(t1.getGood_comment_ct() + t2.getGood_comment_ct());

                                return t1;
                            }
                        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow timeWindow, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                                long start = timeWindow.getStart();
                                long end = timeWindow.getEnd();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                                String stt = sdf.format(new Date(start));
                                String edt = sdf.format(new Date(end));
                                for (ProductStats ps : iterable) {
                                    ps.setStt(stt);
                                    ps.setEdt(edt);
                                    ps.setTs(new Date().getTime());

                                    collector.collect(ps);
                                }
                            }
                        }

                );
        //6.1 补充 SKU 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream =
                AsyncDataStream.unorderedWait(productStatsDstream,
                        new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                            @Override
                            public void joinStream(ProductStats productStats, JSONObject jsonObject)
                            {
                                productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                                productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                                productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                                productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                                productStats.setTm_id(jsonObject.getLong("TM_ID"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSku_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //6.2 补充 SPU 维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                            @Override
                            public void joinStream(ProductStats productStats, JSONObject jsonObject)
                            {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //6.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void joinStream(ProductStats productStats, JSONObject jsonObject)
                            {
                                productStats.setCategory3_name(jsonObject.getString("NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);
        //6.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void joinStream(ProductStats productStats, JSONObject jsonObject)
                            {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);


        productStatsWithTmDstream.print(">>>res");
        productStatsWithTmDstream.addSink(ClickHouseUtil.getJdbcSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
