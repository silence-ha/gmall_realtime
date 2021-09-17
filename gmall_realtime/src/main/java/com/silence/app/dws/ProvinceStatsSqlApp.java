package com.silence.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.silence.bean.ProvinceStats;
import com.silence.utils.ClickHouseUtil;
import com.silence.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;



public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);

        //检查点 CK 相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/province_stats_sql");
        env.setStateBackend(fsStateBackend);

        EnvironmentSettings settings=EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);

        String orderWideTopic="dwm_order_wide";
        String groupId="provincestatsgroup";
        tabEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT," +
                " province_name STRING,province_area_code STRING " +
                " ,province_iso_code STRING,province_3166_2_code STRING,order_id STRING,  " +
                " split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) , " +
                " WATERMARK FOR rowtime AS rowtime) "+
                "  WITH ( "+ MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId)  +")");

        Table table = tabEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime,INTERVAL '10'  SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                " province_id,province_name,province_area_code area_code, province_iso_code iso_code ," +
                "province_3166_2_code iso_3166_2 ," +
                "COUNT(distinct order_id)  order_count,sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from ORDER_WIDE " +
                "group by TUMBLE(rowtime,INTERVAL '10' SECOND)," +
                "province_id,province_name,province_area_code,province_iso_code ,province_3166_2_code");

         DataStream<ProvinceStats> provinceStatsDs = tabEnv.toAppendStream(table, ProvinceStats.class);
        provinceStatsDs.print(">>>>>>res");
         provinceStatsDs.addSink(ClickHouseUtil.getJdbcSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));
        env.execute();
    }

}
