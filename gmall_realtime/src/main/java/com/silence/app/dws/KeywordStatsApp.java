package com.silence.app.dws;

import com.silence.app.udf.KeywordUDTF;
import com.silence.bean.KeywordStats;
import com.silence.common.GmallConstant;
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

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);

        //检查点 CK 相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/keyword_stats");
        env.setStateBackend(fsStateBackend);

        EnvironmentSettings settings= EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, settings);
        tabEnv.createTemporarySystemFunction("KeywordFunction", KeywordUDTF.class);

        String topic ="dwd_page_log";
        String groupId="keywordstatsgroup";
        tabEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<String,String>,page MAP<String,String>,ts BIGINT," +
                "rowtime as  TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) WITH ("+ MyKafkaUtil.getKafkaDDL(topic,groupId) +")");

        Table table = tabEnv.sqlQuery("select page['item'] fullword,rowtime " +
                "from page_view where page['item'] is not null " +
                "and page['page_id']='good_list'");

        Table keywordView = tabEnv.sqlQuery("SELECT word, rowtime FROM " + table + ", LATERAL TABLE(KeywordFunction(fullword)) AS T(word)");

        Table table2 = tabEnv.sqlQuery("select DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt,'" + GmallConstant.KEYWORD_SEARCH + "' source," +
                "word,count(*) ct, " +
                "UNIX_TIMESTAMP()*1000 ts "+
                "from " + keywordView + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), word");


        DataStream<KeywordStats> keywordStatsDs = tabEnv.toAppendStream(table2, KeywordStats.class);

        keywordStatsDs.print(">>>>keywordStatsDs");

        keywordStatsDs.addSink(ClickHouseUtil.getJdbcSink("insert into keyword_stats values(?,?,?,?,?,?)"));

        env.execute();
    }
}
