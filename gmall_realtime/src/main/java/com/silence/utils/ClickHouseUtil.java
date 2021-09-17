package com.silence.utils;

import com.silence.bean.TransientSink;
import com.silence.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;

public class ClickHouseUtil {
    public static <T> SinkFunction getJdbcSink(String sql){
        SinkFunction<T> sink = JdbcSink.<T>sink(
                sql,
                (ps, t) -> {
                    Field[] fields = t.getClass().getDeclaredFields();
                    int flag=0;
                    for(int i=0;i<fields.length;i++){
                        Field field=fields[i];
                        TransientSink annot = field.getAnnotation(TransientSink.class);
                        if(annot!=null){
                            flag++;
                        }else{
                            try {
                                field.setAccessible(true);
                                ps.setObject(i+1-flag,field.get(t));
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
        return sink;
    }
}
