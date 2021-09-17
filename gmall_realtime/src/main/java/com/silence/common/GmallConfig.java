package com.silence.common;

public class GmallConfig {

    public static final String HBASE_SCHEMA="GMALL_REALTIME";

    //Phonenix连接的服务器地址
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";

    //ClickHouse的URL连接地址
    public static final String CLICKHOUSE_URL="jdbc:clickhouse://hadoop202:8123/default";

}
