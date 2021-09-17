package com.silence.app.func;

import com.alibaba.fastjson.JSONObject;
import com.silence.common.GmallConfig;
import com.silence.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {
    Connection conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) {
        String tbName=value.getString("sink_table");
        JSONObject dataObj=value.getJSONObject("data");
        if(dataObj!=null && dataObj.size()>0){
            try {
                String sql = genUpsertSql(tbName,dataObj);
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.executeUpdate();
                conn.commit();
                ps.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("执行sql失败");
            }

        }
        String type=value.getString("type");
        if("insert".equals(type) || "delete".equals(type)){
            DimUtil.delCache("tbName",dataObj.getString("id"));
        }


    }

    private String genUpsertSql(String tbName, JSONObject dataObj) {
        Set<String> keys = dataObj.keySet();
        String sql_head="upsert into "+GmallConfig.HBASE_SCHEMA+"."+tbName+"("+ StringUtils.join(keys,",")+")";
        Collection<Object> values = dataObj.values();
        String sql_after=" values ('"+StringUtils.join(values,"','")+"')";
        return sql_head+ sql_after;
    }
}
