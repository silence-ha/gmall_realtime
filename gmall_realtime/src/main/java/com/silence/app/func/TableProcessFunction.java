package com.silence.app.func;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.silence.bean.TableProcess;
import com.silence.common.GmallConfig;
import com.silence.utils.MySqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction<J extends JSON, J1 extends JSON> extends ProcessFunction<JSONObject,JSONObject> {
    Map<String,TableProcess> tableMap=null;
    Set<String> tableSet=null;
    Connection conn=null;
    private OutputTag<JSONObject> outputTag;
    public TableProcessFunction(OutputTag<JSONObject> outputTag){
        this.outputTag=outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        tableMap=new HashMap<>();
        tableSet=new HashSet<>();
        initSql();
        Timer timer=new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                initSql();
            }
        },5000,5000);
    }

    private void initSql() {
        String sql="select * from table_process";
        List<TableProcess> tableList = MySqlUtil.queryList(sql, TableProcess.class, true);
        for(TableProcess tp:tableList){
            String key=tp.getSourceTable()+":"+tp.getOperateType();
            tableMap.put(key,tp);
            if("insert".equals(tp.getOperateType()) && "hbase".equals(tp.getSinkType())){
                boolean isNotExists=tableSet.add(tp.getSourceTable());
                if(isNotExists){
                    checkTable(tp.getSinkTable(),tp.getSinkColumns(),tp.getSinkPk(),tp.getSinkExtend());
                }
            }

        }

    }

    private void checkTable(String sinkTable, String sinkColums, String sinkPk, String sinkExtend) {
        if(sinkPk==null){
            sinkPk="id";
        }
        if(sinkExtend==null){
            sinkExtend="";
        }
        String cols[] = sinkColums.split(",");
        StringBuilder sb=new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS "+ GmallConfig.HBASE_SCHEMA+"."+sinkTable+"(");
        for(int i=0;i<cols.length;i++){
            if(i==cols.length-1){
                if(sinkPk.equals(cols[i])){
                    sb.append(cols[i]+" varchar primary key);");
                }else{
                    sb.append("info."+cols[i]+" varchar)");
                }

            }else{
                if(sinkPk.equals(cols[i])){
                    sb.append(cols[i]+" varchar primary key,");
                }else{
                    sb.append("info."+cols[i]+" varchar,");
                }

            }
        }
        sb.append(sinkExtend);
        String sql=sb.toString();
        try {
            PreparedStatement ps= conn.prepareStatement(sql);
            ps.execute();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("建表失败！！！");
        }

    }


    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
        String sourceTb=jsonObject.getString("table");
        String sinkTp=jsonObject.getString("type");


        //注意：问题修复  如果使用Maxwell的Bootstrap同步历史数据  ，这个时候它的操作类型叫bootstrap-insert
        if ("bootstrap-insert".equals(sinkTp)) {
            sinkTp = "insert";
            jsonObject.put("type", sinkTp);
        }

        String key=sourceTb+":"+sinkTp;
        TableProcess tp = tableMap.get(key);
        if(tp !=null){
            jsonObject.put("sink_table",tp.getSinkTable());
            JSONObject dataObj=jsonObject.getJSONObject("data");
            filterData(dataObj,tp.getSinkColumns());
        }else{
           // System.out.println("没有配置信息");
        }
        if(tp!=null && tp.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
            System.out.println("+++++++"+tp.toString());
            context.output(outputTag,jsonObject);
        }else if(tp!=null && tp.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
            collector.collect(jsonObject);
        }
    }

    private void filterData(JSONObject dataObj, String sinkColums) {
        String vols[]=sinkColums.split(",");
        Set<Map.Entry<String, Object>> dataEntry = dataObj.entrySet();
        List<String> colsList = Arrays.asList(vols);

        Iterator<Map.Entry<String, Object>> iterator = dataEntry.iterator();
        while(iterator.hasNext()){
           Map.Entry<String, Object> next = iterator.next();
           if(!colsList.contains(next.getKey())){
               iterator.remove();
           }
        }

    }
}
