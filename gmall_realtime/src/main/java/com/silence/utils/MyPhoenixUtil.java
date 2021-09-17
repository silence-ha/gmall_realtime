package com.silence.utils;

import com.alibaba.fastjson.JSONObject;
import com.silence.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MyPhoenixUtil {
    public static Connection conn=null;


    public static void main(String[] args) {
//        List<JSONObject> jsonObjects = queryList("select * from DIM_USER_INFO where id='9945'", JSONObject.class);
//        System.out.println(jsonObjects.get(0).toJSONString());
    }
    private static void init()  {

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static <T> List<T> queryList(String sql,Class<T> clazz)  {
        if(conn==null){
            init();
        }
        List<T> sqlList=new ArrayList<>();
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {

            ps = conn.prepareStatement(sql);

            ResultSetMetaData md = ps.getMetaData();
            rs = ps.executeQuery();
            while (rs.next()){
                T t = clazz.newInstance();
                for(int i = 0;i<md.getColumnCount();i++){
                    String cn = md.getColumnName(i + 1);
                    BeanUtils.setProperty(t,cn,rs.getObject(i+1));
                }
                sqlList.add(t);
            }
            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("phoenix查询失败");
        }
        return sqlList;
    }


}
