package com.silence.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySqlUtil {

    public static <T> List<T> queryList(String sql,Class<T> clazz, Boolean underScoreToCamel){

        Connection conn=null;
        PreparedStatement ps=null;
        ResultSet rs = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://hadoop202:3306/gmall_realtime?characterEncoding=utf-8&useSSL=false", "root", "000000");
            ps  = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            ResultSetMetaData md = ps.getMetaData();
            List<T> sqlList=new ArrayList<T>();
            while (rs.next()){
                T obj = clazz.newInstance();
                for(int i=0;i<md.getColumnCount();i++){
                    String proName=md.getColumnName(i+1);
                    if(underScoreToCamel){
                        proName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, proName);

                    }
                    BeanUtils.copyProperty(obj,proName,rs.getObject(i+1));
                }
                sqlList.add(obj);
            }
            return sqlList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询 mysql 失败！");
        }finally {

            try {
                if(rs!=null){
                    rs.close();
                }
                if(ps!=null){
                    ps.close();
                }
                if(conn!=null){
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}