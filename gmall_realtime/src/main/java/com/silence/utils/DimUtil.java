package com.silence.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {
    public static void main(String[] args) {
//        JSONObject queryNoCache = getQueryNoCache("DIM_USER_INFO", Tuple2.of("id", "9945"));
        JSONObject dim_user_info = getQueryWithCache("DIM_USER_INFO", "106");
        System.out.println(dim_user_info.toJSONString());
    }
    public static JSONObject getQueryNoCache(String tableName, Tuple2<String,String>... col){
        StringBuilder sb=new StringBuilder();
        sb.append("select * from "+tableName+" where ");
        for (int i=0;i<col.length;i++){
            if(i==col.length-1){
                sb.append(col[i].f0+"='"+col[i].f1+"' ");
            }else{
                sb.append(col[i].f0+"='"+col[i].f1+"' and ");
            }

        }
        String str = sb.toString();
        System.out.println(str);
        //select * from a where id='' and age='' and name=''
        List<JSONObject> obj = MyPhoenixUtil.queryList(str, JSONObject.class);
        JSONObject jsonObj=null;
        if(obj.size()>0 && obj !=null){
            jsonObj = obj.get(0);
        }else {
            System.out.println("没有查询到数据");
        }
        return jsonObj;
    }

    public static JSONObject getQueryWithCache(String tableName, String idVal){
        Jedis jedis=MyRedisUtil.getRedis();
        JSONObject idObj=null;
        String key="dim:"+tableName.toLowerCase()+":"+idVal;
        String getVal = jedis.get(key);
        if(getVal!=null && getVal.length()>0){
            idObj= JSON.parseObject(getVal);
        }else{
            idObj = getQueryNoCache(tableName, Tuple2.of("id", idVal));
            jedis.setex(key,3600 * 24,idObj.toJSONString());
        }
        if(jedis !=null){
            jedis.close();
        }

        return idObj;
    }

    public static void delCache(String tableName,String id){
        Jedis jedis=MyRedisUtil.getRedis();
        jedis.del("dim:"+tableName.toLowerCase()+":"+id);
        if(jedis!=null){
            jedis.close();
        }
    }

}
