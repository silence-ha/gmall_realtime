package com.silence.app.func;


import com.alibaba.fastjson.JSONObject;
import com.silence.utils.DimUtil;
import com.silence.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{
    ThreadPoolExecutor pool=null;
    String tbNm="";
    public DimAsyncFunction(String tableName){
        this.tbNm=tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        pool= ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        pool.submit(new Runnable() {
            @Override
            public void run() {

                String key= null;
                try {
                    key = getKey(t);
                    JSONObject jsonObj = DimUtil.getQueryWithCache(tbNm, key);
                    joinStream(t,jsonObj);
                    resultFuture.complete(Arrays.asList(t));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException("查询维度出错");
                }

            }
        });
    }

}
