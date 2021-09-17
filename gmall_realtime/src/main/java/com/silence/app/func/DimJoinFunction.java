package com.silence.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

public interface DimJoinFunction<T> {

    public String getKey(T t);

    public void joinStream(T t, JSONObject jsonObj) throws ParseException;
}
