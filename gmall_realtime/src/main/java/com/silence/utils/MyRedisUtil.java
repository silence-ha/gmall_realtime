package com.silence.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class MyRedisUtil {
    public static JedisPool pool;

    public static void main(String[] args) {
        init();
    }
    public static Jedis getRedis(){
        if(pool==null){
            init();
        }
        return pool.getResource();
    }

    private static void init() {
        String redisIp = "192.168.137.202"; // redis服务器IP
        int redisPort = 6379; // redis端口号
        //String redisPassWord = "test123"; // redis连接密码
        int timeOut = 1000;
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(10); // 控制pool中最多有多少个连接给空闲状态
        config.setMaxTotal(20); // 控制pool中最大连接实例
        config.setMaxWaitMillis(10000); // 最大超时等待时间（毫秒）
        config.setTestOnBorrow(true); // 获取连接提前验证
        config.setTestOnReturn(true); // 归还连接提前验证
        pool = new JedisPool(config, redisIp, redisPort, timeOut);

    }
}
