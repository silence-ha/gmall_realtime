package com.silence.gmall.mapper;

import com.silence.gmall.bean.ProductStats;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsMapper {

    @Select("select sum(order_amount) order_amount from product_stats where toYYYYMMDD(stt)=#{date}")
    public BigDecimal getGMV(int date);

    @Select("select tm_id,tm_name,sum(order_amount) order_amount from product_stats " +
            "where toYYYYMMDD(stt)=#{dt} group by tm_id,tm_name having order_amount>0 " +
            "order by order_amount desc limit #{lmt}")
    public List<ProductStats> getTrademark(int dt, int lmt);

    @Select("select category3_id,category3_name,sum(order_amount) order_amount from product_stats " +
            "where toYYYYMMDD(stt)=#{dt} group by category3_id,category3_name having order_amount>0 " +
            "order by order_amount desc limit #{lmt}")
    public List<ProductStats>  getCategory3(int dt, int lmt);


    @Select("select spu_id,spu_name,sum(order_amount) order_amount from product_stats " +
            "where toYYYYMMDD(stt)=#{dt} group by spu_id,spu_name having order_amount>0 " +
            "order by order_amount desc limit #{lmt}")
    public List<ProductStats> getSpu(int dt, int lmt);
}
