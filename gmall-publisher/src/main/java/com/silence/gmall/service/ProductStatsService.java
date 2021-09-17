package com.silence.gmall.service;

import com.silence.gmall.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

public interface ProductStatsService {
    public BigDecimal getGMV(int date);

    public List<ProductStats> getTrademark(int dt, int lmt);


    public List<ProductStats>  getCategory3(int dt, int lmt);

    public List<ProductStats> getSpu(int dt, int lmt);

}
