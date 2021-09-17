package com.silence.gmall.service.impl;

import com.silence.gmall.bean.ProductStats;
import com.silence.gmall.mapper.ProductStatsMapper;
import com.silence.gmall.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class ProductStatsImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;
    @Override
    public BigDecimal getGMV(int date) {
        return productStatsMapper.getGMV(date);
    }

    @Override
    public List<ProductStats> getTrademark(int dt, int lmt) {
        return productStatsMapper.getTrademark(dt,lmt);
    }

    @Override
    public List<ProductStats> getCategory3(int dt, int lmt) {
        return productStatsMapper.getCategory3(dt,lmt);
    }

    @Override
    public List<ProductStats> getSpu(int dt, int lmt) {
        return productStatsMapper.getSpu(dt,lmt);
    }
}
