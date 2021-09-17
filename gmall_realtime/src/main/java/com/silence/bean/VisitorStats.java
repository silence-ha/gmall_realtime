package com.silence.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class VisitorStats {
    private String stt;
    private String edt;
    private String vc;
    private String ch;
    private String ar;
    private String is_new;
    private Long uv_ct=0L;
    private Long pv_ct=0L;
    //度量： 进入次数
    private Long sv_ct=0L;
    //度量： 跳出次数
    private Long uj_ct=0L;
    //度量： 持续访问时间
    private Long dur_sum=0L;
    //统计时间
    private Long ts;
}
