package com.silence.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String stt;
    private String edt;
    private String word;
    private String source;
    private Long ct;
    private Long ts;
}
