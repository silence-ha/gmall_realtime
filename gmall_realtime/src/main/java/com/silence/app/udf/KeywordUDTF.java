package com.silence.app.udf;

import com.silence.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String s){
        List<String> words = KeywordUtil.getWords(s);
        for(String w:words){
            collect(Row.of(w));
        }
    }
}
