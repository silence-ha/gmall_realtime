package com.silence.utils;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {

    public static List<String> getWords(String str) {
        StringReader sr=new StringReader(str);
        IKSegmenter ik=new IKSegmenter(sr,true);
        List<String> l=new ArrayList<>();
        Lexeme lex=null;
        while (true){

            try {
                if((lex=ik.next())!=null){
                    String lexemeText = lex.getLexemeText();
                    l.add(lexemeText);
                }else{
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return l;
    }
}
