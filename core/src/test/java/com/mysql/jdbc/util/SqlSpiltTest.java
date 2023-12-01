package com.mysql.jdbc.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hjx
 */
public class SqlSpiltTest {

    @Test
    public void sqlSpilt(){
        String sql  =  "select * from  user where id = ? and username = ?";
        List<String> endpointList = new ArrayList<>();
        int start = 0;
        while (true){
            int flag =  sql.indexOf("?",start+1);
            if (flag<0){
                break;
            }
            endpointList.add(sql.substring(start,flag));
            start = flag;
        }
        endpointList.forEach(System.out::println);
    }

    @Test
    public void sqlSplit2(){
        String sql  =  "select * from  user where id = ? and username = ?";
        List<String> endpointList = new ArrayList<>();
        int start = 0;
        int end =0;
        if (sql.contains("?")){
            endpointList.add(sql);
        }
        for (char c :sql.toCharArray()) {
            end++;
            if (c == '?'){
                endpointList.add(sql.substring(start,end-1));
                start = end;
            }
        }
        endpointList.forEach(System.out::println);
    }
}
