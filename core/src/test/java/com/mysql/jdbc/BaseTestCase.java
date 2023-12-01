package com.mysql.jdbc;

import com.mysql.jdbc.core.MysqlConnection;
import junit.framework.TestCase;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author hjx
 */
public class BaseTestCase extends TestCase {

    protected  String url = "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8" +
            "&statementInterceptors=com.mysql.jdbc.interceptor.TestInterceptor";
    protected  String dbClass ="com.mysql.jdbc.core.Driver";
    protected  String username="root";
    protected  String password="123456";
    MysqlConnection connection;

    public BaseTestCase(String name){
        super(name);
    }
    public BaseTestCase(){
    }

    @Override
    protected void setUp() throws Exception {
        try {
            Class.forName(dbClass).newInstance();
            this.connection = (MysqlConnection) DriverManager.getConnection(url,username,password);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("connection done.");
    }
}
