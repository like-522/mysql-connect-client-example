package com.mysql.jdbc;

import static org.junit.Assert.assertTrue;

import com.mysql.jdbc.core.SqlBuffer;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Unit test for simple App.
 */
public class DriverTest {

    String url = "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8" +
            "&useServerPreparedStmts=true&cachePreparedStatements=true";
    String username="root";
    String password="123456";

    @Test
    public void DriverConnection() throws ClassNotFoundException, SQLException {
        Connection connection = getConnection();
        String insertSql =  "insert into user (id, username ) VALUES (10,\"测试1\")";
        String selectSql = "select * from  user";
        PreparedStatement preparedStatement =  connection.prepareStatement(selectSql);
        ResultSet resultSet =  preparedStatement.executeQuery();
        while (resultSet.next()){
           int id = resultSet.getInt(1);
            System.out.println(id);
        }
        System.out.println("success");
    }

   public Connection getConnection() throws SQLException{
        try {
            Class.forName("com.mysql.jdbc.core.Driver");
            return   DriverManager.getConnection(url,username,password);
        }catch (Exception e){
            e.printStackTrace();
            throw new SQLException("getConnection err.");
        }

    }

    @Test
    public void arrayTwo(){
        char c = Character.forDigit(1,10);
        System.out.println(c);
        System.out.println(1);
    }
}
