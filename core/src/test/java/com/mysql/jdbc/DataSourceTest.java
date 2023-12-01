package com.mysql.jdbc;

import com.mysql.jdbc.core.MySqlDataSource;
import org.junit.Test;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author hjx
 */
public class DataSourceTest {

    String url = "jdbc:mysql://127.0.0.1:3306/test";
    String username="11";
    String password="11";
    @Test
    public void getConnection() throws SQLException {
        MySqlDataSource dataSource =  new MySqlDataSource();
        dataSource.setUrl(url);
        Connection connection =  dataSource.getConnection(username,password);
        System.out.println("123");
    }
}
