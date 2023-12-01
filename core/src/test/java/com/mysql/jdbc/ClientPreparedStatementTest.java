package com.mysql.jdbc;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author hjx
 */
public class ClientPreparedStatementTest extends BaseTestCase {

    // 客户端预编译是客户端连接缓存参数和占位符,执行的时候还是发送一整条语句
    //是指 "select * from  user where id = ?"; ?占位符是存在客户端的 例如设置id= 10

    //  发送mysql服务器语句还是完整的语句,例如: select * from  user where id = 10
    //  "select * from  user where id = ? and username = ?";
    // 第一个参数  select * from  user where id = ?
    // 第二个参数   and username =

    public ClientPreparedStatementTest(){}


    public void testQuery() throws SQLException {
        String sql =  "select * from  user where id = ?";
        PreparedStatement preparedStatement = this.connection.prepareStatement(sql);
        preparedStatement.setInt(1,3);
        ResultSet resultSet =  preparedStatement.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getDouble(1));
            System.out.println(resultSet.getString(2));
            System.out.println(resultSet.getString(3));
            Timestamp timestamp = resultSet.getTimestamp(4);
              System.out.println(timestamp.toLocalDateTime().toString());
            System.out.println(resultSet.getBoolean(5));

        }
    }
}
