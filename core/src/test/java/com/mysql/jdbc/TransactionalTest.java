package com.mysql.jdbc;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author hjx
 */
public class TransactionalTest extends BaseTestCase {

    public TransactionalTest(){}


    public void testCommit() throws SQLException {
        String insert = "insert into user(id,username,code) values (108,\"start\",\"99\")";
        String insert1 = "insert into user(id,username,code) values (107,\"start\",\"99\")";
        connection.setAutoCommit(false);
        Statement preparedStatement =  this.connection.createStatement();
        System.out.println(preparedStatement.executeUpdate(insert));
        System.out.println(preparedStatement.executeUpdate(insert1));
        connection.commit();
    }
}
