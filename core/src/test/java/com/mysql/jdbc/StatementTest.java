package com.mysql.jdbc;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author hjx
 */
public class StatementTest extends BaseTestCase {

    public void testCreateStatement() throws SQLException {
        Statement statement =  connection.createStatement();
        String select = "select * from  user where id = 10";
        ResultSet resultSet =  statement.executeQuery(select);
        while (resultSet.next()){
            System.out.println(resultSet.getInt(1));
        }
    }


    public void testUpdateStatement() throws SQLException {
        Statement statement =  connection.createStatement();
        String update = "update user set code = \"statem\" where id = 10";
        System.out.println(statement.executeUpdate(update));
    }

    public void testString(){
        String update = "update user set code = \"statem\" where id = 10";
        byte[] bytes =  update.getBytes();
        System.out.println(new String(bytes));
    }

    //获取新增的主键id.
    public void testInsertId() throws SQLException {
        String insert1 = "insert into user(username,code) values (\"testPrepareStatementAutoGeneratedKeys\",\"99\")," +
                "(\"testPrepareStatementAutoGeneratedKeys\",\"100\")";
        Statement statement =  connection.createStatement();
        int value =  statement.executeUpdate(insert1,Statement.RETURN_GENERATED_KEYS);
        System.out.println(value);
        ResultSet resultSet =  statement.getGeneratedKeys();
        while (resultSet.next()){
            System.out.println(resultSet.getInt(1));
        }
    }

}
