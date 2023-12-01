package com.mysql.jdbc;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author hjx
 */
public class ServerPreparedStatementTest extends BaseTestCase{

    public ServerPreparedStatementTest(String name) {
        super(name);
    }
    public ServerPreparedStatementTest() {
    }

    public void setUp() throws Exception {
        //
        url = "jdbc:mysql://127.0.0.1:3306/test?useUnicode=true&characterEncoding=utf8" +
                "&useServerPreparedStmts=true&cachePreparedStatements=true";
        super.setUp();
    }

    /**
     * 运行所有的测试用例
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(ServerPreparedStatementTest.class);
    }

    //测试用例方法要以test开头
    public void testSelectSql() throws SQLException {
        String selectSql = "select * from  user";
        PreparedStatement preparedStatement =  connection.prepareStatement(selectSql);
        ResultSet resultSet =  preparedStatement.executeQuery();
        while (resultSet.next()){
            int id = resultSet.getInt(1);
            System.out.println(id);
            String value = resultSet.getString(2);
            System.out.println(value);
        }
    }

    public void testCachePreparedStatementSql() throws SQLException {
        String selectSql = "select * from  user";
        PreparedStatement preparedStatement =  connection.prepareStatement(selectSql);
        PreparedStatement preparedStatement1  =  connection.prepareStatement(selectSql);
        System.out.println(preparedStatement== preparedStatement1);
    }

    public void testSelectByParam() throws SQLException {
        String selectSql = "select * from  user where id = ?";
        PreparedStatement preparedStatement =  connection.prepareStatement(selectSql);
        preparedStatement.setInt(1,8);
        ResultSet resultSet =  preparedStatement.executeQuery();
        while (resultSet.next()){
            System.out.println( resultSet.getDouble(1));
            System.out.println(resultSet.getString(2));
            System.out.println(resultSet.getString(3));
             Timestamp timestamp = resultSet.getTimestamp(4);
             System.out.println(timestamp.toLocalDateTime().toString());
            System.out.println(resultSet.getBoolean(5));
        }
    }


}
