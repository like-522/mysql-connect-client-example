package com.mysql.jdbc.core;

import com.mysql.jdbc.core.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hjx
 */
public class StatementImpl implements Statement {
    //mysql 连接
    protected MysqlConnection connection;

    //连接的库
    protected String database;

    // resultSet类型设置, 只读, 可以滚动读, 可以滚动可以更改
    protected int resultSetType;

    // resultSet更新设置, 1007 不能更新, 1008 可以更新.
    protected int resultSetConcurrency;

    // 当前执行语句的 results
    protected MysqlResultSet results = null;

    //Statement 是否关闭
    protected boolean isClosed = false;

    //是否获取当前更新的主键id
    protected boolean retrieveGeneratedKeys = false;

    // 最多字段大小
    protected int maxFieldSize = MysqlDefs.MAX_FIELD_SIZE;

    //当前返回的最多行数
    protected int maxRows = -1;

    //超时时间,没有做超时时间处理
    protected int timeoutInMillis = 0;

    //获取多少行
    protected int fetchSize = 0;

    //获取查询sql的第一个字符,校验语句.
    protected  char firstSqlChar;

    //当前的sql是否在执行,用于取消当前执行sql
    protected final AtomicBoolean statementExecuting = new AtomicBoolean(false);

    public StatementImpl(MysqlConnection connection,String database){
        this.connection = connection;
        this.database = database;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            if (sql == null || sql.length()<=0){
                throw new SQLException("sql is not null");
            }
            MysqlConnection locallyScopedConn = this.connection;
            synchronized (locallyScopedConn){
                this.firstSqlChar = sql.charAt(0);
                checkForDml(firstSqlChar);
                String  oldCatalog = null;
                if (!locallyScopedConn.getCatalog().equals(this.database)) {
                    oldCatalog = locallyScopedConn.getCatalog();
                    locallyScopedConn.setCatalog(this.database);
                }
                statementExecuting.set(true);
                results = locallyScopedConn.executeSQL(sql, this.database,this);
                if (oldCatalog!=null){
                    locallyScopedConn.setCatalog(oldCatalog);
                }
                return results;
            }
        }finally {
            statementExecuting.set(false);
        }

    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            if (sql == null || sql.length()<=0){
                throw new SQLException("sql is not null");
            }
            MysqlConnection locallyScopedConn = this.connection;
            synchronized (locallyScopedConn){
                if (locallyScopedConn.isReadOnly()){
                    throw new SQLException("连接是只读的,只能进行查询.");
                }
                String  oldCatalog = null;
                if (!locallyScopedConn.getCatalog().equals(this.database)) {
                    oldCatalog = locallyScopedConn.getCatalog();
                    locallyScopedConn.setCatalog(this.database);
                }

                statementExecuting.set(true);
                results = locallyScopedConn.executeSQL(sql, this.database,this);
                if (oldCatalog!=null){
                    locallyScopedConn.setCatalog(oldCatalog);
                }
                return results.getUpdateCount();
            }
        }finally {
            statementExecuting.set(false);
        }

    }

    @Override
    public void close() throws SQLException {
        realClose(true);
    }

    protected void realClose(boolean closeOpenResults) throws SQLException {
        if (this.isClosed){
            return;
        }
        this.database = null;
        if (closeOpenResults){
            results.close();
        }
        this.isClosed = true;
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (max<0){
            throw new SQLException("max 必须大于0");
        }
        if (max> MysqlDefs.MAX_FIELD_SIZE){
            throw new SQLException("max 必须小于"+MysqlDefs.MAX_FIELD_SIZE);
        }
        this.maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        if (maxRows<0){
            return 0;
        }
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max<0){
            throw new SQLException("max 必须大于0");
        }
        if (max> MysqlDefs.MAX_ROWS){
            throw new SQLException("max 必须小于"+MysqlDefs.MAX_ROWS);
        }
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        //不实现
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.timeoutInMillis/1000;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds<0){
            throw new SQLException("seconds 必须大于0");
        }
        this.timeoutInMillis = seconds * 1000;
    }

    @Override
    public void cancel() throws SQLException {
        //不实现
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        //不实现
    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            if (sql == null || sql.length()<=0){
                throw new SQLException("sql is not null");
            }
            MysqlConnection locallyScopedConn = this.connection;
            synchronized (locallyScopedConn){
                String  oldCatalog = null;
                if (!locallyScopedConn.getCatalog().equals(this.database)) {
                    oldCatalog = locallyScopedConn.getCatalog();
                    locallyScopedConn.setCatalog(this.database);
                }
                statementExecuting.set(true);
                results = locallyScopedConn.executeSQL(sql, this.database,this);
                if (oldCatalog!=null){
                    locallyScopedConn.setCatalog(oldCatalog);
                }
                if (results!=null){
                    return true;
                }
            }
        }finally {
            statementExecuting.set(false);
        }
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.results;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (this.results == null) {
            return -1;
        }
        return results.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        //不支持返回多个results,不支持多条语句发送.
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
       //
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return java.sql.ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows<0 || rows>this.maxRows){
            throw new SQLException("rows 必须大于0,小于"+maxRows);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        //不实现批量
    }

    @Override
    public void clearBatch() throws SQLException {
        //不实现批量
    }

    @Override
    public int[] executeBatch() throws SQLException {
        //不实现批量
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        if (!this.retrieveGeneratedKeys){
            throw new SQLException("获取主键标识没有开启");
        }
        if (results!=null){
            int begin = results.getInsertId();
            int numKey = results.getUpdateCount();
            ArrayList<ResultSetRow> arrayRows = new ArrayList<>(numKey);
            //不考虑主键为 负数的情况
            for (int i=0;i<numKey;i++){
                byte[][] row = new byte[1][];
                row[0] = StringUtils.getBytes(Long.toString(begin));
                arrayRows.add(new ByteArrayRow(row));
                begin+=this.connection.getAutoIncrementIncrement();
            }
            Field[] fields = new Field[1];
            fields[0] = new Field(this.connection,"", "GENERATED_KEY", Types.BIGINT, 17);
            return new ResultSetImpl(fields,new RowDataStatic(arrayRows),this.connection,resultSetType,resultSetConcurrency,false);
        }
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS){
            this.retrieveGeneratedKeys =true;
        }
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes!=null){
            this.retrieveGeneratedKeys =true;
        }
        return this.executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        if (columnNames!=null){
            this.retrieveGeneratedKeys =true;
        }
        return this.executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys == java.sql.Statement.RETURN_GENERATED_KEYS){
            this.retrieveGeneratedKeys =true;
        }
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes!=null){
            this.retrieveGeneratedKeys =true;
        }
        return this.execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        if (columnNames!=null){
            this.retrieveGeneratedKeys =true;
        }
        return this.execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        //不实现
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        //不实现
    }

    @Override
    public boolean isPoolable() throws SQLException {
        //不实现
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        //不实现
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        //不实现
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        //不实现
        return false;
    }

    public void setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
    }

    public void setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
    }

    protected void checkForDml(char firstStatementChar)
            throws SQLException {
        if ((firstStatementChar == 'I') || (firstStatementChar == 'U')
                || (firstStatementChar == 'D') || (firstStatementChar == 'A')
                || (firstStatementChar == 'C')) {
            throw new SQLException("当前execute只支持查询");
        }
    }
}
