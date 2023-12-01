package com.mysql.jdbc.core;

import com.mysql.jdbc.core.config.PropertyDefinitions;
import com.mysql.jdbc.core.config.PropertyDefinition;
import com.mysql.jdbc.core.config.PropertyKey;
import com.mysql.jdbc.core.config.TransIsolationName;
import com.mysql.jdbc.core.util.LRUCache;
import com.mysql.jdbc.core.util.StringUtils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @author hjx
 */
public class MysqlConnectionImpl implements MysqlConnection {

    //mysql io命令处理
    private transient MysqlIO io = null;

    //一些配置属性
    private Properties props = null;

    //host
    private String host = "localhost";

    //port
    private Integer port = 3306;

    //连接的url
    private String url = null;

    //连接的数据库
    private String database;

    //创建此连接的时间戳
    private long connectionCreationTimeMillis = 0;

    //连接的用户名
    String user;

    //密码
    String password;

    /** 连接返回的线程id */
    private long connectionId;

    /** 连接是否已经关闭 */
    private boolean isClosed;

    // 是否自动提交事务
    private boolean autoCommit = true;

    //事务隔离级别
    private int isolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    //是否支持事务隔离
    private boolean hasIsolationLevels = false;

    //当前连接是否只读
    private boolean readOnly = false;

    //是否使用服务器的预声明语句
    private boolean useServerPreparedStmts;

    //预编译缓存大小
    private int preparedStatementCacheSize;

    //是否缓存预编译语句
    private boolean cachePreparedStatements;

    //预编译的sql缓存长度
    private int preparedStatementCacheSqlLimit;

    //创建socket工厂实例,
    String socketFactoryClassName;

    //缓存预编译的参数
    private Map cachedPreparedStatementParams;

    //mysql服务端的预编译校验语句是否能够缓存
    private LRUCache serverSideStatementCheckCache;

    //mysql服务端的预编译缓存
    private LRUCache serverSideStatementCache;
    //是否使用编码
    private boolean useUnicode = false;

    //编码
    private String characterEncoding = null;


    /** ResultSet  */
    //默认只能向前
    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    //默认只读,不能修改和更新
    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;



    public void setSocketFactoryClassName(String socketFactoryClassName) {
        this.socketFactoryClassName = socketFactoryClassName;
    }

    //socket超时时间
    Integer socketTimout = Integer.MAX_VALUE;

    public void setSocketTimout(Integer socketTimout) {
        this.socketTimout = socketTimout;
    }

    private int autoIncrementIncrement = 0;

    //初始化一些服务端的信息
    private Map<String,String> serverVariables = new HashMap<>();

    List<StatementInterceptor> statementInterceptorList = null;

    public MysqlConnectionImpl(){}

    /**
     * 创建一个连接
     * @param host ip
     * @param port 端口
     * @param database 数据库
     * @param url 连接url
     * @return 一个连接
     */
    public MysqlConnectionImpl(String host, Integer port, String database, String url, Properties info) throws SQLException {
        //实现4.1协议的connection连接.
        this.props = info;
        this.host = host;
        this.port = port;
        this.url = url;
        this.database = database;
        this.connectionCreationTimeMillis = System.currentTimeMillis();
        this.user = info.getProperty(Driver.USER_PROPERTY_KEY);
        this.password = info.getProperty(Driver.PASSWORD_PROPERTY_KEY);
        if ((this.user == null) || this.user.equals("")) {
            this.user = "";
        }
        if (this.password == null) {
            this.password = "";
        }
        initializeProperties(info);
        this.preparedStatementCacheSize = PropertyDefinitions.getIntegerPropertyValue(PropertyKey.preparedStatementCacheSize.name());
        this.useServerPreparedStmts= PropertyDefinitions.getBooleanPropertyValue(PropertyKey.useServerPreparedStmts.name());
        this.cachePreparedStatements= PropertyDefinitions.getBooleanPropertyValue(PropertyKey.cachePreparedStatements.name());
        this.preparedStatementCacheSqlLimit = PropertyDefinitions.getIntegerPropertyValue(PropertyKey.preparedStatementCacheSqlLimit.name());
        this.socketFactoryClassName = PropertyDefinitions.getStringPropertyValue(PropertyKey.socketFactoryClassName.name());
        this.useUnicode = PropertyDefinitions.getBooleanPropertyValue(PropertyKey.useUnicode.name());
        this.characterEncoding = PropertyDefinitions.getStringPropertyValue(PropertyKey.characterEncoding.name());
        if (this.cachePreparedStatements){
            initializeCache();
        }
        initializeStatementInterceptors();
        this.io = new MysqlIO(host,port,this,info,socketFactoryClassName,socketTimout);
        this.io.doHandshake(this.user, this.password,
                this.database);
        this.connectionId = this.io.getThreadId();
        this.isClosed = false;
        //从服务端初始化一些信息
        initializePropsFromServer();

        this.io.setStatementInterceptors(statementInterceptorList);
    }

    /**
     * 初始化拦截器
     */
    private void initializeStatementInterceptors() throws SQLException {
        String interceptors =  PropertyDefinitions.getStringPropertyValue(PropertyKey.statementInterceptors.name());
        if (interceptors == null || interceptors.equals("")){
            statementInterceptorList = new ArrayList<>();
            return;
        }
        String[] interceptorList =  interceptors.split(",");
        statementInterceptorList =  new ArrayList<>(interceptorList.length);
        try {
            for (String className:interceptorList) {
                StatementInterceptor statementInterceptorInstance = (StatementInterceptor) Class.forName(
                        className).newInstance();
                statementInterceptorInstance.init(this, props);
                statementInterceptorList.add(statementInterceptorInstance);
            }
        }catch (Exception e){
            e.printStackTrace();
            throw new SQLException("initializeSafeStatementInterceptors err.");
        }
    }

    /**
     * 从服务端初始化一些信息
     */
    private void initializePropsFromServer()throws SQLException {
        loadServerVariables();
        if (serverVariables.containsKey("auto_increment_increment")){
            String autoIncrementIncrementValue = serverVariables.getOrDefault("auto_increment_increment","1");
            this.autoIncrementIncrement = Integer.parseInt(autoIncrementIncrementValue);
        }

    }

    //加载一些需要的服务端信息
    private void loadServerVariables() {
        java.sql.Statement stmt = null;
        java.sql.ResultSet results = null;
        try {
            stmt = this.createStatement();
            results =  stmt.executeQuery("SELECT @@session.auto_increment_increment");
            if (results.next()) {
                serverVariables.put("auto_increment_increment",results.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException sqlE) {
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlE) {
                }
            }
        }
    }

    /**
     * 初始化缓存
     */
    private void initializeCache() {
        cachedPreparedStatementParams = new HashMap(preparedStatementCacheSize);
        if (useServerPreparedStmts){
            this.serverSideStatementCheckCache = new LRUCache(preparedStatementCacheSize);
            this.serverSideStatementCache = new LRUCache(preparedStatementCacheSize) {
                protected boolean removeEldestEntry(java.util.Map.Entry eldest) {
                    if (this.maxElements <= 1) {
                        return false;
                    }
                    boolean removeIt = super.removeEldestEntry(eldest);
                    if (removeIt) {
                        ServerPreparedStatement ps =
                                (ServerPreparedStatement)eldest.getValue();
                        ps.isCached = false;
                        ps.setClosed(false);
                        try {
                            ps.close();
                        } catch (SQLException sqlEx) {
                            sqlEx.printStackTrace();
                        }
                    }
                    return removeIt;
                }
            };
        }
    }

    /**
     * 初始化属性配置
     * @param info
     */
    private void initializeProperties(Properties info) {
        Properties infoCopy = (Properties) info.clone();
        for (String propKey : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.keySet()) {
            try {
                PropertyDefinition propToSet = PropertyDefinitions.getPropertyDefinition(propKey);
                propToSet.initializeFrom(infoCopy);
            } catch (Exception e) {
               e.printStackTrace();
            }
        }
        for (Object key :infoCopy.keySet()) {
            String val = infoCopy.getProperty((String) key);
            PropertyDefinition def = new PropertyDefinition((String) key, val, "");
            PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.put((String) key,def);
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        // 存储过程语句不实现
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        //不实现,就是去掉sql中的{},以及 '\'转义字符等
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.autoCommit == autoCommit) {
            return;
        }
        String sql = autoCommit ? "set autocommit=1" : "set autocommit=0";
        this.io.sendCommand(MysqlDefs.COM_QUERY,sql,60,null,false);
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        String sql = "commit";
        this.io.sendCommand(MysqlDefs.COM_QUERY,sql,60,null,false);
    }

    @Override
    public void rollback() throws SQLException {
        String sql = "rollback";
        this.io.sendCommand(MysqlDefs.COM_QUERY,sql,60,null,false);
    }

    @Override
    public void close() throws SQLException {
        realClose();
    }

    /**
     * 关闭连接以及释放一些资源
     */
    public void realClose() throws SQLException{
        if (this.isClosed()) {
            return;
        }
        try {
            //事务是手动提交,则回滚当前的sql语句
            if (!getAutoCommit()){
                rollback();
            }
            if (this.statementInterceptorList!=null){
                for (StatementInterceptor statementInterceptor:statementInterceptorList) {
                    statementInterceptor.destroy();
                }
            }
            if (this.io != null) {
                try {
                    this.io.quit();
                } catch (Exception e) {
                }
            }
        }finally {
            this.io = null;
            this.isClosed = true;
        }
    }

    public void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("connection is close");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        //记录整个连接的所有元数据, 不实现
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.checkClosed();
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        if (catalog == null){
            throw new SQLException("catalog is not null ");
        }
        if (this.database.equals(catalog)) {
            return;
        }
        this.executeSQL("USE " + catalog,this.database,null);
        this.database = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.database;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (hasIsolationLevels){
           if (level==this.isolationLevel){
               return;
           }
           String sql;
            switch (level) {
                case Connection.TRANSACTION_NONE:
                    throw new SQLException("不支持事务");

                case Connection.TRANSACTION_READ_COMMITTED:
                    sql = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
                    break;

                case Connection.TRANSACTION_READ_UNCOMMITTED:
                    sql = "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";
                    break;

                case Connection.TRANSACTION_REPEATABLE_READ:
                    sql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ";
                    break;

                case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                    sql = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE";
                    break;

                default:
                    throw new SQLException("不支持"+level+"级别的事务");
            }
            executeSQL(sql, this.database,null);
            this.isolationLevel = level;
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        Statement statement = null;
        ResultSet rs = null;
        try {
            if (hasIsolationLevels){
                statement = createStatement();
                String sql = "select @@tx_isolation";
                rs = statement.executeQuery(sql);
                if (rs.next()) {
                    String s = rs.getString(1);
                    if (s != null) {
                        Integer value = TransIsolationName.getValue(s);
                        if (value != null)
                            return value;
                    }
                }
            }
        }finally {
            if (statement!=null){
                statement.close();
            }
            if (rs!=null){
                rs.close();
            }
        }
        return this.isolationLevel;
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        StatementImpl statement = new StatementImpl(this,this.database);
        statement.setResultSetType(resultSetType);
        statement.setResultSetConcurrency(resultSetConcurrency);
        return statement;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        PreparedStatement pStmt = null;
        boolean canServerPrepare = true;
        if (useServerPreparedStmts){
            canServerPrepare = canHandleAsServerPreparedStatement(sql);
        }
        //使用服务端 的预编译
        if (this.useServerPreparedStmts && canServerPrepare) {
            if (cachePreparedStatements) {
                synchronized (this.serverSideStatementCache) {
                    pStmt = (com.mysql.jdbc.core.ServerPreparedStatement)this.serverSideStatementCache.remove(sql);
                    if (pStmt != null) {
                        ((com.mysql.jdbc.core.ServerPreparedStatement)pStmt).setClosed(false);
                    }
                    if (pStmt == null) {
                        try {
                            pStmt = new ServerPreparedStatement(this, sql,
                                    this.database, resultSetType, resultSetConcurrency);
                            if (sql.length() < preparedStatementCacheSqlLimit) {
                                ((com.mysql.jdbc.core.ServerPreparedStatement)pStmt).isCached = true;
                                this.serverSideStatementCache.put(sql,(com.mysql.jdbc.core.ServerPreparedStatement)pStmt);
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                            throw new SQLException("prepareStatement error");
                        }
                    }
                }
            }else {
                pStmt = new ServerPreparedStatement(this, sql,
                        this.database, resultSetType, resultSetConcurrency);
            }
        }else {
            pStmt = clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
        }
        return pStmt;
    }

    /**
     * 客户端预编译,
     * @param sql sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     */
    private PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
        PreparedStatementImpl pStmt = null;
        if (cachePreparedStatements){
            synchronized (this.cachedPreparedStatementParams) {
                PreparedStatementImpl.ParseInfo info = (PreparedStatementImpl.ParseInfo) this.cachedPreparedStatementParams.get(sql);
                if (info == null){
                    pStmt = new PreparedStatementImpl(this,sql,database);
                    PreparedStatementImpl.ParseInfo parseInfo =  pStmt.getParseInfo();
                    if (parseInfo.statementLength<preparedStatementCacheSqlLimit){
                         if (this.cachedPreparedStatementParams.size()>= preparedStatementCacheSize){
                             Iterator oldestIter = this.cachedPreparedStatementParams
                                     .keySet().iterator();
                             long lruTime = Long.MAX_VALUE;
                             String oldestSql = null;
                             while (oldestIter.hasNext()) {
                                 String sqlKey = (String) oldestIter.next();
                                 PreparedStatementImpl.ParseInfo lruInfo = (PreparedStatementImpl.ParseInfo) this.cachedPreparedStatementParams
                                         .get(sqlKey);
                                 if (lruInfo.lastUsed < lruTime) {
                                     lruTime = lruInfo.lastUsed;
                                     oldestSql = sqlKey;
                                 }
                             }
                             if (oldestSql != null) {
                                 this.cachedPreparedStatementParams
                                         .remove(oldestSql);
                             }
                         }
                         this.cachedPreparedStatementParams.put(sql, pStmt.getParseInfo());
                    }
                }else {
                    info.lastUsed = System.currentTimeMillis();
                    pStmt = new PreparedStatementImpl(this,sql,database,info);
                }
            }
        }else {
            pStmt = new PreparedStatementImpl(this,sql,database);
        }
        pStmt.setResultSetType(resultSetType);
        pStmt.setResultSetConcurrency(resultSetConcurrency);
        return pStmt;
    }


    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        // 存储过程语句不实现
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        //不实现
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
           //不实现
    }

    @Override
    public int getHoldability() throws SQLException {
        //不实现
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        //不实现
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        //不实现
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        //不实现
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        //不实现
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
       return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql,resultSetType,resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        //存储过程不实现
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        // autoGeneratedKeys 自动生成主键.
        PreparedStatement pStmt = prepareStatement(sql);
        ((com.mysql.jdbc.core.PreparedStatementImpl)pStmt).setRetrieveGeneratedKeys(autoGeneratedKeys == java.sql.Statement.RETURN_GENERATED_KEYS);
        return pStmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement pStmt = prepareStatement(sql);
        ((com.mysql.jdbc.core.PreparedStatementImpl)pStmt).setRetrieveGeneratedKeys(columnIndexes!=null);
        return pStmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        PreparedStatement pStmt = prepareStatement(sql);
        ((com.mysql.jdbc.core.PreparedStatementImpl)pStmt).setRetrieveGeneratedKeys(columnNames!=null);
        return pStmt;
    }

    @Override
    public Clob createClob() throws SQLException {
        //不实现 创建 Clob 类型数据
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        //不实现 创建 Blob 类型数据
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        //不实现 创建 NClob 类型数据
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        //不实现 创建 SQLXML 类型数据
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // 不实现
        return false;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // 不实现
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // 不实现
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        // 不实现
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        // 不实现
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        // 不实现
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // 不实现
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        // 不实现

    }

    @Override
    public String getSchema() throws SQLException {
        // 不实现
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        // 不实现
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // 不实现
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        // 不实现
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        // 不实现
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // 不实现
        return false;
    }

    /**
     * 是否使用mysql服务端预编译
     * @param sql sql key
     * @return true or false
     */
    private boolean canHandleAsServerPreparedStatement(String sql) {
        if (sql == null || sql.length() == 0) {
            return true;
        }
        if (!this.useServerPreparedStmts) {
            return false;
        }
        if (cachePreparedStatements){
            synchronized (this.serverSideStatementCheckCache) {
                Boolean flag = (Boolean)this.serverSideStatementCheckCache.get(sql);
                if (flag != null) {
                    return flag.booleanValue();
                }
                boolean canHandle = canHandleAsServerPreparedStatementNoCache(sql);
                if (sql.length()< preparedStatementCacheSqlLimit){
                    this.serverSideStatementCheckCache.put(sql,
                            canHandle ? Boolean.TRUE : Boolean.FALSE);
                }
                return canHandle;
            }
        }
        return canHandleAsServerPreparedStatementNoCache(sql);
    }

    /**
     * 判断语句是否能够缓存.
     * @param sql sql语句
     * @return
     */
    private boolean canHandleAsServerPreparedStatementNoCache(String sql) {
        //call 命令行不能使用预编译
        if (StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(sql,"CALL")){
            return false;
        }
        boolean canHandleAsStatement = true;
        if (StringUtils.startsWithIgnoreCaseAndWs(sql, "CREATE TABLE") ||
                StringUtils.startsWithIgnoreCaseAndWs(sql, "DO") ||
                StringUtils.startsWithIgnoreCaseAndWs(sql, "SET")){
                canHandleAsStatement = false;
        }
        return canHandleAsStatement;
    }

    @Override
    public MysqlIO getIO() {
        return this.io;
    }

    @Override
    public void reCachePreparedStatement(ServerPreparedStatement serverPreparedStatement) {
        synchronized (this.serverSideStatementCache) {
            this.serverSideStatementCache.put(serverPreparedStatement.originalSql, serverPreparedStatement);
        }
    }

    @Override
    public MysqlResultSet executeSQL(String sql, String database,Statement statement) throws SQLException {
        return this.io.queryDirect(sql,database,statement,DEFAULT_RESULT_SET_TYPE,DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public String getCharsetName() {
        if (useUnicode){
            return this.characterEncoding;
        }
        return null;
    }

    @Override
    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

}
