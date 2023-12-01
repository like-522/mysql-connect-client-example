package com.mysql.jdbc.core;

import com.mysql.jdbc.core.exception.ErrorPacketException;
import com.mysql.jdbc.core.exception.PacketTooBigException;
import com.mysql.jdbc.core.util.SecurityUtil;
import com.mysql.jdbc.core.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @author hjx
 */
public class MysqlIO {

    //基础包的4个字节,前3个字节代表payload,后面一个字节代表包序号 readPacketSequence
    protected static final int HEADER_LENGTH = 4;

    //包发送序号
    private byte packetSequence = 0;

    //包读取序号
    private byte readPacketSequence = -1;

    //最大的3个字节.
    protected int maxThreeBytes = 255 * 255 * 255;

    protected static final int INITIAL_PACKET_SIZE = 1024;
    //utf-8编码
    private static final int UTF8_CHARSET_INDEX = 33;

    // 4.1.1协议其他的字节数长度.
    protected static final int AUTH_411_OVERHEAD = 32;

    //最后一个包发送的时间戳
    protected long lastPacketSentTimeMs = 0;

    //最后一个包 接收的时间戳
    protected long lastPacketReceivedTimeMs = 0;

    //connection实例
    protected MysqlConnection connection;

    //连接mysql 端口号
    protected int port = 3306;

    //连接的mysql host
    protected String host;

    //socket输入流
    protected InputStream mysqlInput = null;

    //socket连接
    protected Socket mysqlConnection = null;

    //socket输出流
    protected OutputStream mysqlOutput = null;

    //要创建的socket工厂名称
    private String socketFactoryClassName;

    //socket工厂示例
    private SocketFactory socketFactory;

    //共享的 sharedSendPacket
    private SqlBuffer sharedSendPacket = null;

    //包头字节
    private byte[] packetHeaderBuf = new byte[4];


    //协议版本号
    private byte protocolVersion = 0;

    //服务版本号
    private String serverVersion = null;

    //服务端的线程id
    private long threadId;

    //挑战随机数,，用于数据库验证(后面还跟着一个结束符填充值0x00)
    protected String seed;

    // 服务器功能选项 server_capabilities,两个字节
    protected int serverCapabilities;

    // 字节编码 1个字节
    private int serverCharsetNumber;

    // 服务器状态 2个字节
    private int serverStatus;

    // 警告数量
    private int warningCount;

    //客户端需要的功能标识
    protected long clientParam = 0;

    String ASCII_ENCODING = "ASCII";

    private int maxAllowedPacket = 1024 * 1024;

    List<StatementInterceptor> statementInterceptorList;

    //client  Capabilities start 客户端和mysql服务协议,使用的一些功能标识

    //客户端连接使用密码校验.
    int CLIENT_LONG_PASSWORD = 1;

    // Found instead of affected rows
    // 返回找到（匹配）的行数，而不是改变了的行数。
    int CLIENT_FOUND_ROWS = 2;

    // Get all column flags
    //获取所有的列标识
    int CLIENT_LONG_FLAG = 4;

    // One can specify db on connect
    //客户端连接使用db
    int CLIENT_CONNECT_WITH_DB = 8;

    // Don't allow database.table.column
    // 不允许“数据库名.表名.列名”这样的语法。这是对于ODBC的设置。
    // 当使用这样的语法时解析器会产生一个错误，这对于一些ODBC的程序限制bug来说是有用的。
    int CLIENT_NO_SCHEMA = 16;

    // 使用压缩协议
    int CLIENT_COMPRESS = 32;

    // Odbc client
    int CLIENT_ODBC = 64;

    // Can use LOAD DATA LOCAL
    //能够使用本地加载数据
    int CLIENT_LOCAL_FILES = 128;

    // Ignore spaces before '('
    // 允许在函数名后使用空格。所有函数名可以预留字。
    int CLIENT_IGNORE_SPACE = 256;

    // 新的 4.1 协议 这是一个交互式客户端
    int CLIENT_PROTOCOL_41 = 512;

    // 允许使用关闭连接之前的不活动交互超时的描述，而不是等待超时秒数。
    // 客户端的会话等待超时变量变为交互超时变量。
    int CLIENT_INTERACTIVE = 1024;

    // Switch to SSL after handshake
    // 使用SSL。这个设置不应该被应用程序设置，他应该是在客户端库内部是设置的。
    // 可以在调用mysql_real_connect()之前调用mysql_ssl_set()来代替设置。
    int CLIENT_SSL = 2048;

    // IGNORE sigpipes
    // 阻止客户端库安装一个SIGPIPE信号处理器。
    // 这个可以用于当应用程序已经安装该处理器的时候避免与其发生冲突。
    int CLIENT_IGNORE_SIGPIPE = 4096;

    // Client knows about transactions
    //客户端事务
    int CLIENT_TRANSACTIONS = 8192;

    // Old flag for 4.1 protocol
    //老的4.1协议不使用
    int CLIENT_RESERVED = 16384;

    // New 4.1 authentication
    //新的4.1协议验证
    int CLIENT_SECURE_CONNECTION = 32768;

    // Enable/disable multi-stmt support
    // 通知服务器客户端可以发送多条语句（由分号分隔）。如果该标志为没有被设置，多条语句执行。
    int CLIENT_MULTI_STATEMENTS = 65536;

    // Enable/disable multi-results
    // 通知服务器客户端可以处理由多语句或者存储过程执行生成的多结果集。
    // 当打开CLIENT_MULTI_STATEMENTS时，这个标志自动的被打开。
    int CLIENT_MULTI_RESULTS = 131072;
    //client  Capabilities end

    public MysqlIO(String host, Integer port, MysqlConnection connection, Properties info, String socketFactoryName, int socketTimout) throws SQLException {
        this.connection = connection;
        this.host = host;
        this.port = port;
        this.socketFactoryClassName = socketFactoryName;
        this.socketFactory = createSocketFactory();
        try {
            this.mysqlConnection = socketFactory.connect(host, port, info);
            if (socketTimout != 0) {
                try {
                    this.mysqlConnection.setSoTimeout(socketTimout);
                } catch (Exception ex) {
                    // 忽略当前 异常
                }
            }
            this.mysqlInput = new BufferedInputStream(this.mysqlConnection.getInputStream(), 16384);
            this.mysqlOutput = new BufferedOutputStream(this.mysqlConnection.getOutputStream(), 16384);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 创建socket创建示例
     *
     * @return
     * @throws SQLException
     */
    private SocketFactory createSocketFactory() throws SQLException {
        if (this.socketFactoryClassName == null) {
            throw new SQLException("socketFactoryClassName is null");
        }
        try {
            return (SocketFactory) (Class.forName(this.socketFactoryClassName)
                    .newInstance());
        } catch (Exception e) {
            throw new SQLException("socketFactory crate err..");
        }

    }

    /**
     * 进行握手协议校验
     *
     * @param user     用户名
     * @param password 密码
     * @param database 需要连接的数据库.
     */
    public void doHandshake(String user, String password, String database) throws SQLException {
        //读取第一个包
        SqlBuffer buffer = readPacket();
        //处理握手协议.
        this.protocolVersion = buffer.readByte();
        this.serverVersion = buffer.readStringWithNull(ASCII_ENCODING);
        this.threadId = buffer.readLong();
        this.seed = buffer.readStringWithNull(ASCII_ENCODING);
        this.serverCapabilities = buffer.readInt();

        //4.1新协议
        if (protocolVersion > 9 && (this.serverCapabilities & CLIENT_PROTOCOL_41) != 0) {
            int position = buffer.getPosition();
            this.serverCharsetNumber = buffer.readByte() & 0xff;
            this.serverStatus = buffer.readInt();
            //新的协议有16个字节来描述服务器的特征,跳过
            buffer.setPosition(position + 16);

            // 挑战随机数用于数据库验证（后面跟着结束符填充值0x00）
            String seed2 = buffer.readStringWithNull(ASCII_ENCODING);
            seed = seed + seed2;
        }else {
            throw new SQLException("protocolVersion is not support...");
        }

        //响应 protocol::HandshakeResponse
        secureAuth411(user, password, database);
    }

    /**
     * 响应4.1.1协议
     *
     * @param user     用户名
     * @param password 密码
     * @param database 要连接的数据库
     */
    private void secureAuth411(String user, String password, String database) throws SQLException {
        int passwordLength = (password != null) ? password.length() : 0;
        int userLength = (user != null) ? user.length() : 0;
        int databaseLength = (database != null) ? database.length() : 0;
        int packLength = ((userLength + passwordLength + databaseLength) * 2) + HEADER_LENGTH + AUTH_411_OVERHEAD;
        SqlBuffer packet = new SqlBuffer(packLength);
        clientParam = getClientCapabilities();
        packet.writeLong(clientParam);
        packet.writeLong(maxThreeBytes);
        //字符集，JDBC 将连接为 'utf8'
        packet.writeByte((byte) UTF8_CHARSET_INDEX);
        // 保留供将来使用的字节集
        packet.writeBytesNoNull(new byte[23]);
        if (user != null) {
            packet.writeStringWithNull(user, StandardCharsets.UTF_8.name());
        } else {
            packet.writeByte((byte) 0);
        }
        if (password != null) {
            try {
                packet.writeBytesLength(SecurityUtil.scramble411(password, seed, ASCII_ENCODING));
            } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new SQLException("unable handler password ...");
            }
        } else {
            packet.writeByte((byte) 0);
        }
        if (database != null) {
            packet.writeStringWithNull(database, StandardCharsets.UTF_8.name());
        } else {
            packet.writeByte((byte) 0);
        }
        sendResponsePacket(packet, packet.getPosition());

        //接收响应的包是否是异常包
        SqlBuffer replayPacket = checkErrorPacket();
        //如果是EOF包,使用323格式校验密码
        if (replayPacket.isLastDataPacket()) {
            packet.clear();
            packet.writeStringWithNull(Objects.requireNonNull(SecurityUtil.newCrypt(password, seed)), StandardCharsets.UTF_8.name());
            sendResponsePacket(packet, packet.getPosition());
            checkErrorPacket();
        }
    }

    /**
     * 校验响应的包是否是异常包
     *
     * @return
     */
    public SqlBuffer checkErrorPacket() throws SQLException {
        SqlBuffer replayPacket = readPacket();
        int statsCode = replayPacket.readByte();
        if (statsCode == (byte) 0xff) {
            int errorCode = replayPacket.readInt();
            String serverErrorMessage = replayPacket.readStringWithNull(StandardCharsets.UTF_8.name());
            if (serverErrorMessage.charAt(0) == '#') {
                if (serverErrorMessage.length() > 6) {
                    String sqlState = serverErrorMessage.substring(0, 6);
                    serverErrorMessage = serverErrorMessage.substring(6);
                    throw new ErrorPacketException(errorCode, sqlState, serverErrorMessage);
                }
            } else {
                throw new ErrorPacketException(errorCode, serverErrorMessage);
            }
        }
        //如果第一个字节不是异常,复位
        replayPacket.setPosition(0);
        return replayPacket;
    }

    /**
     * 发送响应包
     *
     * @param packet    包
     * @param packetLen 包的长度
     */
    private void sendResponsePacket(SqlBuffer packet, int packetLen) throws SQLException {
        try {
            if (maxAllowedPacket > 0 && packetLen > maxAllowedPacket) {
                throw new PacketTooBigException(packetLen, maxAllowedPacket);
            }
            this.packetSequence++;
            lastPacketSentTimeMs = System.currentTimeMillis();
            packet.setPosition(0);
            packet.writeLongInt(packetLen - HEADER_LENGTH);
            packet.writeByte(this.packetSequence);
            this.mysqlOutput.write(packet.getByteBuffer(), 0,
                    packetLen);
            this.mysqlOutput.flush();
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("responsePacket send error...");
        }

    }

    /**
     * 读取mysql服务发送的包
     *
     * @return buffer
     * @throws SQLException
     */
    public SqlBuffer readPacket() throws SQLException {
        try {
            int lengthRead = readFully(this.mysqlInput,
                    this.packetHeaderBuf, 0, 4);
            if (lengthRead < 4) {
                forceClose();
                throw new IOException("包header字节错误");
            }
            //前3个字节代表包payload的字节数.
            int packetLength = (this.packetHeaderBuf[0] & 0xff) +
                    ((this.packetHeaderBuf[1] & 0xff) << 8) +
                    ((this.packetHeaderBuf[2] & 0xff) << 16);
            //读取序号
            readPacketSequence = packetHeaderBuf[3];
            lastPacketReceivedTimeMs = System.currentTimeMillis();

            byte[] buffer = new byte[packetLength];
            int numBytesRead = readFully(this.mysqlInput, buffer, 0,
                    packetLength);
            if (numBytesRead != packetLength) {
                throw new IOException("期望读取" +
                        packetLength + " 字节, 实际读取 " + numBytesRead);
            }
            return new SqlBuffer(buffer);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException("readPacket error.");
        }
    }

    /**
     * 客户端使用的功能
     *
     * @return
     */
    private long getClientCapabilities() {
        int capabilities = 0;
        capabilities |= CLIENT_LONG_PASSWORD;
        capabilities |= CLIENT_FOUND_ROWS;
        capabilities |= CLIENT_LONG_FLAG;
        capabilities |= CLIENT_CONNECT_WITH_DB;
        capabilities |= CLIENT_ODBC;
        capabilities |= CLIENT_LOCAL_FILES;
        capabilities |= CLIENT_IGNORE_SPACE;
        capabilities |= CLIENT_PROTOCOL_41;
        capabilities |= CLIENT_INTERACTIVE;
        capabilities |= CLIENT_IGNORE_SIGPIPE;
        capabilities |= CLIENT_TRANSACTIONS;
        capabilities |= CLIENT_SECURE_CONNECTION;

        return capabilities;
    }

    /**
     * 读取socket字节
     *
     * @param in  socket输入流
     * @param b   读入的字节数组
     * @param off 读取起始位置
     * @param len 读取结束位置
     * @return 读取的字节数
     * @throws IOException
     */
    private final int readFully(InputStream in, byte[] b, int off, int len)
            throws IOException {
        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException("MysqlIO.EOF");
            }
            n += count;
        }
        return n;
    }

    //mysql返回的线程id
    protected long getThreadId() {
        return this.threadId;
    }

    /**
     * 强制关闭底层的socket连接.
     */
    protected final void forceClose() {
        try {
            try {
                if (this.mysqlInput != null) {
                    this.mysqlInput.close();
                }
            } finally {
                if (this.mysqlConnection != null && !this.mysqlConnection.isClosed() && !this.mysqlConnection.isInputShutdown()) {
                    try {
                        this.mysqlConnection.shutdownInput();
                    } catch (UnsupportedOperationException ex) {
                        // ignore, some sockets do not support this method
                    }
                }
            }
        } catch (IOException ioEx) {
            this.mysqlInput = null;
        }
        try {
            try {
                if (this.mysqlOutput != null) {
                    this.mysqlOutput.close();
                }
            } finally {
                if (this.mysqlConnection != null && !this.mysqlConnection.isClosed() && !this.mysqlConnection.isOutputShutdown()) {
                    try {
                        this.mysqlConnection.shutdownOutput();
                    } catch (UnsupportedOperationException ex) {
                        // ignore, some sockets do not support this method
                    }
                }
            }
        } catch (IOException ioEx) {
            this.mysqlOutput = null;
        }
        try {
            if (this.mysqlConnection != null) {
                this.mysqlConnection.close();
            }
        } catch (IOException ioEx) {
            this.mysqlConnection = null;
        }
    }

    /**
     * 发送命令行给mysql
     *
     * @param command       mysql协议命令
     * @param sql           需要发送的sql
     * @param timeoutMillis 超时时间
     * @param packet 发送的包
     * @param skipCheck 是否跳过校验包异常,以及不需要返回的包数据
     * @return 返回的包
     */
    public SqlBuffer sendCommand(int command, String sql, int timeoutMillis, SqlBuffer packet,boolean skipCheck) throws SQLException {
        int oldTimeout = 0;
        if (timeoutMillis != 0) {
            try {
                oldTimeout = this.mysqlConnection.getSoTimeout();
                timeoutMillis = oldTimeout;
                this.mysqlConnection.setSoTimeout(timeoutMillis);
            } catch (SocketException e) {
                e.printStackTrace();
                throw new SQLException("socket timeout set error......");
            }
        }
        try {
            clearInputStream();
            //发送包 要从0开始
            this.packetSequence = -1;
            if (packet == null) {
                int packetLen = HEADER_LENGTH + 1 + (sql == null ? 0 : sql.length());
                SqlBuffer sendPacket = new SqlBuffer(packetLen);
                sendPacket.writeByte((byte) command);
                sendPacket.writeBytesNoNull(sql.getBytes(StandardCharsets.UTF_8.name()));
                this.sendResponsePacket(sendPacket, sendPacket.getPosition());
            } else {
                this.sendResponsePacket(packet, packet.getPosition());
            }
            SqlBuffer returnPacket = null;
            if (!skipCheck){
                returnPacket = checkErrorPacket();
            }
            return returnPacket;
        } catch (SQLException | UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new SQLException("sendCommand error:command=" + command + ",+data=" + sql);
        }
    }

    /**
     * 清除剩下还未读取的字节
     *
     * @throws SQLException
     */
    protected void clearInputStream() throws SQLException {
        try {
            int len = this.mysqlInput.available();

            while (len > 0) {
                this.mysqlInput.skip(len);
                len = this.mysqlInput.available();
            }
        } catch (IOException ioEx) {
            ioEx.printStackTrace();
            throw new SQLException("clearInputStream io IOException");
        }
    }

    /**
     * 解析字段包 Column Definition的包
     *
     * @param packet 包数据
     * @return
     */
    public Field unpackField(SqlBuffer packet) {
        //去除代表长度的字节数
        int catalogLenStart = packet.getPosition();
        int catalogLength = packet.fastSkipLenString();
        catalogLenStart = adjustStartForFieldLength(catalogLenStart, catalogLength);

        int databaseLenStart = packet.getPosition();
        int databaseLength = packet.fastSkipLenString();
        databaseLenStart = adjustStartForFieldLength(databaseLenStart, databaseLength);

        int tableNameLenStart = packet.getPosition();
        int tableNameLength = packet.fastSkipLenString();
        tableNameLenStart = adjustStartForFieldLength(tableNameLenStart, tableNameLength);


        int originalTableNameLenStart = packet.getPosition();
        int originalTableNameLength = packet.fastSkipLenString();
        originalTableNameLenStart = adjustStartForFieldLength(originalTableNameLenStart, originalTableNameLength);

        int fieldNameLenStart = packet.getPosition();
        int fieldNameLenLength = packet.fastSkipLenString();
        fieldNameLenStart = adjustStartForFieldLength(fieldNameLenStart, fieldNameLenLength);

        int originalFieldNameLenStart = packet.getPosition();
        int originalFieldNameLenLength = packet.fastSkipLenString();
        originalFieldNameLenStart = adjustStartForFieldLength(originalFieldNameLenStart, originalFieldNameLenLength);

        packet.readByte();

        int charSetNumber = packet.readInt();
        long colLength = packet.readLong();
        int colType = packet.readByte() & 0xff;
        long colFlag = packet.readInt();

        int decimals = packet.readByte() & 0xff;

        Field field = new Field(this.connection, packet.getByteBuffer(),
                catalogLenStart, catalogLength,
                databaseLenStart, databaseLength, tableNameLenStart,
                tableNameLength, originalTableNameLenStart,
                originalTableNameLength, fieldNameLenStart, fieldNameLenLength,
                originalFieldNameLenStart, originalFieldNameLenLength,
                colLength, colType, colFlag, decimals, charSetNumber);
        return field;
    }

    /**
     * 调整字段的起始长度, 因为协议中有字节是代表data的长度的,需要跳过
     *
     * @param start
     * @param length
     * @return
     */
    private int adjustStartForFieldLength(int start, int length) {
        //用一个字节表示data长度,
        if (length < 251) {
            return start + 1;
        }
        //用2个字节表示data长度,
        if (length >= 251 && length < 65536) {
            return start + 2;
        }
        //用3个字节表示data长度,
        if (length >= 65536 && length < 16777216) {
            return start + 3;
        }
        //用8个字节表示data长度,
        return start + 8;
    }

    /**
     * 读取发送sql命令返回的包转化为 ResultSet
     * @param resultBuffer 返回的包
     * @param isBinaryEncoded 行数是否是二进制编码.
     * @return
     */
    public ResultSetImpl readResultSet(SqlBuffer resultBuffer,boolean isBinaryEncoded,int resultSetType,int resultSetConcurrency) throws SQLException {
        ResultSetImpl resultSet;
        int columnCount = resultBuffer.readLength();
        if (columnCount == 0){
            //新增编辑,删除返回
            resultSet = buildResultSetWithUpdates(resultBuffer,resultSetType,resultSetConcurrency,isBinaryEncoded);
        }else {
            //有多个列定义.查询语句返回
            resultSet = getResultSet(columnCount,isBinaryEncoded,resultSetType,resultSetConcurrency);
        }
        return resultSet;
    }

    /**
     *  构建修改sql的 ResultSetImpl
     * @param resultBuffer 读取的buffer
     * @return ResultSetImpl
     */
    private ResultSetImpl buildResultSetWithUpdates(SqlBuffer resultBuffer,int resultSetType,int resultSetConcurrency,boolean isBinaryEncoded) {
        int updateCount = resultBuffer.readLength();
        int updateId = resultBuffer.readLength();
        this.serverStatus = resultBuffer.readInt();
        this.warningCount = resultBuffer.readInt();
        String serverInfo = resultBuffer.readRestOfPacketString(StandardCharsets.UTF_8.name());
        return new ResultSetImpl(updateCount,updateId,serverInfo, (MysqlConnection) this.connection,resultSetType,resultSetConcurrency,isBinaryEncoded);
    }

    /**
     *  获取行数据
     * @param columnCount 当前列的数量
     * @return
     */
    private ResultSetImpl getResultSet(int columnCount,boolean isBinaryEncoded,int resultSetType,int resultSetConcurrency) throws SQLException {
        try {
            Field[] fields = new Field[columnCount];
            for (int i = 0; i<columnCount;i++){
                SqlBuffer fieldPacket = null;
                fieldPacket = this.readPacket();
                fields[i] = this.unpackField(fieldPacket);
            }
            //校验异常,并跳过eof包
            this.checkErrorPacket();
            RowData rowData = null;
            ArrayList<ResultSetRow> rows = new ArrayList<>();
            while (true){
                SqlBuffer rowPacket = this.checkErrorPacket();
                if (rowPacket.isLastDataPacket()){
                    break;
                }
                ResultSetRow resultSetRow;
                if (isBinaryEncoded){
                    //跳过第一个ok字节
                    rowPacket.setPosition(1);
                    resultSetRow = nextRow(fields,rowPacket);
                }else {
                    resultSetRow = nextTextRow(fields,rowPacket);
                }
                rows.add(resultSetRow);
            }
            rowData = new RowDataStatic(rows);
            return new ResultSetImpl(fields,rowData, (MysqlConnection) this.connection,resultSetType,resultSetConcurrency,isBinaryEncoded);
        }catch (SQLException e){
            e.printStackTrace();
            throw new SQLException("getResultSet err..");
        }
    }

    /**
     * 获取文本行的row数据
     * @param fields
     * @param rowPacket
     * @return
     */
    private ResultSetRow nextTextRow(Field[] fields, SqlBuffer rowPacket) {
        int len = fields.length;
        byte[][] rowData = new byte[len][];
        for (int i= 0;i<len;i++){
            rowData[i] = rowPacket.readLenByteArray(0);
        }
        return new ByteArrayRow(rowData);
    }

    /**
     * 读取一行的数据, 数据包是以二进制表示
     * @param fields 字段值
     * @param rowPacket row字节数据包
     * @return
     */
    private ResultSetRow nextRow(Field[] fields, SqlBuffer rowPacket) throws SQLException {
        int len = fields.length;
        byte[][] rowData = new byte[len][];
        //为列为null值的标记留位置 nullbit
        int nullCount = (len + 9) / 8;
        byte[] nullBitMask = new byte[nullCount];
        for (int i = 0; i < nullCount; i++) {
            nullBitMask[i] = rowPacket.readByte();
        }
        int nullMaskPos = 0;
        //mysql协议文档中说明偏移量2位保留
        int bit = 1<<2;
        for (int i= 0;i<len;i++){
            //位图中位置为0的代表该列有值,不为null
            if ((nullBitMask[nullMaskPos] & bit) == 0){
                extractNativeEncodedColumn(rowPacket, fields, i,
                        rowData);
            }else {
                //位图中位置为1的代表该列的值为null
                rowData[i] = null;
            }
            //每一个字节位代表一个参数的null值.
            //每次向前移动一位.
            if (((bit <<= 1) & 255) == 0) {
                bit = 1; /* 第一个字节位图用完,轮询到第二个字节 */
                nullMaskPos++;
            }
        }
        return new ByteArrayRow(rowData);
    }

    /**
     * 获取不为null的一列的值
     * @param rowPacket row包
     * @param fields 字段
     * @param columnIndex 列位置
     * @param rowData 行字节.
     */
    private final void extractNativeEncodedColumn(SqlBuffer rowPacket, Field[] fields, int columnIndex, byte[][] rowData) throws SQLException {
        Field curField = fields[columnIndex];

        switch (curField.getMysqlType()) {
            case MysqlDefs.FIELD_TYPE_NULL:
                break; // for dummy binds

            case MysqlDefs.FIELD_TYPE_TINY:
                rowData[columnIndex] = new byte[] {rowPacket.readByte()};
                break;

            case MysqlDefs.FIELD_TYPE_SHORT:
            case MysqlDefs.FIELD_TYPE_YEAR:
                rowData[columnIndex] = rowPacket.getBytes(2);
                break;

            case MysqlDefs.FIELD_TYPE_LONG:
            case MysqlDefs.FIELD_TYPE_INT24:
                rowData[columnIndex] = rowPacket.getBytes(4);
                break;

            case MysqlDefs.FIELD_TYPE_LONGLONG:
                rowData[columnIndex] = rowPacket.getBytes(8);
                break;

            case MysqlDefs.FIELD_TYPE_FLOAT:
                rowData[columnIndex] = rowPacket.getBytes(4);
                break;

            case MysqlDefs.FIELD_TYPE_DOUBLE:
                rowData[columnIndex] = rowPacket.getBytes(8);
                break;

            case MysqlDefs.FIELD_TYPE_TIME:
                int length = (int) rowPacket.readLength();
                rowData[columnIndex] = rowPacket.getBytes(length);
                break;

            case MysqlDefs.FIELD_TYPE_DATE:
                length = (int) rowPacket.readLength();
                rowData[columnIndex] = rowPacket.getBytes(length);
                break;

            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                length = (int) rowPacket.readLength();
                rowData[columnIndex] = rowPacket.getBytes(length);
                break;

            case MysqlDefs.FIELD_TYPE_TINY_BLOB:
            case MysqlDefs.FIELD_TYPE_MEDIUM_BLOB:
            case MysqlDefs.FIELD_TYPE_LONG_BLOB:
            case MysqlDefs.FIELD_TYPE_BLOB:
            case MysqlDefs.FIELD_TYPE_VAR_STRING:
            case MysqlDefs.FIELD_TYPE_VARCHAR:
            case MysqlDefs.FIELD_TYPE_STRING:
            case MysqlDefs.FIELD_TYPE_DECIMAL:
            case MysqlDefs.FIELD_TYPE_NEW_DECIMAL:
            case MysqlDefs.FIELD_TYPE_GEOMETRY:
                rowData[columnIndex] = rowPacket.readLenByteArray(0);
                break;

            case MysqlDefs.FIELD_TYPE_BIT:
                rowData[columnIndex] = rowPacket.readLenByteArray(0);
                break;

            default:
                throw new SQLException("未知的mysql数据类型");
        }
    }


    /**
     * 执行 0x03: COM_QUERY 命令的sql
     * @param sql sql
     * @param database
     * @return
     */
    public MysqlResultSet queryDirect(String sql, String database, Statement statement ,int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            if (this.statementInterceptorList != null) {
                MysqlResultSet interceptedResults = invokeStatementInterceptorsPre(sql, statement);
                if (interceptedResults != null) {
                    return interceptedResults;
                }
            }

            SqlBuffer resultBuffer = this.sendCommand(MysqlDefs.COM_QUERY,sql,0,null,false);
            MysqlResultSet originalResultSet =  this.readResultSet(resultBuffer,false, resultSetType,resultSetConcurrency);

            if (this.statementInterceptorList != null) {
                MysqlResultSet interceptedResults = invokeStatementInterceptorsPost(sql, statement,originalResultSet);
                if (interceptedResults != null) {
                    originalResultSet =  interceptedResults;
                }
            }
            return originalResultSet;
        }catch (SQLException e){
            throw new SQLException("QueryDirect err.");
        }
    }

    /**
     * 调用拦截器的post方法
     * @param sql
     * @param statement
     * @param originalResultSet
     * @return
     * @throws SQLException
     */
    public MysqlResultSet invokeStatementInterceptorsPost(String sql, Statement statement, MysqlResultSet originalResultSet) throws SQLException {
        MysqlResultSet resultSet = null;
        for (StatementInterceptor statementInterceptor:statementInterceptorList) {
            MysqlResultSet currentResultSet =  statementInterceptor.postProcess(sql,statement,originalResultSet,this.connection);
            if (currentResultSet!=null){
                resultSet =  currentResultSet;
            }
        }
        return resultSet;
    }

    /**
     * 调用拦截器的pre方法
     * @param sql
     * @param statement
     * @return
     * @throws SQLException
     */
    public MysqlResultSet invokeStatementInterceptorsPre(String sql, Statement statement) throws SQLException {
        MysqlResultSet resultSet = null;
        for (StatementInterceptor statementInterceptor:statementInterceptorList) {
            MysqlResultSet currentResultSet =  statementInterceptor.preProcess(sql,statement,this.connection);
            if (currentResultSet!=null){
                resultSet =  currentResultSet;
            }
        }
        return resultSet;
    }

    /**
     * 共享的发送包,由外部的锁保护数据一致性
     * @return
     */
    public SqlBuffer getSharedSendPacket() {
        if (this.sharedSendPacket == null) {
            this.sharedSendPacket = new SqlBuffer(INITIAL_PACKET_SIZE);
        }
        return this.sharedSendPacket;
    }

    /**
     * 退出当前socket连接
     * @throws SQLException
     */
    final void quit() throws SQLException {
        try {
            try {
                if (!this.mysqlConnection.isClosed()) {
                    try {
                        this.mysqlConnection.shutdownInput();
                    } catch (UnsupportedOperationException ex) {
                    }
                }
            } catch (IOException ioEx) {
                ioEx.printStackTrace();
            }

            SqlBuffer packet = new SqlBuffer(6);
            this.packetSequence = -1;
            packet.writeByte((byte) MysqlDefs.QUIT);
            sendResponsePacket(packet, packet.getPosition());
        } finally {
            forceClose();
        }
    }

    public void setStatementInterceptors(List<StatementInterceptor> statementInterceptorList) {
          this.statementInterceptorList = statementInterceptorList;
    }
}
