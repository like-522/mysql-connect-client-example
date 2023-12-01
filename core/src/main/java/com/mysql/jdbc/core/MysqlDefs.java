package com.mysql.jdbc.core;

/**
 * @author hjx
 */
public class MysqlDefs {

    static final int QUIT = 1;
    static final int COM_QUERY = 0x03;

    // statement command start
    //预编译sql语句
    static final int COM_PREPARE = 0x16;
    //执行预编译sql语句

    public static final int COM_STMT_EXECUTE = 0x17;

    //关闭预编译sql语句
    public static final int COM_STMT_CLOSE = 0x179;
    // statement command end

    static final byte OPEN_CURSOR_FLAG = 1;

    // field Types start
    static final int FIELD_TYPE_DECIMAL = 0;

    static final int FIELD_TYPE_TINY = 1;

    static final int FIELD_TYPE_SHORT = 2;

    //4个字节,对应java的int类型
    static final int FIELD_TYPE_LONG = 3;

    static final int FIELD_TYPE_FLOAT = 4;

    static final int FIELD_TYPE_DOUBLE = 5;

    static final int FIELD_TYPE_NULL = 6;

    static final int FIELD_TYPE_TIMESTAMP = 7;

    static final int FIELD_TYPE_LONGLONG = 8;

    static final int FIELD_TYPE_INT24 = 9;

    static final int FIELD_TYPE_DATE = 10;

    static final int FIELD_TYPE_TIME = 11;

    static final int FIELD_TYPE_DATETIME = 12;

    static final int FIELD_TYPE_YEAR = 13;

    static final int FIELD_TYPE_NEWDATE = 14;

    static final int FIELD_TYPE_VARCHAR = 15;

    static final int FIELD_TYPE_BIT = 16;

    static final int FIELD_TYPE_NEW_DECIMAL = 246;

    static final int FIELD_TYPE_ENUM = 247;

    static final int FIELD_TYPE_SET = 248;

    static final int FIELD_TYPE_TINY_BLOB = 249;

    static final int FIELD_TYPE_MEDIUM_BLOB = 250;

    static final int FIELD_TYPE_LONG_BLOB = 251;

    public static final int FIELD_TYPE_BLOB = 252;

    static final int FIELD_TYPE_VAR_STRING = 253;

    static final int FIELD_TYPE_STRING = 254;

    static final int FIELD_TYPE_GEOMETRY = 255;

    //
    static final int  MAX_FIELD_SIZE = 65536;
    static final int MAX_ROWS = 50000000; // mysql文档中说明.































    // field Types end
}
