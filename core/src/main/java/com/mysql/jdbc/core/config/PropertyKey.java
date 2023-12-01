package com.mysql.jdbc.core.config;

/**
 * @author hjx
 */
public enum PropertyKey {
    useUnicode("useUnicode"),
    characterEncoding("characterEncoding"),
    cachePreparedStatements("cachePreparedStatements"),
    preparedStatementCacheSize("preparedStatementCacheSize"),
    preparedStatementCacheSqlLimit("preparedStatementCacheSqlLimit"),
    socketFactoryClassName("socketFactoryClassName"),
    useServerPreparedStmts("useServerPreparedStmts"),
    statementInterceptors("statementInterceptors");

    private String name;

    PropertyKey(String name){
        this.name = name;
    }

}
