package com.mysql.jdbc.core.config;

import java.sql.Connection;

/**
 * @author hjx
 */
public enum TransIsolationName {

    TRANSACTION_READ_UNCOMMITTED("READ-UNCOMMITED", Connection.TRANSACTION_READ_UNCOMMITTED),
    TRANSACTION_READ_UNCOMMITTED_V("READ-UNCOMMITTED", Connection.TRANSACTION_READ_UNCOMMITTED),
    TRANSACTION_READ_COMMITTED("READ-COMMITTED", Connection.TRANSACTION_READ_COMMITTED),
    TRANSACTION_REPEATABLE_READ("REPEATABLE-READ", Connection.TRANSACTION_REPEATABLE_READ),
    TRANSACTION_SERIALIZABLE("SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);
    private String name;
    private Integer value;
    TransIsolationName(String name, Integer value){
        this.name = name;
        this.value =value;
    }

    public static Integer getValue(String name){
        for (TransIsolationName transIsolationName:TransIsolationName.values()){
            if (transIsolationName.name.equals(name)){
                return transIsolationName.value;
            }
        }
        return null;
    }
}
