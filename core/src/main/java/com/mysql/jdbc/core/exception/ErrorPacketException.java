package com.mysql.jdbc.core.exception;

import java.sql.SQLException;

/**
 * @author hjx
 */
public class ErrorPacketException extends SQLException {
    public ErrorPacketException(int errorCode,String sqlStatue, String message){
        super("ErrorPacketException."+"errorCode="+errorCode+",sqlStatue = "+sqlStatue+","+"errorMessage="+message);
    }

    public ErrorPacketException(int errorCode,String message){
        super("ErrorPacketException."+"errorCode="+errorCode+"errorMessage="+message);
    }
}
