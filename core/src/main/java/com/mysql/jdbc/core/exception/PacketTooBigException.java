package com.mysql.jdbc.core.exception;

import java.sql.SQLException;

/**
 * @author hjx
 */
public class PacketTooBigException extends SQLException {

    public PacketTooBigException(long packetSize, long maximumPacketSize) {
       super("PacketTooBigException"+"packet len"+packetSize,"max Len"+maximumPacketSize);
    }
}
