package com.mysql.jdbc.core;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

/**
 * @author hjx
 */
public interface SocketFactory {

    /**
     * 创建socket连接
     * @param host ip
     * @param portNumber 端口
     * @param props 配置属性
     * @return 一个socket连接
     * @throws SocketException
     * @throws IOException
     */
    Socket connect(String host, int portNumber, Properties props)
            throws SocketException, IOException;
}
