package com.mysql.jdbc.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Properties;

/**
 * @author hjx
 */
public class StandardSocketFactory implements SocketFactory {

    public static final String TCP_NO_DELAY_PROPERTY_NAME = "tcpNoDelay";
    public static final String TCP_NO_DELAY_DEFAULT_VALUE = "true";

    public static final String TCP_KEEP_ALIVE_PROPERTY_NAME = "tcpKeepAlive";
    public static final String TCP_KEEP_ALIVE_DEFAULT_VALUE = "true";

    public static final String TCP_RCV_BUF_PROPERTY_NAME = "tcpRcvBuf";

    public static final String TCP_SND_BUF_PROPERTY_NAME = "tcpSndBuf";

    public static final String TCP_RCV_BUF_DEFAULT_VALUE = "0";

    public static final String TCP_SND_BUF_DEFAULT_VALUE = "0";


    protected String host = null;

    protected int port = 3306;

    protected Socket rawSocket = null;

    @Override
    public Socket connect(String host, int portNumber, Properties props) throws SocketException, IOException {
        if (props!=null){
            this.host = host;
            this.port= portNumber;
            this.rawSocket = new Socket();
            configureSocket(this.rawSocket, props);
            rawSocket.connect(new InetSocketAddress(this.host,this.port));
            return rawSocket;
        }
        throw new SocketException("Unable to create socket");
    }

    /**
     * 进行socket配置
     * @param sock socket
     * @param props 属性配置
     * @throws SocketException
     * @throws IOException
     */
    private void configureSocket(Socket sock, Properties props) throws SocketException,
            IOException {
        try {
            sock.setTcpNoDelay(Boolean.parseBoolean(
                    props.getProperty(TCP_NO_DELAY_PROPERTY_NAME,
                            TCP_NO_DELAY_DEFAULT_VALUE)));

            String keepAlive = props.getProperty(TCP_KEEP_ALIVE_PROPERTY_NAME,
                    TCP_KEEP_ALIVE_DEFAULT_VALUE);

            if (keepAlive != null && keepAlive.length() > 0) {
                sock.setKeepAlive(Boolean.parseBoolean(keepAlive));
            }

            int receiveBufferSize = Integer.parseInt(props.getProperty(
                    TCP_RCV_BUF_PROPERTY_NAME, TCP_RCV_BUF_DEFAULT_VALUE));

            if (receiveBufferSize > 0) {
                sock.setReceiveBufferSize(receiveBufferSize);
            }

            int sendBufferSize = Integer.parseInt(props.getProperty(
                    TCP_SND_BUF_PROPERTY_NAME, TCP_SND_BUF_DEFAULT_VALUE));

            if (sendBufferSize > 0) {
                sock.setSendBufferSize(sendBufferSize);
            }
        } catch (Throwable t) {
            throw new SocketException("Unable to configure socket");
        }
    }
}
