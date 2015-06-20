package com.zzp.lightdfs.common;


import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.server.RMISocketFactory;

public class DFSRMISocket extends RMISocketFactory {
    int dataPort = 10002;
    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return new Socket(host, port);
    }
    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        if(port == 0)
            port = dataPort;
        return new ServerSocket(port);
    }
    public void setDataPort(int _dataPort){
        this.dataPort = _dataPort;
    }
}
