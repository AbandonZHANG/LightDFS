package com.zzp.simpledfs.common;

public class DFSDataNodeRPCAddress {
    private String ip, port;
    public DFSDataNodeRPCAddress(String _ip, String _port){
        ip = _ip;
        port = _port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
