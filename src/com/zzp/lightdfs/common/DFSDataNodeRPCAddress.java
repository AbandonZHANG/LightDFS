package com.zzp.lightdfs.common;

import java.io.Serializable;

public class DFSDataNodeRPCAddress implements Serializable{
    private String ip, clientrpcport, datanoderpcport, namenoderpcport;
    public DFSDataNodeRPCAddress(){}
    public DFSDataNodeRPCAddress(DFSDataNodeRPCAddress _origin){
        ip = _origin.getIp();
        clientrpcport = _origin.getPort();
        datanoderpcport = _origin.getDatanoderpcport();
        namenoderpcport = _origin.getNamenoderpcport();
    }
    public DFSDataNodeRPCAddress(String _ip, String _clientrpcport, String _datanoderpcport, String _namenoderpcport){
        ip = _ip;
        clientrpcport = _clientrpcport;
        datanoderpcport = _datanoderpcport;
        namenoderpcport = _namenoderpcport;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getPort() {
        return clientrpcport;
    }

    public void setPort(String port) {
        this.clientrpcport = port;
    }

    public String getDatanoderpcport() {
        return datanoderpcport;
    }

    public void setDatanoderpcport(String datanoderpcport) {
        this.datanoderpcport = datanoderpcport;
    }

    public String getNamenoderpcport() {
        return namenoderpcport;
    }

    public void setNamenoderpcport(String namenoderpcport) {
        this.namenoderpcport = namenoderpcport;
    }
}
