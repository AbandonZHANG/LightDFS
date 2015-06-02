package com.zzp.simpledfs.common;

import java.io.Serializable;

public class DFSDataNodeState implements Serializable {
    private String datanodeID;
    private String ip, port;
    private long totalSpace;    // 总空间大小，Byte
    private long freeSpace;     // 剩余空间大小，Byte
    private long usedSpace;     // 已用空间大小，Byte

    public String getDatanodeID() {
        return datanodeID;
    }

    public void setDatanodeID(String datanodeID) {
        this.datanodeID = datanodeID;
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

    public long getTotalSpace() {
        return totalSpace;
    }

    public void setTotalSpace(long totalSpace) {
        this.totalSpace = totalSpace;
    }

    public long getFreeSpace() {
        return freeSpace;
    }

    public void setFreeSpace(long freeSpace) {
        this.freeSpace = freeSpace;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }
}
