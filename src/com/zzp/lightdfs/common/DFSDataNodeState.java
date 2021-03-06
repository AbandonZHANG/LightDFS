package com.zzp.lightdfs.common;

import java.io.Serializable;
import java.time.LocalDateTime;

public class DFSDataNodeState implements Serializable {
    private String datanodeID;
    public DFSDataNodeRPCAddress addr = new DFSDataNodeRPCAddress();
    private LocalDateTime lastJumpTime;     //上一次心跳的时间
    private long totalSpace;    // 总空间大小，Byte
    private long freeSpace;     // 剩余空间大小，Byte
    private long usedSpace;     // 已用空间大小，Byte

    public String getDatanodeID() {
        return datanodeID;
    }

    public void setDatanodeID(String datanodeID) {
        this.datanodeID = datanodeID;
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

    public LocalDateTime getLastJumpTime() {
        return lastJumpTime;
    }

    public void setLastJumpTime(LocalDateTime lastJumpTime) {
        this.lastJumpTime = lastJumpTime;
    }
}
