package main.java.com.zzp.simpledfs;

import java.io.Serializable;

public class DFSDataNodeState implements Serializable {
    String datanodeID;
    String ip, port;
    long totalSpace;    // 总空间大小，Byte
    long freeSpace;     // 剩余空间大小，Byte
    long usedSpace;     // 已用空间大小，Byte
}
