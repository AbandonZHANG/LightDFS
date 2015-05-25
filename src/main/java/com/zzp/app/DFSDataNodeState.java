package main.java.com.zzp.app;

import java.io.Serializable;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSDataNodeState implements Serializable {
    String datanodeID;
    String ip, port;
    long totalSpace;    // 总空间大小，Byte
    long freeSpace;     // 剩余空间大小，Byte
    long usedSpace;     // 已用空间大小，Byte
}
