package main.java.com.zzp.app;

import java.io.Serializable;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSDataNodeState implements Serializable {
    String datanodeID;
    String ip, port;
    long totalSpace;    // �ܿռ��С��Byte
    long freeSpace;     // ʣ��ռ��С��Byte
    long usedSpace;     // ���ÿռ��С��Byte
}
