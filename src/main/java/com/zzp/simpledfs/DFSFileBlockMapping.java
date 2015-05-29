package main.java.com.zzp.simpledfs;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSFileBlockMapping implements Serializable {
    String filePath;    // 文件在DFS目录下的绝对路径
    boolean ifLittleFile;   // 是否是小文件：小文件合并，大文件切割
    ArrayList<String> blocks;   // 大文件数据块列表, 有顺序
    long fileSize;      // 文件大小,最大2^63-1 byte大小
    int offset;         // 小文件所在数据块偏移地址
}
