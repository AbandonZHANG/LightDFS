package com.zzp.simpledfs.common;

import java.io.Serializable;

public class DFSBlock implements Serializable{
    private String blockName;
    private String datanodeID;
    private long blockSize;

    public DFSBlock(String _datanodeID, String _blockName, long _blockSize){
        datanodeID = _datanodeID;
        blockName = _blockName;
        blockSize = _blockSize;
    }

    public String getBlockName() {
        return blockName;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public String getDatanodeID() {
        return datanodeID;
    }
}
