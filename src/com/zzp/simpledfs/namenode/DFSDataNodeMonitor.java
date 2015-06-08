package com.zzp.simpledfs.namenode;

import com.zzp.simpledfs.common.DFSBlock;
import com.zzp.simpledfs.common.DFSDataNodeState;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class DFSDataNodeMonitor extends Thread{
    private HashMap<String, DFSDataNodeState> activeDatanodes;     // 当前活跃数据节点状态
    private long intervalTime;  // ms，超过多少毫秒就表示数据节点已断开
    private ArrayList<String> excludeNodes;
    private DFSConsistentHashing consistentHash;
    private HashMap<String, DFSBlock> blockDataNodeMappings;

    public DFSDataNodeMonitor(
            HashMap<String,
            DFSDataNodeState> _activeDatanodes,
            ArrayList<String> _excludeNodes,
            DFSConsistentHashing _consistentHash,
            HashMap<String, DFSBlock> _blockDataNodeMappings,
            long _intervalTime){
        super();
        activeDatanodes = _activeDatanodes;
        excludeNodes = _excludeNodes;
        consistentHash = _consistentHash;
        blockDataNodeMappings = _blockDataNodeMappings;
        intervalTime = _intervalTime;
    }

    @Override
    public void run(){
        while(true){
            for(Map.Entry<String, DFSDataNodeState> entry : activeDatanodes.entrySet()){
                DFSDataNodeState thisDatanode = entry.getValue();
                if(ChronoUnit.MILLIS.between(thisDatanode.getLastJumpTime(), LocalDateTime.now()) > intervalTime){
                    // 标记断开连接
                    activeDatanodes.remove(thisDatanode.getDatanodeID());
                    // 更新exclude标记
                    excludeNodes.add(thisDatanode.getDatanodeID());
                    // 更新一致性哈希
                    consistentHash.removeNode(thisDatanode.getDatanodeID());
                    // 更新数据块目录
                    for(Map.Entry<String, DFSBlock> entryBlock:blockDataNodeMappings.entrySet()){
                        DFSBlock thisBlock = entryBlock.getValue();
                        if(thisBlock.getDatanodeID().equals(thisDatanode.getDatanodeID())){
                            blockDataNodeMappings.remove(thisBlock.getBlockName());
                        }
                    }
                }
            }
            try{
                sleep(intervalTime);
            }
            catch (Exception e){
                System.out.println("[MONITOR ERROR!] DataNode Monitor error!");
                return ;
            }
        }
    }
}
