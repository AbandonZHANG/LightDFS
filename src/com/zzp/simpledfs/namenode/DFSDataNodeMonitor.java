package com.zzp.simpledfs.namenode;

import com.zzp.simpledfs.common.DFSBlock;
import com.zzp.simpledfs.common.DFSDataNodeState;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DFSDataNodeMonitor extends Thread{
    private HashMap<String, DFSDataNodeState> activeDatanodes;     // 当前活跃数据节点状态
    private long intervalTime;  // ms，超过多少毫秒就表示数据节点已断开
    private ArrayList<String> excludeNodes;
    private DFSConsistentHashing consistentHash;
    private HashMap<String, DFSBlock> blockDataNodeMappings;
    boolean ifRun;

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
        ifRun = true;
    }

    @Override
    public void run(){
        while(ifRun){
            Iterator<Map.Entry<String, DFSDataNodeState>> iter = activeDatanodes.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry<String, DFSDataNodeState> entry = iter.next();
                DFSDataNodeState thisDatanode = entry.getValue();
                if(ChronoUnit.MILLIS.between(thisDatanode.getLastJumpTime(), LocalDateTime.now()) > intervalTime){
                    // 标记断开连接
                    iter.remove();
                    // 更新exclude标记
                    excludeNodes.add(thisDatanode.getDatanodeID());
                    // 更新一致性哈希
                    // consistentHash.removeNode(thisDatanode.getDatanodeID());
                    // 更新数据块目录
                    Iterator<Map.Entry<String, DFSBlock>> iterBlock = blockDataNodeMappings.entrySet().iterator();
                    while(iterBlock.hasNext()){
                        DFSBlock thisBlock = iterBlock.next().getValue();
                        if(thisBlock.getDatanodeID().equals(thisDatanode.getDatanodeID())){
                            iterBlock.remove();
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
        System.out.println("[INFO] The DataNode Monitor is closed.");
    }
}
