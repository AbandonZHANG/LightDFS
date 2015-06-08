package com.zzp.simpledfs.namenode;

import java.util.*;

public class DFSConsistentHashing {
    static final private int virtualNumber = 32;
    static final private int unitSpace = 64*1024*1024;  // 当数据节点硬盘空间小于64M时从一致性哈希中删除

    public class CHNode{
        String nodeID;      // 节点名
        ArrayList<String> nodeKeys;   // 节点在Consistent Hashing中Hash前的Key值
        CHNode(String _nodeID){
            nodeID = _nodeID;
            nodeKeys = new ArrayList<>();
            for(int i = 0; i < virtualNumber; i ++){
                nodeKeys.add(UUID.randomUUID().toString());
            }
        }
    }
    private final HashMap<String, CHNode> chNodes = new HashMap<>();
    private final TreeMap<Integer, String> chCircle = new TreeMap<>();
    public void addNode(String nodeName){
        // 新建CHNode节点信息
        CHNode newNode = new CHNode(nodeName);
        // 将节点加入Consistent Hashing节点列表中
        chNodes.put(nodeName, newNode);
        int vNum = newNode.nodeKeys.size();
        for(int i = 0; i < vNum; i ++){
            // 求出每个key的hashcode，加入circle域中
            chCircle.put(DFSHashFunction.hash(newNode.nodeKeys.get(i)), nodeName);
        }
        // 数据迁移
    }
    public void updateNode(String nodeName, long freeSpace){
        if(chNodes.containsKey(nodeName) && freeSpace < unitSpace){
            removeNode(nodeName);
        }
    }
    public void removeNode(String nodeName){
        if(chNodes.containsKey(nodeName)){
            // 找到对应CHNode节点信息
            CHNode theNode = chNodes.get(nodeName);
            // 将节点从Consistent Hashing节点列表中删除
            chNodes.remove(nodeName);
            int vNum = theNode.nodeKeys.size();
            for(int i = 0; i < vNum; i ++){
                // 求出每个key的hashcode，从circle域中删除
                chCircle.remove(DFSHashFunction.hash(theNode.nodeKeys.get(i)));
            }
            // ！！！数据迁移
        }
    }
    public String getNode(String key){
        Integer hashcode = DFSHashFunction.hash(key);
        if(chCircle.containsKey(hashcode)){
            return chCircle.get(hashcode);
        }
        else{
            String node;
            Map.Entry entry = chCircle.higherEntry(hashcode);
            if(entry == null){
                entry = chCircle.firstEntry();
                if(entry == null)
                    return null;
                else
                    node = (String)entry.getValue();
            }
            else{
                node = (String)entry.getValue();
            }
            return node;
        }
    }
}
