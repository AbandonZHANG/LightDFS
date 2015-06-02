package com.zzp.simpledfs.namenode;

import java.util.*;

public class DFSConsistentHashing {
    static private long unitSpace = 64*1024*1024;  // 每unitSpace MB硬盘空间一个虚拟节点, 默认64MB
    public static void setUnitSpace(long unitSpace) {
        DFSConsistentHashing.unitSpace = unitSpace;
    }

    private class CHNode{
        String nodeName;    // 节点名
        long freeSpace;     // 节点硬盘空间MB
        int virtualNumber;  // 虚拟节点个数，根据数据节点硬盘空间改变虚拟节点个数
        ArrayList<String> nodeKeys;   // 节点在Consistent Hashing中Hash前的Key值
        CHNode(String _nodeName, long _freeSpace){
            nodeName = _nodeName;
            freeSpace = _freeSpace;
            virtualNumber = (int)(freeSpace / DFSConsistentHashing.unitSpace);
            nodeKeys = new ArrayList<String>();
            for(int i = 0; i < virtualNumber; i ++){
                nodeKeys.add(UUID.randomUUID().toString());
            }
        }
        public void setVirtualNumber(long _freeSpace){
            freeSpace = _freeSpace;
            virtualNumber = (int)(freeSpace / DFSConsistentHashing.unitSpace);
        }
    }
    private final HashMap<String, CHNode> chNodes = new HashMap<String, CHNode>();
    private final TreeMap<Integer, String> chCircle = new TreeMap<Integer, String>();
    public void addNode(String nodeName, long freeSpace){
        // 新建CHNode节点信息
        CHNode newNode = new CHNode(nodeName, freeSpace);
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
        // 找到对应CHNode节点信息
        CHNode theNode = chNodes.get(nodeName);
        // 修正虚拟节点个数
        int originVNum = theNode.virtualNumber;
        theNode.setVirtualNumber(freeSpace);
        if(originVNum > theNode.virtualNumber){
            int diffNum = originVNum - theNode.virtualNumber;
            while(diffNum != 0){
                String delKey = theNode.nodeKeys.get(theNode.nodeKeys.size()-1);
                // 删除多余的虚拟节点key
                theNode.nodeKeys.remove(theNode.nodeKeys.size()-1);
                // 删除circle中对应的值
                chCircle.remove(DFSHashFunction.hash(delKey));
                diffNum --;
            }
        }
        else{
            int diffNum = theNode.virtualNumber - originVNum;
            while(diffNum != 0){
                // 加入新的虚拟节点Key
                String newKey = UUID.randomUUID().toString();
                theNode.nodeKeys.add(newKey);
                chCircle.put(DFSHashFunction.hash(newKey), nodeName);
                diffNum --;
            }
        }
    }
    public void removeNode(String nodeName){
        // 找到对应CHNode节点信息
        CHNode theNode = chNodes.get(nodeName);
        // 将节点从Consistent Hashing节点列表中删除
        chNodes.remove(nodeName);
        int vNum = theNode.nodeKeys.size();
        for(int i = 0; i < vNum; i ++){
            // 求出每个key的hashcode，加入circle域中
            chCircle.remove(DFSHashFunction.hash(theNode.nodeKeys.get(i)));
        }
        // ！！！数据迁移

    }
    public String getNode(String key){
        Integer hashcode = DFSHashFunction.hash(key);
        if(chCircle.containsKey(hashcode)){
            return chCircle.get(hashcode);
        }
        else{
            String node = null;
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
