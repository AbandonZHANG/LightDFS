package com.zzp.simpledfs.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

public class DFSINode implements Serializable {
    public static DFSINode getInstance(String name, boolean ifDir){
        return new DFSINode(name, ifDir);
    }
    public String name;    // 文件or目录名称
    public HashMap<String, DFSINode> childInode;   // childInode == null 表示该节点是文件, childInode != null 表示该节点是目录
    DFSINode(String _name, boolean ifDir){
        name = _name;
        if(ifDir)
            childInode = new HashMap<String, DFSINode>();
    }
    /**
     * @param inode
     * @param depth
     */
    public static void listDirTree(DFSINode inode, int depth){
        for(int i = 1; i < depth; i++){
            if(i != depth-1){
                System.out.print("|  ");
            }
            else{
                System.out.print("|--");
            }
        }
        System.out.println(inode.name);
        if(inode.childInode == null)
            return;
        Iterator iter = inode.childInode.keySet().iterator();
        while(iter.hasNext()){
            Object key = iter.next();
            DFSINode child = inode.childInode.get(key);
            listDirTree(child, depth+1);
        }
    }
}
