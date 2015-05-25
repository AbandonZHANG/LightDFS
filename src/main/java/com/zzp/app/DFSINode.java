package main.java.com.zzp.app;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSINode implements Serializable {
    String name;    // �ļ�orĿ¼����
    HashMap<String, DFSINode> childInode;   // childInode == null ��ʾ�ýڵ����ļ�, childInode != null ��ʾ�ýڵ���Ŀ¼

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
