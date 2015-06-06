package com.zzp.simpledfs.common;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.RemoteException;
import java.util.HashMap;

public class DFSINode implements Serializable {
    public static DFSINode getInstance(String name, boolean ifDir){
        return new DFSINode(name, ifDir);
    }

    public static char splitChar = '/';    // 目录分隔符
    public static String splitStr = "/";
    public String name;    // 文件or目录名称
    public HashMap<String, DFSINode> childInode;   // childInode == null 表示该节点是文件, childInode != null 表示该节点是目录

    DFSINode(String _name, boolean ifDir){
        name = _name;
        if(ifDir)
            childInode = new HashMap<>();
    }

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
        for(String key:inode.childInode.keySet()){
            DFSINode child = inode.childInode.get(key);
            listDirTree(child, depth+1);
        }
    }

    /**
     * DFSInode 核心操作函数
     * @param inodePath DFSINode节点绝对路径
     * @param method
     *        11: add new file
     *        12: delete a file
     *         1: add new dir
     *         2: delete a dir
     *         0: return the exists dir INode. If not exists, return null.
     * @return method == 0 且存在该节点时返回对应DFSINode，其他情况返回null
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    public static DFSINode updateDFSINode(DFSINode inode, String inodePath, int method) throws RemoteException, FileNotFoundException, FileAlreadyExistsException {
        // 将路径分隔出各级目录来
        String[] splits;
        splits = inodePath.split(splitStr);

        int length = splits.length-1;

        for (int i = 0; i <= length; i ++){
            String eachPath = splits[i];
            // 当前路径是文件
            if(inode.childInode == null){
                throw new FileAlreadyExistsException("");
            }
            // 当前目录存在名为eachPath的子文件/目录
            // ！！！当前还没有判断对应的儿子INode节点是目录还是文件
            else if(inode.childInode.containsKey(eachPath)){
                if(i == length){
                    if(method == 11){
                        throw new FileAlreadyExistsException("");
                    }
                    else if(method == 1){
                        throw new FileAlreadyExistsException("");
                    }
                    else if(method == 12){
                        DFSINode tmpChild = inode.childInode.get(eachPath);
                        if(tmpChild.childInode == null)
                            inode.childInode.remove(eachPath);
                        else
                            // It's a Directory!
                            throw new FileNotFoundException();
                    }
                    else if(method == 2){
                        DFSINode tmpChild = inode.childInode.get(eachPath);
                        if(tmpChild.childInode != null)
                            inode.childInode.remove(eachPath);
                        else
                            // It's a File!
                            throw new FileNotFoundException();
                    }
                    else if(method == 0){
                        return inode.childInode.get(eachPath);
                    }
                }
                else{
                    // 继续往下遍历
                    inode = inode.childInode.get(eachPath);
                }
            }
            else{
                if(i == length){
                    if(method == 11){
                        DFSINode newInode = DFSINode.getInstance(eachPath, false);
                        inode.childInode.put(eachPath, newInode);
                    }
                    else if(method == 1){
                        DFSINode newInode = DFSINode.getInstance(eachPath, true);
                        inode.childInode.put(eachPath, newInode);
                    }
                    else if(method == 2 || method == 12){
                        throw new FileNotFoundException();
                    }
                    else if(method == 0){
                        return null;
                    }
                }
                else{
                    if(method == 1){
                        // 新建目录
                        DFSINode newInode = DFSINode.getInstance(eachPath, true);
                        inode.childInode.put(eachPath, newInode);
                        // 继续沿着新建目录往下遍历
                        inode = newInode;
                    }
                    else{
                        throw new FileNotFoundException();
                    }
                }
            }
        }
        return null;
    }
}
