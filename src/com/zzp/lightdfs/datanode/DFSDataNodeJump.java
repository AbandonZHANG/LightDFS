package com.zzp.lightdfs.datanode;

import java.io.File;
import java.util.ArrayList;

public class DFSDataNodeJump extends Thread{
    String datanodeID;
    DFSDataNode datanode;
    int perSeconds;  // 每隔N ms发送一次心跳
    boolean ifRun;
    DFSDataNodeJump(String _datanodeID, DFSDataNode _datanode, int _perSecond){
        super();
        datanodeID = _datanodeID;
        datanode = _datanode;
        perSeconds = _perSecond;
        ifRun = true;
    }
    @Override
    public void run() {
        while(ifRun) {
            try {
                ArrayList<String> toDelBlocks = datanode.datanodeRPC.sendDataNodeJump(datanodeID);
                // 如果主控节点有返回数据块列表，则删除
                if(toDelBlocks != null){
                    for(String block : toDelBlocks){
                        File rFile = new File(datanode.absoluteBlockDirectory+"\\"+block);
                        if(!rFile.delete()){
                            System.out.println("[DELETE ERROR!]");
                        }
                    }
                    // 发送更新后的数据块目录
                    datanode.sendBlockRecord();
                    // 发送更新后的硬盘空间
                    datanode.sendNodeStates();
                }
                //System.out.println("[INFO] Sending datanode jump...");
                sleep(perSeconds);
            } catch (Exception e) {
                System.out.println("[JUMP ERROR!] The Namenode RMI serve is not found!");
                return ;
            }
        }
        System.out.println("[INFO] The DataNode Jump is closed.");
    }
}
