package com.zzp.simpledfs.datanode;

import com.zzp.simpledfs.common.DataNodeNameNodeRPCInterface;

public class DFSDataNodeJump extends Thread{
    String datanodeID;
    int perSeconds;  // 每隔N ms发送一次心跳
    DataNodeNameNodeRPCInterface datanodeRPC;
    boolean ifRun;
    DFSDataNodeJump(String _datanodeID, DataNodeNameNodeRPCInterface _datanodeRPC, int _perSecond){
        super();
        datanodeID = _datanodeID;
        datanodeRPC = _datanodeRPC;
        perSeconds = _perSecond;
        ifRun = true;
    }
    @Override
    public void run() {
        while(ifRun) {
            try {
                datanodeRPC.sendDataNodeJump(datanodeID);
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
