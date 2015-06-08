package com.zzp.simpledfs.datanode;

import com.zzp.simpledfs.common.DataNodeNameNodeRPCInterface;

public class DFSDataNodeJump extends Thread{
    String datanodeID;
    int perSeconds;  // 每隔N ms发送一次心跳
    DataNodeNameNodeRPCInterface datanodeRPC;
    DFSDataNodeJump(String _datanodeID, DataNodeNameNodeRPCInterface _datanodeRPC, int _perSecond){
        super();
        datanodeID = _datanodeID;
        datanodeRPC = _datanodeRPC;
        perSeconds = _perSecond;
    }
    @Override
    public void run() {
        while(true) {
            try {
                datanodeRPC.sendDataNodeJump(datanodeID);
                System.out.println("[INFO] Sending datanode jump...");
                sleep(perSeconds);
            } catch (Exception e) {
                System.out.println("[JUMP ERROR!] The Namenode RMI serve is not found!");
                return ;
            }
        }
    }
}
