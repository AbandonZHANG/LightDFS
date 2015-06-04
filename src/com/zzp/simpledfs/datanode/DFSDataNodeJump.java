package com.zzp.simpledfs.datanode;

import com.zzp.simpledfs.common.DataNodeNameNodeRPCInterface;

import java.rmi.Naming;

public class DFSDataNodeJump extends Thread{
    String datanodeName;
    int perSeconds;  // 每隔N ms发送一次心跳
    String namenodeIp, namenodePort;
    DFSDataNodeJump(String _datanodeName, String _namenodeIp, String _namenodePort, int _perSecond){
        super();
        datanodeName = _datanodeName;
        namenodeIp = _namenodeIp;
        namenodePort = _namenodePort;
        perSeconds = _perSecond;
    }
    public void run() {
        while(true) {
            try {
                DataNodeNameNodeRPCInterface datanodeRmi = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://"+namenodeIp+":"+namenodePort+"/DFSNameNode");
                datanodeRmi.sendDataNodeJump(datanodeName);
                sleep(perSeconds);
            } catch (Exception e) {
                //e.printStackTrace();
                System.out.println("The Namenode RMI serve is not found!");
            }
        }
    }
}
