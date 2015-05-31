package main.java.com.zzp.simpledfs;

import java.rmi.Naming;

public class DFSDataNodeJump extends Thread{
    String datanodeName;
    int perSeconds;  // 每隔N ms发送一次心跳
    int errorNumToQuit; //连续N次没有连接上NameNode就退出
    DFSDataNodeJump(String _datanodeName, int _perSecond, int _errorNumToQuit){
        super();
        datanodeName = _datanodeName;
        perSeconds = _perSecond;
        errorNumToQuit = _errorNumToQuit;
    }
    public void run() {
        while(true) {
            int errorTime = 0;
            try {
                DataNodeNameNodeRPCInterface datanodeRmi = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
                datanodeRmi.sendDataNodeJump(datanodeName);
                errorTime = 0;
                sleep(perSeconds);
            } catch (Exception e) {
                //e.printStackTrace();
                System.out.println("The Namenode RMI serve is not found!");
                errorTime ++;
                // 如果 NameNode 多次未反应，则断开连接
                if(errorTime == errorNumToQuit)
                    return;
            }
        }
    }
}
