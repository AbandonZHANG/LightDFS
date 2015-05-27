package main.java.com.zzp.simpledfs;

import java.io.File;
import java.rmi.Naming;

/**
 * Created by Zhipeng Zhang on 15/05/26 0026.
 */
public class DFSDataNodeJump extends Thread{
    String workingDirectory;        // 数据服务器工作目录
    final DFSDataNodeState mystate = new DFSDataNodeState();
    public void getNodeStates(){
        try {
            File workingDir = new File(workingDirectory);
            mystate.totalSpace = workingDir.getTotalSpace();
            mystate.freeSpace = workingDir.getFreeSpace();
            mystate.usedSpace = mystate.totalSpace - mystate.freeSpace;
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void run() {
        while(true) {
            getNodeStates();
            int lentime = 0;
            try {
                DataNodeNameNodeRPCInterface datanodeRmi = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
                datanodeRmi.sendDataNodeStates(mystate);
                lentime = 0;
                /**
                 * 每隔1分钟发送一次心跳
                 */
                sleep(6000);
            } catch (Exception e) {
                //e.printStackTrace();
                System.out.println("The Namenode RMI serve is not found!");
                lentime ++;
                // 如果 NameNode 5次未反应，则断开连接
                if(lentime == 5)
                    return;
            }
        }
    }
}
