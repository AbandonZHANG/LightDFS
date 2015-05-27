package main.java.com.zzp.simpledfs;

import java.io.File;
import java.rmi.Naming;

/**
 * Created by Zhipeng Zhang on 15/05/26 0026.
 */
public class DFSDataNodeJump extends Thread{
    String workingDirectory;        // ���ݷ���������Ŀ¼
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
                 * ÿ��1���ӷ���һ������
                 */
                sleep(6000);
            } catch (Exception e) {
                //e.printStackTrace();
                System.out.println("The Namenode RMI serve is not found!");
                lentime ++;
                // ��� NameNode 5��δ��Ӧ����Ͽ�����
                if(lentime == 5)
                    return;
            }
        }
    }
}
