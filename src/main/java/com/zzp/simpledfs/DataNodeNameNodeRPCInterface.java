package main.java.com.zzp.simpledfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public interface DataNodeNameNodeRPCInterface extends Remote{
    void sendDataNodeJump(String datanode) throws RemoteException;
    boolean registerDataNode(DFSDataNodeState datanode) throws RemoteException;
    void sendDataNodeStates(DFSDataNodeState datanode) throws RemoteException;
    boolean unRegisterDataNode(String datanodeID) throws RemoteException;
    void sendDataNodeBlockList(String datanodeID, ArrayList<String> blocks) throws RemoteException;
}
