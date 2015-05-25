package main.java.com.zzp.app;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public interface DataNodeNameNodeRPCInterface extends Remote{
    public void sendDataNodeStates(DFSDataNodeState datanode) throws RemoteException;
    public boolean registerDataNode(DFSDataNodeState datanode) throws RemoteException;
    public boolean unRegisterDataNode(String datanodeID) throws RemoteException;
    public void sendDataNodeBlockList(String datanodeID, ArrayList<String> blocks) throws RemoteException;
}
