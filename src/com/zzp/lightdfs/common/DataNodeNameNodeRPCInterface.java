package com.zzp.lightdfs.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface DataNodeNameNodeRPCInterface extends Remote{
    ArrayList<String> sendDataNodeJump(String datanodeID) throws RemoteException;
    boolean registerDataNode(DFSDataNodeState datanode) throws RemoteException;
    void sendDataNodeStates(DFSDataNodeState datanode) throws RemoteException;
    boolean unRegisterDataNode(String datanodeID) throws RemoteException;
    void sendDataNodeBlockList(String datanodeID, ArrayList<DFSBlock> blocks) throws RemoteException;
}
