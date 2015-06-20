package com.zzp.lightdfs.common;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface NameNodeDataNodeRPCInterface extends Remote{
    void sendBlocksTo(String datanodeIp, String datanodePort, ArrayList<String>blockNames) throws RemoteException, NotBoundException;
}
