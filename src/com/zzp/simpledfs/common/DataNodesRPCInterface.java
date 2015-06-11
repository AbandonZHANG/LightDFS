package com.zzp.simpledfs.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

public interface DataNodesRPCInterface extends Remote{
    void sendBlocks(HashMap<String, byte[]> blocks) throws RemoteException;
}
