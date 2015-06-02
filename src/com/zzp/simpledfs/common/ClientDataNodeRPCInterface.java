package com.zzp.simpledfs.common;

import java.io.FileNotFoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientDataNodeRPCInterface extends Remote {
    void uploadBlock(byte[] content, String blockName) throws RemoteException;
    byte[] downloadBlock(String blockName) throws RemoteException, FileNotFoundException;
    void deleteBlock(String blockName) throws RemoteException, FileNotFoundException;
}
