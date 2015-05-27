package main.java.com.zzp.simpledfs;

import java.io.FileNotFoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by Zhipeng Zhang on 15/05/26 0026.
 */
public interface ClientDataNodeRPCInterface extends Remote {
    public void uploadBlock(byte[] content, String blockName) throws RemoteException;
    public byte[] downloadBlock(String blockName) throws RemoteException, FileNotFoundException;
    public void deleteBlock(String blockName) throws RemoteException, FileNotFoundException;
}
