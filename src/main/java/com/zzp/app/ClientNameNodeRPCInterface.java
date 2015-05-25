package main.java.com.zzp.app;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public interface ClientNameNodeRPCInterface extends Remote {
    public void addDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    public void delDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    public boolean ifExistsDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    public ArrayList< Map.Entry<String, String> > newDFSFileMapping(String filePath, int blocks_num, boolean ifLittleFile) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    public void renameDFSFile(String filePath, String newfilePath) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    public ArrayList< Map.Entry<String, String> > removeDFSFile(String filePath) throws RemoteException, FileNotFoundException;
    public ArrayList< Map.Entry<String, String> > lookupFileBlocks(String filePath) throws RemoteException, FileNotFoundException;
    //public ArrayList< Map.Entry<String, Boolean> > lsDFSDirectory(String path) throws RemoteException, FileNotFoundException;

}
