package main.java.com.zzp.simpledfs;

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
    int registerUser(String username, String password) throws RemoteException;
    int unRegisterUser(String username, String password) throws RemoteException;
    void addDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    void delDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    boolean ifExistsDFSDirectory(String path) throws RemoteException;
    ArrayList< Map.Entry<String, String> > newDFSFileMapping(String filePath, int blocks_num, boolean ifLittleFile) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    void renameDFSFile(String filePath, String newfilePath) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    ArrayList< Map.Entry<String, String> > removeDFSFile(String filePath) throws RemoteException, FileNotFoundException;
    ArrayList< Map.Entry<String, String> > lookupFileBlocks(String filePath) throws RemoteException, FileNotFoundException;
    ArrayList< Map.Entry<String, Boolean> > lsDFSDirectory(String path) throws RemoteException, FileNotFoundException;
}
