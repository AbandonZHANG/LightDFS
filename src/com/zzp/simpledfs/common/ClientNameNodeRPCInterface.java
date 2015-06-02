package com.zzp.simpledfs.common;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;

public interface ClientNameNodeRPCInterface extends Remote {
    boolean
        registerUser(String username, String password) throws RemoteException;
    boolean
        unRegisterUser(String username, String password) throws RemoteException;
    boolean
        login(String userName, String password) throws RemoteException;
    long
        getUserTotalSpace(String loginUserName) throws RemoteException;
    long
        getUserUsedSpace(String loginUserName) throws RemoteException;
    boolean
        setUserTotalSpace(String userName, long totalSpace) throws RemoteException;
    void
        addDFSDirectory(String userName, String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    void
        delDFSDirectory(String userName, String path) throws RemoteException, FileNotFoundException;
    boolean
        ifExistsDFSINode(String userName, String path) throws RemoteException;
    ArrayList< Map.Entry<String, String> >
        newDFSFileMapping(String userName, String filePath, long fileSize, int blockNum) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    void
        renameDFSFile(String userName, String filePath, String newFilePath) throws RemoteException, FileNotFoundException, FileAlreadyExistsException;
    ArrayList< Map.Entry<String, String> >
        removeDFSFile(String userName, String filePath) throws RemoteException, FileNotFoundException;
    ArrayList< Map.Entry<String, String> >
        lookupFileBlocks(String userName, String filePath) throws RemoteException, FileNotFoundException;
    DFSINode
        getDFSINode(String userName, String path) throws RemoteException, FileNotFoundException;
}
