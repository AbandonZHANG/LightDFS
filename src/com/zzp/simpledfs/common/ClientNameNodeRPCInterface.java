package com.zzp.simpledfs.common;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;

public interface ClientNameNodeRPCInterface extends Remote {
    boolean
        registerUser(String username, String password)
            throws RemoteException;
    boolean
        unRegisterUser(String username, String password)
            throws RemoteException, UserNotFoundException;
    boolean
        login(String userName, String password)
            throws RemoteException;
    long
        getUserTotalSpace(String loginUserName)
            throws RemoteException, UserNotFoundException;
    long
        getUserUsedSpace(String loginUserName)
            throws RemoteException, UserNotFoundException;
    void
        setUserTotalSpace(String userName, long totalSpace)
            throws RemoteException, UserNotFoundException;
    void
        addDFSDirectory(String userName, String path)
            throws RemoteException, FileNotFoundException, FileAlreadyExistsException, UserNotFoundException;
    void
        delDFSDirectory(String userName, String path)
            throws RemoteException, FileNotFoundException, UserNotFoundException;
    boolean
        ifExistsDFSINode(String userName, String path)
            throws RemoteException, UserNotFoundException;
    void
    renameDFSFile(String userName, String filePath, String newFilePath)
            throws RemoteException, UserNotFoundException, FileNotFoundException, FileAlreadyExistsException;
    ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> >
        newDFSFileMapping(String userName, String filePath, long fileSize, int blockNum)
            throws RemoteException, UserNotFoundException, NoEnoughSpaceException, FileNotFoundException, FileAlreadyExistsException;
    ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> >
        removeDFSFile(String userName, String filePath)
            throws RemoteException, UserNotFoundException, FileNotFoundException;
    ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> >
        lookupFileBlocks(String userName, String filePath)
            throws RemoteException, UserNotFoundException, FileNotFoundException;
    DFSINode
        getDFSINode(String userName, String path)
            throws RemoteException, UserNotFoundException, FileNotFoundException;
}
