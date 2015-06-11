package com.zzp.simpledfs.common;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;

public interface ClientNameNodeRPCInterface extends Remote {
    /**************************** 用户管理 ***********************************/
    boolean
        registerUser(String username, String password)
            throws RemoteException;
    boolean
        login(String userName, String password)
            throws RemoteException;
    boolean
        changePassword(String userName, String password, String newPassword)
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

    /**************************** 目录管理 ***********************************/
    void
        addDFSDirectory(String userName, String path)
            throws RemoteException, FileNotFoundException, FileAlreadyExistsException, UserNotFoundException;
    void
        deleteDFSDirectory(String userName, String path)
            throws RemoteException, FileNotFoundException, UserNotFoundException;
    void
        clearDFSDirectory(String userName, String path)
            throws RemoteException, FileNotFoundException, UserNotFoundException;
    boolean
        ifExistsDFSINode(String userName, String path)
            throws RemoteException, UserNotFoundException;
    void
        renameDFSINode(String userName, String inodePath, String newINodeName, boolean ifFile)
            throws RemoteException, UserNotFoundException, FileNotFoundException, FileAlreadyExistsException;
    ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> >
        addDFSFile(String userName, String filePath, long fileSize, int blockNum)
            throws RemoteException, NotBoundException, UserNotFoundException, NoEnoughSpaceException, FileNotFoundException, FileAlreadyExistsException;
    void
        deleteDFSFile(String userName, String filePath)
            throws RemoteException, UserNotFoundException, FileNotFoundException;
    ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> >
        lookupFileBlocks(String userName, String filePath)
            throws RemoteException, NotBoundException, UserNotFoundException, FileNotFoundException;
    DFSINode
        getDFSINode(String userName, String path)
            throws RemoteException, UserNotFoundException, FileNotFoundException;
}
