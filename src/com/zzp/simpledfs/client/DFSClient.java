package com.zzp.simpledfs.client;

import com.zzp.simpledfs.common.ClientDataNodeRPCInterface;
import com.zzp.simpledfs.common.ClientNameNodeRPCInterface;
import com.zzp.simpledfs.common.DFSINode;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;

public class DFSClient {
    public static DFSClient getInstance() throws RemoteException{
        return new DFSClient();
    }
    private ClientNameNodeRPCInterface clientRmi;
    private String loginUserName;    // 当前登陆用户名
    private String namenodeIp, namenodePort;  // 连接的NameNode ip, port
    private int blockSize;
    DFSClient() throws RemoteException{
        // 读取配置文件client.xml
        readConfigFile("client.xml");

        try{
            clientRmi = (ClientNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
        }
        catch (NotBoundException e){

        }
        catch (MalformedURLException a){

        }
        //clientRmi = (ClientNameNodeRmiInterface) Naming.lookup("rmi://"+namenodeIp+":"+namenodePort+"/DFSNameNode");
    }
    private void readConfigFile(String file){
        Properties props = new Properties();
        try{
            InputStream fin = new FileInputStream(file);
            props.loadFromXML(fin);
            fin.close();
            namenodeIp = props.getProperty("namenodeip");
            namenodePort = props.getProperty("namenodeport");
            blockSize = Integer.valueOf(props.getProperty("blocksize"));
            blockSize *= 1024*1024;     // MB换算成Byte
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static boolean registerUser(String userName, String password) throws RemoteException{
        boolean res = false;
        try{
            ClientNameNodeRPCInterface clientRmi = (ClientNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");;
            res = clientRmi.registerUser(userName, DFSBase64.Base64Encode(password));
        }
        catch (RemoteException e){
            throw e;
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return res;
    }

    public boolean unRegisterUser(String userName, String password) throws RemoteException{
        boolean res = false;
        res = clientRmi.unRegisterUser(userName, DFSBase64.Base64Encode(password));
        return res;
    }

    public boolean login(String userName, String password) throws RemoteException{
        boolean res = false;
        res = clientRmi.login(userName, DFSBase64.Base64Encode(password));
        if(res){
            loginUserName = userName;
        }
        return res;
    }

    public void logout(){
        loginUserName = null;
    }

    public long getUserTotalSpace() throws RemoteException{
        if(loginUserName == null)
            return -1;
        else
            return clientRmi.getUserTotalSpace(loginUserName);
    }
    public long getUserUsedSpace() throws RemoteException{
        if(loginUserName == null)
            return -1;
        else
            return clientRmi.getUserUsedSpace(loginUserName);
    }
    public boolean setUserTotalSpace(long totalSpace) throws RemoteException{
        if(loginUserName == null)
            return false;
        else
            return clientRmi.setUserTotalSpace(loginUserName, totalSpace);
    }
    /**
     * @param dfsPath DFS目录绝对路径
     */
    public boolean mkdir(String dfsPath) throws RemoteException, FileAlreadyExistsException, FileNotFoundException{
        if(loginUserName == null)
            return false;
        else{
            clientRmi.addDFSDirectory(loginUserName, dfsPath);
            return true;
        }
    }

    public boolean rmdir(String dfsPath) throws RemoteException, FileNotFoundException{
        if(loginUserName == null)
            return false;
        else {
            clientRmi.delDFSDirectory(loginUserName, dfsPath);
            return true;
        }
    }

    /**
     * 检查该DFS路径是否存在
     * @param dfsPath DFS目录绝对路径
     */
    public boolean checkdir(String dfsPath) throws RemoteException{
        boolean res = false;
        if(loginUserName == null)
            return false;
        res = clientRmi.ifExistsDFSINode(loginUserName, dfsPath);
        return res;
    }

    public DFSINode getINode(String dfsPath) throws RemoteException, FileNotFoundException{
        if(loginUserName == null)
            return null;
        return clientRmi.getDFSINode(loginUserName, dfsPath);
    }

    /**
     * @param originPath 原DFS目录绝对路径
     * @param newPath 新DFS目录绝对路径
     */
    public boolean renameFile(String originPath, String newPath) throws FileNotFoundException, FileAlreadyExistsException, RemoteException{
        if(loginUserName == null)
            return false;
        clientRmi.renameDFSFile(loginUserName, originPath, newPath);
        return true;
    }

    /**
     * @param localFilePath 本地文件绝对路径或者相对路径
     * @param dfsPath DFS目录绝对路径
     */
    public boolean uploadFile(String localFilePath, String dfsPath) throws RemoteException, NotBoundException, FileAlreadyExistsException, FileNotFoundException {
        if(loginUserName == null)
            return false;
        try{
            File localFile = new File(localFilePath);
            // 读取本地文件返回字节流
            ArrayList<byte[]> byteblocks = transToByte(localFile);
            // 向NameNode发送请求，新建Inode文件节点，分配数据块标识，分配数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.newDFSFileMapping(loginUserName, dfsPath, localFile.length(), byteblocks.size());
            if(blockDatanodes == null){
                throw new FileNotFoundException();
            }
            int i = 0;
            for(Map.Entry<String, String> eachTrans:blockDatanodes){
                String block = eachTrans.getKey();
                //String datanodeip = eachTrans.getValue();
                ClientDataNodeRPCInterface transRmi = (ClientDataNodeRPCInterface)Naming.lookup("rmi://localhost:2021/DFSDataNode");
                //ClientDataNodeRmiInterface transRmi = (ClientDataNodeRmiInterface)Naming.lookup("rmi://"+datanodeip+":2021/DFSDataNode");
                transRmi.uploadBlock(byteblocks.get(i), block);
                i ++;
            }
        }
        catch (MalformedURLException e){
            e.printStackTrace();
        }
        return true;
    }

    /**
     * @param dfsPath DFS目录绝对路径
     * @param localPath 本地绝对路径或者相对路径
     */
    public boolean downloadFile(String dfsPath, String localPath) throws RemoteException, NotBoundException, FileNotFoundException{
        if(loginUserName == null)
            return false;
        // 向NameNode询问某文件的数据块标识及数据节点
        // 返回数据块标识和存储数据节点
        ArrayList<Map.Entry<String, String>> blockDatanodes = clientRmi.lookupFileBlocks(loginUserName, dfsPath);
        File wfile = new File(localPath);
        BufferedOutputStream bufOut = new BufferedOutputStream(new FileOutputStream(wfile));

        for (Map.Entry<String, String> blockDatanode : blockDatanodes) {
            String block = blockDatanode.getKey();
            //String datanodeip = blockDatanode.getValue();
            ClientDataNodeRPCInterface transRmi = null;

            // !!!避免lookup重复查询
            try{
                transRmi = (ClientDataNodeRPCInterface) Naming.lookup("rmi://localhost:2021/DFSDataNode");
            }
            catch (MalformedURLException e){
                e.printStackTrace();
            }
            //ClientDataNodeRmiInterface transRmi = (ClientDataNodeRmiInterface)Naming.lookup("rmi://"+datanodeip+":2021/DFSDataNode");
            byte[] content = transRmi.downloadBlock(block);
            try{
                bufOut.write(content);
            }
            catch (IOException e){
                e.printStackTrace();
            }
            return true;
        }
        try{
            bufOut.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @param dfsPath DFS目录绝对路径
     */
    public void removeFile(String dfsPath) throws RemoteException, FileNotFoundException, NotBoundException, MalformedURLException{
        // 向NameNode询问某文件的数据块标识及数据节点
        // 返回数据块标识和存储数据节点
        ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.removeDFSFile(loginUserName, dfsPath);
        for (Map.Entry<String, String> blockDatanode:blockDatanodes){
            String block = blockDatanode.getKey();
            //String datanodeip = blockDatanode.getValue();
            ClientDataNodeRPCInterface transRmi = (ClientDataNodeRPCInterface)Naming.lookup("rmi://localhost:2021/DFSDataNode");
            //ClientDataNodeRmiInterface transRmi = (ClientDataNodeRmiInterface)Naming.lookup("rmi://"+datanodeip+":2021/DFSDataNode");
            transRmi.deleteBlock(block);
        }
    }
    /**
     * 读入本地文件，将字节流存在byte[]中
     * @param localFile 本地文件的绝对路径或相对路径
     * @return 返回切割后的byte数据块列表
     */
    private ArrayList<byte[]> transToByte(File localFile) throws FileNotFoundException{
        ArrayList<byte[]> byteBlocks = new ArrayList<byte[]>();

        // 计算切割blocks个数
        long bytelength = localFile.length();
        int blocks_num = (int)(bytelength / blockSize);   // block大小为64M
        if (localFile.length() % blockSize != 0)
            blocks_num++;
        BufferedInputStream bufIn = new BufferedInputStream(new FileInputStream(localFile));

        try{
            for(int i = 0; i < blocks_num; ++i){
                int blockLength = (int)Math.min((long)blockSize, bytelength);
                byte[] content = new byte[blockLength];    // 每个byte 64M
                bufIn.read(content, 0, blockLength);
                byteBlocks.add(content);
                bytelength -= blockSize;
            }
            bufIn.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return byteBlocks;
    }
}
