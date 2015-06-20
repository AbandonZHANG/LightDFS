package com.zzp.lightdfs.client;

import com.zzp.lightdfs.common.*;

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
            clientRmi = (ClientNameNodeRPCInterface) Naming.lookup("rmi://"+namenodeIp+":"+namenodePort+"/DFSNameNode");
        }
        catch (RemoteException e){
            throw e;
        }
        catch (Exception e){
            e.printStackTrace();
        }
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

    /***************************  用户管理  *************************************/
    public boolean registerUser(String userName, String password) throws RemoteException{
        boolean res = false;
        try{
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

    public boolean login(String userName, String password) throws RemoteException{
        boolean res;
        res = clientRmi.login(userName, DFSBase64.Base64Encode(password));
        if(res){
            loginUserName = userName;
        }
        return res;
    }

    public void logout(){
        loginUserName = null;
    }

    public boolean changePassword(String password, String newPassword) throws RemoteException, UserNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        else
            return clientRmi.changePassword(loginUserName, DFSBase64.Base64Encode(password), DFSBase64.Base64Encode(newPassword));
    }

    public long getUserTotalSpace() throws RemoteException, UserNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        else
            return clientRmi.getUserTotalSpace(loginUserName);
    }

    public long getUserUsedSpace() throws RemoteException, UserNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        else
            return clientRmi.getUserUsedSpace(loginUserName);
    }

    public void setUserTotalSpace(long totalSpace) throws RemoteException, UserNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        else
            clientRmi.setUserTotalSpace(loginUserName, totalSpace);
    }


    /***************************  目录管理  *************************************/
    /**
     * @param dfsPath DFS目录绝对路径
     */
    public void mkdir(String dfsPath) throws RemoteException, UserNotFoundException, FileAlreadyExistsException, FileNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        else{
            clientRmi.addDFSDirectory(loginUserName, dfsPath);
        }
    }

    public void rmdir(String dfsPath) throws RemoteException, UserNotFoundException, FileNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        else {
            clientRmi.deleteDFSDirectory(loginUserName, dfsPath);
        }
    }

    public void cldir(String dfsPath) throws RemoteException, UserNotFoundException, FileNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        else {
            clientRmi.clearDFSDirectory(loginUserName, dfsPath);
        }
    }

    /**
     * 检查该DFS路径是否存在
     * @param dfsPath DFS目录绝对路径
     */
    public boolean checkdir(String dfsPath) throws RemoteException, UserNotFoundException{
        boolean res = false;
        if(loginUserName == null)
            throw new UserNotFoundException();
        res = clientRmi.ifExistsDFSINode(loginUserName, dfsPath);
        return res;
    }

    public DFSINode getINode(String dfsPath) throws RemoteException, UserNotFoundException, FileNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        return clientRmi.getDFSINode(loginUserName, dfsPath);
    }

    public boolean renameDir(String originPath, String newName) throws FileNotFoundException, UserNotFoundException, FileAlreadyExistsException, RemoteException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        clientRmi.renameDFSINode(loginUserName, originPath, newName, false);
        return true;
    }


    /***************************  文件管理  *************************************/
    /**
     * @param localFilePath 本地文件绝对路径或者相对路径
     * @param dfsPath DFS目录绝对路径
     */
    public boolean uploadFile(String localFilePath, String dfsPath) throws RemoteException, NoEnoughSpaceException, UserNotFoundException, NotBoundException, FileAlreadyExistsException, FileNotFoundException {
        if(loginUserName == null)
            throw new UserNotFoundException();
        try{
            File localFile = new File(localFilePath);
            // 读取本地文件返回字节流
            ArrayList<byte[]> byteblocks = transToByte(localFile);
            // 向NameNode发送请求，新建Inode文件节点，分配数据块标识，分配数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, DFSDataNodeRPCAddress> > blockDatanodes =  clientRmi.addDFSFile(loginUserName, dfsPath, localFile.length(), byteblocks.size());
            if(blockDatanodes == null){
                throw new FileNotFoundException();
            }
            int i = 0;
            for(Map.Entry<String, DFSDataNodeRPCAddress> eachTrans:blockDatanodes){
                String block = eachTrans.getKey();
                DFSDataNodeRPCAddress datanodeAddr = eachTrans.getValue();
                ClientDataNodeRPCInterface transRmi = (ClientDataNodeRPCInterface)Naming.lookup("rmi://"+datanodeAddr.getIp()+":"+datanodeAddr.getPort()+"/DFSDataNode");
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
    public void downloadFile(String dfsPath, String localPath) throws RemoteException, UserNotFoundException, NotBoundException, FileNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        // 向NameNode询问某文件的数据块标识及数据节点
        // 返回数据块标识和存储数据节点
        ArrayList<Map.Entry<String, DFSDataNodeRPCAddress> > blockDatanodes = clientRmi.lookupFileBlocks(loginUserName, dfsPath);
        File wfile = new File(localPath);
        BufferedOutputStream bufOut = new BufferedOutputStream(new FileOutputStream(wfile));

        for (Map.Entry<String, DFSDataNodeRPCAddress> blockDatanode : blockDatanodes) {
            String block = blockDatanode.getKey();
            DFSDataNodeRPCAddress datanodeAddr = blockDatanode.getValue();
            ClientDataNodeRPCInterface transRmi = null;

            // !!!避免lookup重复查询
            try{
                transRmi = (ClientDataNodeRPCInterface)Naming.lookup("rmi://"+datanodeAddr.getIp()+":"+datanodeAddr.getPort()+"/DFSDataNode");
            }
            catch (MalformedURLException e){
                e.printStackTrace();
            }
            byte[] content = transRmi.downloadBlock(block);
            try{
                bufOut.write(content);
                bufOut.flush();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
        try{
            bufOut.close();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

    public void removeFile(String dfsPath) throws RemoteException, UserNotFoundException, FileNotFoundException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        // 删除主控节点上的元数据
        clientRmi.deleteDFSFile(loginUserName, dfsPath);
    }

    /**
     * @param originPath 原DFS目录绝对路径
     * @param newName 新目录路径
     */
    public boolean renameFile(String originPath, String newName) throws FileNotFoundException, UserNotFoundException, FileAlreadyExistsException, RemoteException{
        if(loginUserName == null)
            throw new UserNotFoundException();
        clientRmi.renameDFSINode(loginUserName, originPath, newName, true);
        return true;
    }

    /**
     * 读入本地文件，将字节流存在byte[]中
     * @param localFile 本地文件的绝对路径或相对路径
     * @return 返回切割后的byte数据块列表
     */
    private ArrayList<byte[]> transToByte(File localFile) throws FileNotFoundException{
        ArrayList<byte[]> byteBlocks = new ArrayList<>();

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
