package main.java.com.zzp.simpledfs;

import java.io.*;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by Zhipeng Zhang on 15/05/26 0026.
 */
public class DFSDataNodeRPC extends UnicastRemoteObject implements ClientDataNodeRPCInterface, Runnable{
    Registry registry;                              // RMI Registry
    String datanodeIp, datanodePort,namenodeIp,namenodePort;
    String blocksDirectory,absoluteBlockDirectory;  // 数据块存储目录
    ArrayList<String> blocks;  // 此数据节点上的数据块列表
    String datanodeID;  // 设备标识：DN-IP-Port
    final DFSDataNodeJump jump = new DFSDataNodeJump();

    DFSDataNodeRPC() throws RemoteException {
        super();
    }
    public void run(){
        if(register()){
            System.out.println("[SUCCESS!] Register Successful!");
        }
        else{
            System.out.println("[ERROR!] Register Refused! Check if the node is in the include file of namenode.");
            return ;
        }

        sendBlockRecord();

        sendNodeStates();

        try{
            // 启动 rmiregistry
            registry = LocateRegistry.createRegistry(2021);
            // 绑定 RMI 服务
            Naming.rebind("rmi://localhost:2021/DFSDataNode", this);
            //System.out.println("The DataNode RMI is running...");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void initialize(){
        // 读取配置文件datanode_properties.xml
        readConfigFile();

        // 初始化，读取数据块目录
        blocks = new ArrayList<String>();
        loadLocalBlocks(new File(absoluteBlockDirectory));

        //生成 DataNode ID
        datanodeID = "DN-" + datanodeIp + "-" + datanodePort;
    }
    /**
     * 读取 Properties 配置文件:datanode.xml
     */
    public void readConfigFile(){
        Properties props = new Properties();
        try{
            InputStream fin = new FileInputStream("datanode.xml");
            props.loadFromXML(fin);
            fin.close();
            datanodeIp = props.getProperty("ip");
            if(datanodeIp.equals(""))
                datanodeIp = InetAddress.getLocalHost().toString();
            datanodePort = props.getProperty("port");
            namenodeIp = props.getProperty("namenodeip");
            namenodePort = props.getProperty("namenodeport");
            blocksDirectory = props.getProperty("blockdir");
            absoluteBlockDirectory = System.getProperty("user.dir")+"\\"+blocksDirectory;
        }
        catch (Exception e){
            System.out.println("[ERROR!] Read config file error!");
        }
    }
    /**
     * 读取本地数据块列表
     * @param path
     */
    public void loadLocalBlocks(File path){
        blocks.clear();
        if(!path.isDirectory()){
            return;
        }
        else{
            File[] blockFiles = path.listFiles();
            for(File blockFile:blockFiles){
                if(blockFile.isDirectory()){
                }
                else{
                    blocks.add(blockFile.getName());
                }
            }
        }
    }
    /**
     * 列出当前数据节点中的所有数据块列表
     */
    public void listBlocks(){
        System.out.println("This DataNode has following blocks:");
        for(String block:blocks){
            System.out.println("[Blocks]"+block);
        }
    }
    /**
     * 向主控服务器请求连接
     */
    public boolean register(){
        System.out.println("[INFO] Sending register application to the namenode ...");
        try{
            DFSDataNodeState myState = jump.mystate;
            myState.ip = datanodeIp;
            myState.port = datanodePort;
            myState.datanodeID = datanodeID;
            DataNodeNameNodeRPCInterface datanode = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
            return datanode.registerDataNode(myState);
        }
        catch (Exception e){
            System.out.println("[ERROR!] The Namenode RMI serve is not found!");
        }
        return false;
    }
    /**
     * 向主控服务器发送本地数据块目录
     */
    public void sendBlockRecord(){
        System.out.println("[INFO] Sending block records to the namenode ...");
        try{
            DataNodeNameNodeRPCInterface datanode = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
            datanode.sendDataNodeBlockList(datanodeID, blocks);
            System.out.println("[SUCCESS!] Sending block records successful! ...");
        }
        catch (Exception e){
            System.out.println("[ERROR!] The Namenode RMI serve is not found!");
        }
    }

    /**
     * 向主控服务器发送心跳包
     */
    public void sendNodeStates(){
        System.out.println("[INFO] Sending node states to the namenode ...");
        jump.workingDirectory = System.getProperty("user.dir");;     // 设置当前目录为数据服务器工作目录
        jump.mystate.ip = datanodeIp;
        jump.mystate.port = datanodePort;
        jump.mystate.datanodeID = datanodeID;
        // Thread类的start()才能实现线程,run()只是普通的类调用
        jump.start();
    }
    public void unRegister(){
        System.out.println("[INFO] Sending unregister request to the namenode ...");
        try{
            DataNodeNameNodeRPCInterface datanode = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
            datanode.unRegisterDataNode(datanodeID);
        }
        catch (Exception e){
            System.out.println("[ERROR!] The Namenode RMI serve is not found!");
        }
    }
    public void close(){
        // 向NameNode发送注销申请
        unRegister();
        // 关闭RMI服务
        try{
            UnicastRemoteObject.unexportObject(registry, true);
            //System.out.println("The DataNode RMI is done...");
            System.exit(0);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    @Override
    public void uploadBlock(byte[] content, String blockName)throws RemoteException{
        File wfile = new File(absoluteBlockDirectory+"\\"+blockName);
        if(!wfile.getParentFile().exists()){
            try{
                wfile.getParentFile().mkdirs();
            }
            catch (Exception e){
                throw new RemoteException("Failed to create remote directory!");
            }
        }
        if(!wfile.exists()){
            try{
                wfile.createNewFile();
            }
            catch (Exception e){
                throw new RemoteException("Failed to create remote file!");
            }
        }
        try{
            BufferedOutputStream bufOut = new BufferedOutputStream(new FileOutputStream(wfile));
            bufOut.write(content);
            bufOut.flush();
            bufOut.close();
            // 向NameNode发送更新后的数据块目录
            sendBlockRecord();
        }
        catch (Exception e){
            throw new RemoteException("Failed to write remote data!");
        }
    }
    @Override
    public byte[] downloadBlock(String blockName)throws RemoteException, FileNotFoundException{
        File rFile = new File(absoluteBlockDirectory+"\\"+blockName);
        byte[] res = null;
        if(!rFile.exists()){
            throw new FileNotFoundException("Block not found!");
        }
        else{
            res = new byte[(int)rFile.length()];
            BufferedInputStream bufIn = new BufferedInputStream(new FileInputStream(rFile));
            try {
                bufIn.read(res);
                return res;
            }
            catch (Exception e){
                throw new RemoteException("Read Error!");
            }
        }
    }
    @Override
    public void deleteBlock(String blockName)throws RemoteException, FileNotFoundException{
        File rFile = new File(absoluteBlockDirectory+"\\"+blockName);
        byte[] res = null;
        if(!rFile.exists()){
            throw new FileNotFoundException("Block not found!");
        }
        else{
            rFile.delete();
            // 向NameNode发送更新后的数据块目录
            sendBlockRecord();
        }
    }
}
