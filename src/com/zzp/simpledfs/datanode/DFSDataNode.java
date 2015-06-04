package com.zzp.simpledfs.datanode;

import com.zzp.simpledfs.common.DFSDataNodeState;
import com.zzp.simpledfs.common.ClientDataNodeRPCInterface;
import com.zzp.simpledfs.common.DataNodeNameNodeRPCInterface;

import java.io.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;

public class DFSDataNode extends UnicastRemoteObject implements ClientDataNodeRPCInterface, Runnable{
    DFSDataNode() throws RemoteException {
        super();
    }
    Registry registry;  // RMI Registry
    DataNodeNameNodeRPCInterface datanodeRPC;
    String datanodeIp, datanodePort, namenodeIp, namenodePort;
    String blocksDirectory, absoluteBlockDirectory;  // 数据块存储目录
    ArrayList<String> blocks;  // 此数据节点上的数据块列表
    String datanodeID;

    private class JumpProperties{
        String datanodeName;
        int perSeconds;     // 每隔N ms发送一次心跳
    }
    private final JumpProperties jumpProperties = new JumpProperties();

    public void run(){
        try{
            datanodeRPC = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://"+namenodeIp+":"+namenodePort+"/DFSNameNode");
        }
        catch (Exception e){
            System.out.println("[ERROR!] The Namenode RMI serve is not found!");
        }

        if(register()){
            System.out.println("[SUCCESS!] Register Successful!");
        }
        else{
            System.out.println("[ERROR!] Register Refused! Check if the node is in the include file of namenode.");
            return ;
        }

        sendBlockRecord();

        sendJump();

        try{
            // 启动 rmiregistry
            registry = LocateRegistry.createRegistry(Integer.valueOf(datanodePort));
            // 绑定 RMI 服务
            Naming.rebind("rmi://"+datanodeIp+":"+datanodePort+"/DFSDataNode", this);
            //System.out.println("The DataNode RMI is running...");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void initialize(){
        // 读取or生成数据节点全局唯一ID
        readDataNodeID();

        // 读取配置文件datanode_properties.xml
        readConfigFile();

        // 初始化，读取数据块目录
        blocks = new ArrayList<String>();
        loadLocalBlocks(new File(absoluteBlockDirectory));
    }
    private void readDataNodeID(){
        File datanodeIDFile = new File("datanodecore");
        try {
            if (datanodeIDFile.exists()) {
                Scanner in = new Scanner(new FileReader(datanodeIDFile));
                datanodeID = in.next();
                in.close();
            } else {
                datanodeIDFile.createNewFile();
                datanodeID = "DN" + UUID.randomUUID().toString();
                BufferedWriter bufWriter = new BufferedWriter(new FileWriter(datanodeIDFile));
                bufWriter.write(datanodeID);
                bufWriter.close();
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    private void readConfigFile(){
        Properties props = new Properties();
        try{
            InputStream fin = new FileInputStream("datanode.xml");
            props.loadFromXML(fin);
            fin.close();
            datanodeIp = props.getProperty("ip");
            datanodePort = props.getProperty("port");

            namenodeIp = props.getProperty("namenodeip");
            namenodePort = props.getProperty("namenodeport");

            blocksDirectory = props.getProperty("blockdir");
            absoluteBlockDirectory = System.getProperty("user.dir")+"\\"+blocksDirectory;

            jumpProperties.perSeconds = Integer.valueOf(props.getProperty("perseconds"));
            jumpProperties.datanodeName = datanodeID;
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
        if(path.isDirectory()){
            File[] blockFiles = path.listFiles();
            if(blockFiles != null) {
                for (File blockFile : blockFiles) {
                    if (!blockFile.isDirectory()) {
                        blocks.add(blockFile.getName());
                    }
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
    public DFSDataNodeState getCurrentState(){
        DFSDataNodeState myState = new DFSDataNodeState();
        File workDir = new File(System.getProperty("user.dir"));
        myState.setDatanodeID(datanodeID);
        myState.setIp(datanodeIp);
        myState.setPort(datanodePort);
        myState.setTotalSpace(workDir.getTotalSpace());
        myState.setFreeSpace(workDir.getFreeSpace());
        myState.setUsedSpace(workDir.getUsableSpace());
        return myState;
    }
    /**
     * 向主控服务器请求连接
     */
    public boolean register(){
        System.out.println("[INFO] Sending register application to the namenode ...");
        DFSDataNodeState myState = getCurrentState();
        try{
            return datanodeRPC.registerDataNode(myState);
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
            datanodeRPC.sendDataNodeBlockList(datanodeID, blocks);
            System.out.println("[SUCCESS!] Sending block records successful! ...");
        }
        catch (Exception e){
            System.out.println("[ERROR!] The Namenode RMI serve is not found!");
        }
    }
    public void sendNodeStates(){
        DFSDataNodeState myState = getCurrentState();
        try {
            datanodeRPC.sendDataNodeStates(myState);
        }
        catch (Exception e){
            System.out.println("[ERROR!] The Namenode RMI serve is not found!");
        }
    }
    /**
     * 向主控服务器发送心跳包。心跳包不包含任何信息，仅为确认DataNode存活
     */
    public void sendJump(){
        System.out.println("[INFO] Sending node states to the namenode ...");
        DFSDataNodeJump jump = new DFSDataNodeJump(jumpProperties.datanodeName, namenodeIp, namenodePort, jumpProperties.perSeconds);
        // Thread类的start()才能实现线程,run()只是普通的类调用
        jump.start();
    }
    public void unRegister(){
        System.out.println("[INFO] Sending unregister request to the namenode ...");
        try{
            datanodeRPC.unRegisterDataNode(datanodeID);
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
            System.exit(0);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @param content
     * @param blockName
     * @throws RemoteException
     */
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
        if(wfile.exists()){
            wfile.delete();
        }
        try{
            wfile.createNewFile();
            BufferedOutputStream bufOut = new BufferedOutputStream(new FileOutputStream(wfile));
            bufOut.write(content);
            bufOut.flush();
            bufOut.close();
            // 发送更新后的数据块目录
            sendBlockRecord();
            // 发送更新后的硬盘空间
            sendNodeStates();
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
        if(!rFile.exists()){
            throw new FileNotFoundException("Block not found!");
        }
        else{
            rFile.delete();
            // 发送更新后的数据块目录
            sendBlockRecord();
            // 发送更新后的硬盘空间
            sendNodeStates();
        }
    }
}
