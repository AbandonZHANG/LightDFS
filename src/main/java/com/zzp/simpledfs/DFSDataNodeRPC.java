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

public class DFSDataNodeRPC extends UnicastRemoteObject implements ClientDataNodeRPCInterface, Runnable{
    DFSDataNodeRPC() throws RemoteException {
        super();
    }

    Registry registry;  // RMI Registry
    String datanodeIp, datanodePort, namenodeIp, namenodePort;
    String blocksDirectory, absoluteBlockDirectory;  // 数据块存储目录
    ArrayList<String> blocks;  // 此数据节点上的数据块列表
    String datanodeID;  // 设备标识：DN-IP-Port

    private class JumpProperties{
        String datanodeName;
        int perSeconds;     // 每隔N ms发送一次心跳
        int errorNumToQuit; //连续N次没有连接上NameNode就退出
    }
    private final JumpProperties jumpProperties = new JumpProperties();

    public void run(){
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
            //生成 DataNode ID
            datanodeID = "DN-" + datanodeIp + "-" + datanodePort;

            namenodeIp = props.getProperty("namenodeip");
            namenodePort = props.getProperty("namenodeport");

            blocksDirectory = props.getProperty("blockdir");
            absoluteBlockDirectory = System.getProperty("user.dir")+"\\"+blocksDirectory;

            jumpProperties.perSeconds = Integer.valueOf(props.getProperty("perseconds"));
            jumpProperties.errorNumToQuit = Integer.valueOf(props.getProperty("errornumtoquitjump"));
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
        myState.datanodeID = datanodeID;
        myState.ip = datanodeIp;
        myState.port = datanodePort;
        File workDir = new File(System.getProperty("user.dir"));
        myState.totalSpace = workDir.getTotalSpace();
        myState.freeSpace = workDir.getFreeSpace();
        myState.usedSpace = workDir.getUsableSpace();
        return myState;
    }
    /**
     * 向主控服务器请求连接
     */
    public boolean register(){
        System.out.println("[INFO] Sending register application to the namenode ...");
        DFSDataNodeState myState = getCurrentState();
        try{
            DataNodeNameNodeRPCInterface datanodeRPC = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
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
            DataNodeNameNodeRPCInterface datanodeRPC = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
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
            DataNodeNameNodeRPCInterface datanodeRPC = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
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
        DFSDataNodeJump jump = new DFSDataNodeJump(jumpProperties.datanodeName, jumpProperties.perSeconds, jumpProperties.errorNumToQuit);
        // Thread类的start()才能实现线程,run()只是普通的类调用
        jump.start();
    }
    public void unRegister(){
        System.out.println("[INFO] Sending unregister request to the namenode ...");
        try{
            DataNodeNameNodeRPCInterface datanodeRPC = (DataNodeNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
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
        byte[] res = null;
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
