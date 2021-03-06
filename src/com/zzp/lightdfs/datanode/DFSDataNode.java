package com.zzp.lightdfs.datanode;

import com.zzp.lightdfs.common.*;

import java.io.*;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class DFSDataNode extends UnicastRemoteObject implements ClientDataNodeRPCInterface, DataNodesRPCInterface, NameNodeDataNodeRPCInterface, Runnable{
    public static DFSDataNode getInstance(String hostIp) throws RemoteException{
        // 设定RMI服务器ip地址
        System.setProperty("java.rmi.server.hostname",hostIp);
        return new DFSDataNode();
    }

    private DFSDataNode() throws RemoteException {
        super();
    }

    Registry registry, datanodeRegistry, namenodeRegistry;  // RMI Registry
    DataNodeNameNodeRPCInterface datanodeRPC;
    String datanodeIp, clientRPCPort, datanodeRPCPort, namenodeRPCPort, namenodeIp, namenodePort;
    String blocksDirectory, absoluteBlockDirectory;  // 数据块存储目录
    ArrayList<DFSBlock> blocks;  // 此数据节点上的数据块列表
    String datanodeID;
    DFSDataNodeJump jump;
    private final JumpProperties jumpProperties = new JumpProperties();

    private class JumpProperties{
        String datanodeName;
        int perSeconds;     // 每隔N ms发送一次心跳
    }

    public void run(){
        try{
            // 启动 rmiregistry
            registry = LocateRegistry.createRegistry(Integer.valueOf(clientRPCPort));
            // 绑定 RMI 服务
            Naming.rebind("rmi://"+datanodeIp+":"+clientRPCPort+"/DFSDataNode", this);
            // 启动 rmiregistry
            datanodeRegistry = LocateRegistry.createRegistry(Integer.valueOf(datanodeRPCPort));
            // 绑定 RMI 服务
            Naming.rebind("rmi://"+datanodeIp+":"+datanodeRPCPort+"/DFSDataNodeToDataNode", this);
            // 启动 rmiregistry
            namenodeRegistry = LocateRegistry.createRegistry(Integer.valueOf(namenodeRPCPort));
            // 绑定 RMI 服务
            Naming.rebind("rmi://"+datanodeIp+":"+namenodeRPCPort+"/DFSDataNodeToNameNode", this);

            System.out.println("The DataNode RMI is running...");
        }
        catch (Exception e){
            e.printStackTrace();
        }

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
    }

    public void initialize(){
        // 读取or生成数据节点全局唯一ID
        readDataNodeID();

        // 读取配置文件datanode_properties.xml
        readConfigFile();

        // 初始化，读取数据块目录
        blocks = new ArrayList<>();
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
            clientRPCPort = props.getProperty("clientrpcport");
            datanodeRPCPort = props.getProperty("datanoderpcport");
            namenodeRPCPort = props.getProperty("namenoderpcport");
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

    public void loadLocalBlocks(File path){
        blocks.clear();
        if(path.isDirectory()){
            File[] blockFiles = path.listFiles();
            if(blockFiles != null) {
                for (File blockFile : blockFiles) {
                    if (!blockFile.isDirectory()) {
                        blocks.add(new DFSBlock(datanodeID, blockFile.getName(), blockFile.length()));
                    }
                }
            }
        }
    }

    public DFSDataNodeState getCurrentState(){
        DFSDataNodeState myState = new DFSDataNodeState();
        File workDir = new File(System.getProperty("user.dir"));
        myState.setDatanodeID(datanodeID);
        myState.addr.setIp(datanodeIp);
        myState.addr.setPort(clientRPCPort);
        myState.addr.setDatanoderpcport(datanodeRPCPort);
        myState.addr.setNamenoderpcport(namenodeRPCPort);
        myState.setTotalSpace(workDir.getTotalSpace());
        myState.setFreeSpace(workDir.getFreeSpace());
        myState.setUsedSpace(workDir.getUsableSpace());
        return myState;
    }

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

    public void sendBlockRecord(){
        loadLocalBlocks(new File(absoluteBlockDirectory));
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

    public void sendJump(){
        System.out.println("[INFO] Sending datanode jump...");
        jump = new DFSDataNodeJump(jumpProperties.datanodeName, this, jumpProperties.perSeconds);
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
        if(jump != null)
            jump.ifRun = false;
        // 关闭RMI服务
        try{
            if(registry != null)
                UnicastRemoteObject.unexportObject(registry, true);
            if(datanodeRegistry != null)
                UnicastRemoteObject.unexportObject(datanodeRegistry, true);
            if(namenodeRegistry != null)
                UnicastRemoteObject.unexportObject(namenodeRegistry, true);
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
        byte[] res;
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
    public void deleteBlock(String blockName) throws RemoteException, FileNotFoundException{
        File rFile = new File(absoluteBlockDirectory+"\\"+blockName);
        if(!rFile.exists()){
            throw new FileNotFoundException("Block not found!");
        }
        else{
            if(!rFile.delete()){
                throw new RemoteException();
            }
            // 发送更新后的数据块目录
            sendBlockRecord();
            // 发送更新后的硬盘空间
            sendNodeStates();
        }
    }

    @Override
    public void sendBlocks(HashMap<String, byte[]> blocks) throws RemoteException{
        for (Map.Entry<String, byte[]> entry : blocks.entrySet()){
            String blockName = entry.getKey();
            byte[] blockData = entry.getValue();
            File newFile = new File(absoluteBlockDirectory+"\\"+blockName);
            try{
                BufferedOutputStream bufOut = new BufferedOutputStream(new FileOutputStream(newFile));
                bufOut.write(blockData);
                bufOut.flush();
                bufOut.close();
            }
            catch (Exception e){
                throw new RemoteException("Failed to write remote data!");
            }
        }
        // 发送更新后的数据块目录
        sendBlockRecord();
        // 发送更新后的硬盘空间
        sendNodeStates();
    }

    @Override
    public void sendBlocksTo(String datanodeIp, String datanodePort, ArrayList<String> blockNames) throws RemoteException, NotBoundException{
        try{
            ArrayList<String> toDelBlocks = new ArrayList<>();
            HashMap<String, byte[]> blocks = new HashMap<>();
            DataNodesRPCInterface dataNodesRPC = (DataNodesRPCInterface)Naming.lookup("rmi://"+datanodeIp+":"+datanodePort+"/DFSDataNodeToDataNode");
            for (String block : blockNames){
                byte[] blockDatas = loadBlockData(block);
                blocks.put(block, blockDatas);
                toDelBlocks.add(absoluteBlockDirectory+"\\"+block);
            }
            dataNodesRPC.sendBlocks(blocks);

            for(String toDelBlock : toDelBlocks){
                File tmp = new File(toDelBlock);
                tmp.delete();
            }
            sendBlockRecord();
            sendNodeStates();
        }
        catch (NotBoundException e){
            System.out.println("[SEND BLOCKS ERROR!] Send blocks to other datanode error!");
            throw  e;
        }
        catch (MalformedURLException e){
            System.out.println("[ERROR!] sendBlocksTo Funtion Error!");
        }
    }
    private byte[] loadBlockData(String blockName){
        File theFile = new File(absoluteBlockDirectory+"\\"+blockName);
        if(!theFile.exists())
            return null;
        try{
            BufferedInputStream bufIn = new BufferedInputStream(new FileInputStream(theFile));
            byte[] datas = new byte[(int)theFile.length()];
            bufIn.read(datas);
            //theFile.delete();
            bufIn.close();
            return datas;
        }
        catch (Exception e){
            System.out.println("[ERROR!] loadBlockData Funtion Error!");
            return null;
        }
    }
}
