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
    String blocksDirectory,absoluteBlockDirectory;  // ���ݿ�洢Ŀ¼
    ArrayList<String> blocks;  // �����ݽڵ��ϵ����ݿ��б�
    String datanodeID;  // �豸��ʶ��DN-IP-Port
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
            // ���� rmiregistry
            registry = LocateRegistry.createRegistry(2021);
            // �� RMI ����
            Naming.rebind("rmi://localhost:2021/DFSDataNode", this);
            //System.out.println("The DataNode RMI is running...");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void initialize(){
        // ��ȡ�����ļ�datanode_properties.xml
        readConfigFile();

        // ��ʼ������ȡ���ݿ�Ŀ¼
        blocks = new ArrayList<String>();
        loadLocalBlocks(new File(absoluteBlockDirectory));

        //���� DataNode ID
        datanodeID = "DN-" + datanodeIp + "-" + datanodePort;
    }
    /**
     * ��ȡ Properties �����ļ�:datanode.xml
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
     * ��ȡ�������ݿ��б�
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
     * �г���ǰ���ݽڵ��е��������ݿ��б�
     */
    public void listBlocks(){
        System.out.println("This DataNode has following blocks:");
        for(String block:blocks){
            System.out.println("[Blocks]"+block);
        }
    }
    /**
     * �����ط�������������
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
     * �����ط��������ͱ������ݿ�Ŀ¼
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
     * �����ط���������������
     */
    public void sendNodeStates(){
        System.out.println("[INFO] Sending node states to the namenode ...");
        jump.workingDirectory = System.getProperty("user.dir");;     // ���õ�ǰĿ¼Ϊ���ݷ���������Ŀ¼
        jump.mystate.ip = datanodeIp;
        jump.mystate.port = datanodePort;
        jump.mystate.datanodeID = datanodeID;
        // Thread���start()����ʵ���߳�,run()ֻ����ͨ�������
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
        // ��NameNode����ע������
        unRegister();
        // �ر�RMI����
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
            // ��NameNode���͸��º�����ݿ�Ŀ¼
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
            // ��NameNode���͸��º�����ݿ�Ŀ¼
            sendBlockRecord();
        }
    }
}
