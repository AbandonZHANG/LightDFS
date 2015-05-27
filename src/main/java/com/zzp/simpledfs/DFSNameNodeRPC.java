package main.java.com.zzp.simpledfs;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSNameNodeRPC extends UnicastRemoteObject implements DataNodeNameNodeRPCInterface,ClientNameNodeRPCInterface,Runnable{

    DFSINode inode;    // DFS�ļ�Ŀ¼, ��Ҫ���û��洢
    HashMap<String, DFSFileBlockMapping> fileBlockMapping; // �ļ�-���ݿ�ӳ��, ��Ҫ���û��洢

    // ���ݿ�-���ݽڵ�ӳ��, �����ݽڵ㷢��
    final HashMap<String, String> blockDataNodeMappings = new HashMap<String, String>();
    // ��ǰ��Ծ���ݽڵ�״̬
    HashMap<String, DFSDataNodeState> datanodeStates = new HashMap<String, DFSDataNodeState>();
    // ������include, exclude
    final ArrayList<String> includeNodes = new ArrayList<String>();
    final ArrayList<String> excludeNodes = new ArrayList<String>();
    // һ���Թ�ϣ
    final DFSConsistentHashing<DFSDataNodeState> consistentHash = new DFSConsistentHashing<DFSDataNodeState>();

    Registry registry;  // RMI Registry

    DFSNameNodeRPC() throws RemoteException {
        // Necessary to call UnicastRemoteObject();
        super();
    };
    public void initialize(){
        // ��ȡ���û��洢��Inode��FileBlock Mapping
        loadState();

        // ��ȡ���ݷ�����������
        try {
            readIncludeFile("include", true);
            readIncludeFile("exclude", false);
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed to read include/exclude file! Check if the file is exists!");
        }
    }
    /**
     * �������û�����
     */
    public void loadState(){
        File inodeFile = new File("inode");
        File filemapFile = new File("filemap");
        if(!inodeFile.exists()){
            inode = new DFSINode();
            // ��ʼ��DFS INode��Ŀ¼
            inode.name = "root";
            inode.childInode = new HashMap<String, DFSINode>();
        }
        else{
            try {
                ObjectInputStream inodeIs = new ObjectInputStream(new FileInputStream(inodeFile));
                inode = (DFSINode) inodeIs.readObject();
            }
            catch (Exception e) {
                System.out.println("[ERROR!] Load inode data failed!");
            }
        }

        if(!filemapFile.exists()){
            fileBlockMapping = new HashMap<String, DFSFileBlockMapping>();
        }
        else{
            try {
                ObjectInputStream filemapIs = new ObjectInputStream(new FileInputStream(filemapFile));
                fileBlockMapping = (HashMap<String, DFSFileBlockMapping>) filemapIs.readObject();
            }
            catch (Exception e){
                System.out.println("[ERROR!] Load filemap data failed!");
            }
        }
    }

    /**
     * Inode �� File-Block Mapping �������û�
     */
    public void storeState(){
        File inodeFile = new File("inode");
        File filemapFile = new File("filemap");
        if(inodeFile.exists()){
            inodeFile.delete();
        }
        if(filemapFile.exists()){
            filemapFile.delete();
        }
        try{
            ObjectOutputStream inodeOos = new ObjectOutputStream(new FileOutputStream(inodeFile));
            inodeOos.writeObject(inode);
            ObjectOutputStream filemapOos = new ObjectOutputStream(new FileOutputStream(filemapFile));
            filemapOos.writeObject(fileBlockMapping);
        }
        catch (Exception e){
            System.out.println("[ERROR!] Stored data failed!");
        }
    }
    public void run(){
        try{
            // ���� rmiregistry
            registry = LocateRegistry.createRegistry(2020);
            // �� RMI ����
            Naming.rebind("rmi://localhost:2020/DFSNameNode", this);
            System.out.println("[INFO] The NameNode RMI is running...");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void close(){
        try{
            // �������û�
            storeState();
            // �ر� rmiregistry
            if(registry!=null)
                UnicastRemoteObject.unexportObject(registry,true);
            System.out.println("[INFO] The NameNode RMI is done...");
            System.exit(0);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * @param filename
     * @param include �����ȡinclude�ļ�����Ϊtrue����ȡexclude�ļ�Ϊfalse
     * @throws FileNotFoundException
     */
    public void readIncludeFile(String filename, boolean include) throws FileNotFoundException {
        File File = new File(filename);
        if(!File.exists()){
            throw new FileNotFoundException("Node Include/Exclude File(include/exclude) not found!");
        }
        else{
            FileReader fReader = new FileReader(File);
            BufferedReader bfReader = new BufferedReader(fReader);
            try{
                String tmpS = null;
                while((tmpS = bfReader.readLine())!=null){
                    if (include)
                        includeNodes.add(tmpS);
                    else
                        excludeNodes.add(tmpS);
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    /**
     * �г���ǰDFS�����ӵ����ݽڵ��е��������ݿ��б�
     */
    public void listBlocks(){
        Iterator iter = blockDataNodeMappings.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next();
            System.out.println(entry.getKey() + "    Affiliated with DataNode: " + entry.getValue());
        }
    }

    /**
     * �г��ļ��������ݿ�ӳ���б�
     */
    public void listFileBlockMappings(){
        Iterator iter = fileBlockMapping.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next();
            System.out.println("---"+entry.getKey());
            DFSFileBlockMapping thefileBlockMapping = (DFSFileBlockMapping)entry.getValue();
            for (String block:thefileBlockMapping.blocks){
                System.out.println("          ---"+block);
            }
        }
    }

    /**
     * DataNodes RMI - sendDataNodeStates
     * @param datanode
     * @throws RemoteException
     */
    public void sendDataNodeStates(DFSDataNodeState datanode) throws RemoteException{
        // ״̬���
        if(includeNodes.contains(datanode.datanodeID) && !excludeNodes.contains(datanode.datanodeID)){
            datanodeStates.put(datanode.datanodeID, datanode);
        }
    }

    /**
     * DataNodes RMI - registerDataNode
     * @param datanode
     * @return
     * @throws RemoteException
     */
    public boolean registerDataNode(DFSDataNodeState datanode) throws RemoteException{
        if(includeNodes.contains(datanode.datanodeID) && excludeNodes.contains(datanode.datanodeID)){
            // ��Ƿ�����������
            excludeNodes.remove(datanode.datanodeID);
            // ע��һ���Թ�ϣ
            consistentHash.addNode(datanode.datanodeID, datanode);
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * DataNodes RMI - sendDataNodeBlockList
     * @param datanodeID
     * @param blocks
     * @throws RemoteException
     */
    public void sendDataNodeBlockList(String datanodeID, ArrayList<String> blocks)throws RemoteException{
        if(includeNodes.contains(datanodeID) && !excludeNodes.contains(datanodeID)){
            for(String block:blocks){
                // �������ݿ�-���ݽڵ�ӳ����
                blockDataNodeMappings.put(block, datanodeID);
            }
        }
    }

    /**
     * DataNodes RMI - unRegisterDataNode
     * @param datanodeID
     * @return
     * @throws RemoteException
     */
    public boolean unRegisterDataNode(String datanodeID) throws RemoteException{
        if(includeNodes.contains(datanodeID) && !excludeNodes.contains(datanodeID)){
            // ɾ�����ݽڵ��µ�ӳ��
            Iterator iter = blockDataNodeMappings.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry entry = (Map.Entry)iter.next();
                if(entry.getValue().equals(datanodeID)){
                    blockDataNodeMappings.remove(entry.getKey());
                }
            }
            // ɾ����ǰ��Ծ���ݽڵ�״̬
            datanodeStates.remove(datanodeID);
            // ɾ��һ���Թ�ϣ�еĽڵ�
            consistentHash.deleteNode(datanodeID);
            // ���±��Ϊδ����
            excludeNodes.add(datanodeID);
            return true;
        }
        return false;
    }

    /**
     * Clients RMI - lsDFSDirectory
     * @param path
     * @return
     * @throws RemoteException
     * @throws FileNotFoundException
     */
    public ArrayList< Map.Entry<String, Boolean> > lsDFSDirectory(String path) throws RemoteException, FileNotFoundException{
        DFSINode theInode;
        ArrayList< Map.Entry<String, Boolean> > res = new ArrayList<Map.Entry<String, Boolean>>();
        try {
            theInode = updateDFSINode(path, (short) 0);
            if(theInode == null){
                throw new FileNotFoundException();
            }
            else{
                Iterator iter = theInode.childInode.entrySet().iterator();
                while(iter.hasNext()){
                    Map.Entry entry = (Map.Entry)iter.next();
                    DFSINode tmpchild = (DFSINode)entry.getValue();
                    //res.add(new AbstractMap.SimpleEntry<String, Boolean>((String)entry.getKey(), new Boolean(tmpchild.childInode==null)));
                }
            }
        }
        catch (FileAlreadyExistsException e){

        }
        return null;
    }
    /**
     * Clients RMI - newDFSFileMapping
     * @param filePath
     * @param blocks_num
     * @param ifLittleFile
     * @return
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    public ArrayList< Map.Entry<String, String> > newDFSFileMapping(String filePath, int blocks_num, boolean ifLittleFile) throws RemoteException, FileNotFoundException, FileAlreadyExistsException {
        ArrayList< Map.Entry<String, String> > blockDatanodes = new ArrayList<Map.Entry<String, String>>();

        // �½�File-Block Mapping�ڵ�
        DFSFileBlockMapping newFileBlockMapping = new DFSFileBlockMapping();
        newFileBlockMapping.filePath = filePath;
        newFileBlockMapping.ifLittleFile = ifLittleFile;
        newFileBlockMapping.blocks = new ArrayList<String>();

        // ������ļ�
        if(!ifLittleFile) {
            // ����block��ʶ
            for (int i = 0; i < blocks_num; ++i) {
                UUID newUUID = UUID.randomUUID();
                // ���뵽file��block�б���
                newFileBlockMapping.blocks.add(newUUID.toString());
            }

            // Ϊ���ݿ�������ݽڵ�-���ؾ���
            // ��ȡ��һ�����ݽڵ�
            // �������������û�����ݽڵ㣬������Ҫ�ع�
            Iterator iter = datanodeStates.entrySet().iterator();
            // �ҵ��ɷ�������ݽڵ�
            if(iter.hasNext()){

                // ��File��ӽ�Inode�ļ��ڵ�
                updateDFSINode(filePath, (short)11);

                // ��ӱ��ε�File-Block Mapping
                fileBlockMapping.put(filePath, newFileBlockMapping);

                Map.Entry<String, DFSDataNodeState> entry = (Map.Entry<String, DFSDataNodeState>)iter.next();
                DFSDataNodeState dataState = entry.getValue();
                for (String block:newFileBlockMapping.blocks){
                    blockDatanodes.add(new AbstractMap.SimpleEntry<String, String>(block, dataState.ip));
                    // ���Block-DataNodeӳ��
                    blockDataNodeMappings.put(block, dataState.datanodeID);
                }
                return blockDatanodes;
            }
            else{
                return null;
            }
        }
        // ����С�ļ�
        else{
            return blockDatanodes;
        }
    }

    /**
     * Clients RMI - lookupFileBlocks
     * @param filePath
     * @return
     * @throws RemoteException
     * @throws FileNotFoundException
     */
    public ArrayList< Map.Entry<String, String> > lookupFileBlocks(String filePath) throws RemoteException, FileNotFoundException{
        ArrayList< Map.Entry<String, String> > blockDatanodes = new ArrayList<Map.Entry<String, String>>();
        if(fileBlockMapping.containsKey(filePath)){
            // ��ȡ�ļ���Ӧ�����ݿ�ӳ�����
            DFSFileBlockMapping fileBlocks = fileBlockMapping.get(filePath);
            for (String block:fileBlocks.blocks){
                // �����ݿ�-���ݽڵ�ӳ���в�ѯ���ݿ��������ݽڵ��ʶ
                String datanode = blockDataNodeMappings.get(block);
                // ��ȡ��ǰ�����ݽڵ��ip
                String datanodeip = datanodeStates.get(datanode).ip;
                blockDatanodes.add(new AbstractMap.SimpleEntry<String, String>(block, datanodeip));
            }
        }
        else{
            throw new FileNotFoundException("File not found!");
        }
        return blockDatanodes;
    }

    /**
     * Clients RMI - removeDFSFile
     * @param filePath
     * @return
     * @throws RemoteException
     * @throws FileNotFoundException
     */
    public ArrayList< Map.Entry<String, String> > removeDFSFile(String filePath) throws RemoteException, FileNotFoundException{
        ArrayList< Map.Entry<String, String> > blockDatanodes = new ArrayList<Map.Entry<String, String>>();
        try{
            // del the origin file
            updateDFSINode(filePath, (short) 12);
        }
        catch (Exception e){

        }
        if(fileBlockMapping.containsKey(filePath)){
            // get the file mapping
            DFSFileBlockMapping fileBlocks = fileBlockMapping.get(filePath);
            // delete the file-block mapping
            fileBlockMapping.remove(filePath);
            for (String block:fileBlocks.blocks){
                // �����ݿ�-���ݽڵ�ӳ���в�ѯ���ݿ��������ݽڵ��ʶ
                String datanode = blockDataNodeMappings.get(block);
                // ��ȡ��ǰ�����ݽڵ��ip
                String datanodeip = datanodeStates.get(datanode).ip;
                blockDatanodes.add(new AbstractMap.SimpleEntry<String, String>(block, datanodeip));
            }
        }
        else{
            throw new FileNotFoundException("Illegal Path!");
        }

        return blockDatanodes;
    }

    /**
     * Clients RMI - renameDFSFile
     * @param filePath
     * @param newfilePath
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    public void renameDFSFile(String filePath, String newfilePath) throws RemoteException, FileNotFoundException, FileAlreadyExistsException{
        // del the origin file
        updateDFSINode(filePath, (short) 12);
        // get the file mapping
        DFSFileBlockMapping tmpFileBlockMapping = fileBlockMapping.get(filePath);
        // add the new file
        try{
            updateDFSINode(newfilePath, (short) 11);
            // remove and put the new file mapping
            fileBlockMapping.remove(filePath);
            tmpFileBlockMapping.filePath = newfilePath;
            fileBlockMapping.put(newfilePath, tmpFileBlockMapping);
        }
        catch (FileAlreadyExistsException e){
            // add new file error, roll back
            updateDFSINode(filePath, (short) 11);
            throw e;
        }
        catch (Exception e){

        }
    }

    /**
     * Clients RMI - addDFSDirectory
     * @param path
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    public void addDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException{
        updateDFSINode(path, (short) 1);
    }

    /**
     * Clients RMI - delDFSDirectory
     * @param path
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    public void delDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException{
        updateDFSINode(path, (short) 2);
    }

    /**
     * Clients RMI - ifExistsDFSDirectory
     * @param path
     * @return
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    public boolean ifExistsDFSDirectory(String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException{
        return updateDFSINode(path, (short) 0)!=null;
    }

    /**
     * Inode ���Ĳ�������
     * @param path
     * @param method
     *        11: add new file
     *        12: delete a file
     *         1: add new dir
     *         2: delete a dir
     *         0: return the exists dir INode. If not exists, return null.
     * @return
     */
    public DFSINode updateDFSINode(String path, short method) throws RemoteException, FileNotFoundException, FileAlreadyExistsException {
        DFSINode tmp = inode;
        // ��·����"\"���ָ�������Ŀ¼��
        String[] splits = path.split("\\\\");   // �ָ���"\"
        int length = splits.length-1;

        // ����β'\'
        if (splits[splits.length - 1].equals("")){
            length --;
        }

        // ����ʡ�Ը�Ŀ¼ʽ�ľ���·����ʽ
        if(splits[0].equals("") || splits[0].equals("root"))
            splits[0] = "root";
        else
            throw new FileNotFoundException("Illegal Path!");

        for (int i = 1; i <= length; i ++){
            String eachPath = splits[i];
            // ��ǰ·�����ļ�
            if(tmp.childInode == null){
                throw new FileAlreadyExistsException("Illegal Path!");
            }
            // ��ǰĿ¼������ΪeachPath�����ļ�/Ŀ¼
            // ��������ǰ��û���ж϶�Ӧ�Ķ���INode�ڵ���Ŀ¼�����ļ�
            else if(tmp.childInode.containsKey(eachPath)){
                if(i == length){
                    if(method == 11){
                        throw new FileAlreadyExistsException("Inode has already exists!");
                    }
                    else if(method == 1){
                        throw new FileAlreadyExistsException("Inode has already exists!");
                    }
                    else if(method == 12){
                        DFSINode tmpChild = tmp.childInode.get(eachPath);
                        if(tmpChild.childInode == null)
                            tmp.childInode.remove(eachPath);
                        else
                            throw new FileNotFoundException("It is a Directory!");
                    }
                    else if(method == 2){
                        DFSINode tmpChild = tmp.childInode.get(eachPath);
                        if(tmpChild.childInode != null)
                            tmp.childInode.remove(eachPath);
                        else
                            throw new FileNotFoundException("It is a File!");
                    }
                    else if(method == 0){
                        return tmp.childInode.get(eachPath);
                    }
                }
                else{
                    // �������±���
                    tmp = tmp.childInode.get(eachPath);
                }
            }
            else{
                if(i == length){
                    if(method == 11){
                        DFSINode newInode = new DFSINode();
                        newInode.name = eachPath;
                        tmp.childInode.put(eachPath, newInode);
                    }
                    else if(method == 1){
                        DFSINode newInode = new DFSINode();
                        newInode.name = eachPath;
                        // Ŀ¼��Ҫ��childInode����
                        newInode.childInode = new HashMap<String, DFSINode>();
                        tmp.childInode.put(eachPath, newInode);
                    }
                    else if(method == 2 || method == 12){
                        throw new FileNotFoundException("Directory not found!");
                    }
                    else if(method == 0){
                        return null;
                    }
                }
                else{
                    if(method == 1){
                        // �½�Ŀ¼
                        DFSINode newInode = new DFSINode();
                        newInode.name = eachPath;
                        // Ŀ¼��Ҫ��childInode����
                        newInode.childInode = new HashMap<String, DFSINode>();
                        tmp.childInode.put(eachPath, newInode);
                        // ���������½�Ŀ¼���±���
                        tmp = newInode;
                    }
                    else{
                        throw new FileNotFoundException("Illegal Path!");
                    }
                }
            }
        }
        return null;
    }
}
