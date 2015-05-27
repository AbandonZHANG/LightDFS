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

    DFSINode inode;    // DFS文件目录, 需要永久化存储
    HashMap<String, DFSFileBlockMapping> fileBlockMapping; // 文件-数据块映射, 需要永久化存储

    // 数据块-数据节点映射, 由数据节点发送
    final HashMap<String, String> blockDataNodeMappings = new HashMap<String, String>();
    // 当前活跃数据节点状态
    HashMap<String, DFSDataNodeState> datanodeStates = new HashMap<String, DFSDataNodeState>();
    // 白名单include, exclude
    final ArrayList<String> includeNodes = new ArrayList<String>();
    final ArrayList<String> excludeNodes = new ArrayList<String>();
    // 一致性哈希
    final DFSConsistentHashing<DFSDataNodeState> consistentHash = new DFSConsistentHashing<DFSDataNodeState>();

    Registry registry;  // RMI Registry

    DFSNameNodeRPC() throws RemoteException {
        // Necessary to call UnicastRemoteObject();
        super();
    };
    public void initialize(){
        // 读取永久化存储的Inode和FileBlock Mapping
        loadState();

        // 读取数据服务器白名单
        try {
            readIncludeFile("include", true);
            readIncludeFile("exclude", false);
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed to read include/exclude file! Check if the file is exists!");
        }
    }
    /**
     * 载入永久化数据
     */
    public void loadState(){
        File inodeFile = new File("inode");
        File filemapFile = new File("filemap");
        if(!inodeFile.exists()){
            inode = new DFSINode();
            // 初始化DFS INode根目录
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
     * Inode 和 File-Block Mapping 数据永久化
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
            // 启动 rmiregistry
            registry = LocateRegistry.createRegistry(2020);
            // 绑定 RMI 服务
            Naming.rebind("rmi://localhost:2020/DFSNameNode", this);
            System.out.println("[INFO] The NameNode RMI is running...");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void close(){
        try{
            // 数据永久化
            storeState();
            // 关闭 rmiregistry
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
     * @param include 如果读取include文件，则为true；读取exclude文件为false
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
     * 列出当前DFS中连接的数据节点中的所有数据块列表
     */
    public void listBlocks(){
        Iterator iter = blockDataNodeMappings.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry)iter.next();
            System.out.println(entry.getKey() + "    Affiliated with DataNode: " + entry.getValue());
        }
    }

    /**
     * 列出文件及其数据块映射列表
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
        // 状态监控
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
            // 标记服务器已连接
            excludeNodes.remove(datanode.datanodeID);
            // 注册一致性哈希
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
                // 加入数据块-数据节点映射中
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
            // 删除数据节点下的映射
            Iterator iter = blockDataNodeMappings.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry entry = (Map.Entry)iter.next();
                if(entry.getValue().equals(datanodeID)){
                    blockDataNodeMappings.remove(entry.getKey());
                }
            }
            // 删除当前活跃数据节点状态
            datanodeStates.remove(datanodeID);
            // 删除一致性哈希中的节点
            consistentHash.deleteNode(datanodeID);
            // 重新标记为未连接
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

        // 新建File-Block Mapping节点
        DFSFileBlockMapping newFileBlockMapping = new DFSFileBlockMapping();
        newFileBlockMapping.filePath = filePath;
        newFileBlockMapping.ifLittleFile = ifLittleFile;
        newFileBlockMapping.blocks = new ArrayList<String>();

        // 处理大文件
        if(!ifLittleFile) {
            // 生成block标识
            for (int i = 0; i < blocks_num; ++i) {
                UUID newUUID = UUID.randomUUID();
                // 加入到file的block列表中
                newFileBlockMapping.blocks.add(newUUID.toString());
            }

            // 为数据块分配数据节点-负载均衡
            // 先取第一个数据节点
            // ！！！这里如果没有数据节点，以上需要回滚
            Iterator iter = datanodeStates.entrySet().iterator();
            // 找到可分配的数据节点
            if(iter.hasNext()){

                // 将File添加进Inode文件节点
                updateDFSINode(filePath, (short)11);

                // 添加本次的File-Block Mapping
                fileBlockMapping.put(filePath, newFileBlockMapping);

                Map.Entry<String, DFSDataNodeState> entry = (Map.Entry<String, DFSDataNodeState>)iter.next();
                DFSDataNodeState dataState = entry.getValue();
                for (String block:newFileBlockMapping.blocks){
                    blockDatanodes.add(new AbstractMap.SimpleEntry<String, String>(block, dataState.ip));
                    // 添加Block-DataNode映射
                    blockDataNodeMappings.put(block, dataState.datanodeID);
                }
                return blockDatanodes;
            }
            else{
                return null;
            }
        }
        // 处理小文件
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
            // 获取文件对应的数据块映射对象
            DFSFileBlockMapping fileBlocks = fileBlockMapping.get(filePath);
            for (String block:fileBlocks.blocks){
                // 在数据块-数据节点映射中查询数据块所属数据节点标识
                String datanode = blockDataNodeMappings.get(block);
                // 获取当前该数据节点的ip
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
                // 在数据块-数据节点映射中查询数据块所属数据节点标识
                String datanode = blockDataNodeMappings.get(block);
                // 获取当前该数据节点的ip
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
     * Inode 核心操作函数
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
        // 将路径按"\"符分隔出各级目录来
        String[] splits = path.split("\\\\");   // 分隔符"\"
        int length = splits.length-1;

        // 消除尾'\'
        if (splits[splits.length - 1].equals("")){
            length --;
        }

        // 处理省略根目录式的绝对路径形式
        if(splits[0].equals("") || splits[0].equals("root"))
            splits[0] = "root";
        else
            throw new FileNotFoundException("Illegal Path!");

        for (int i = 1; i <= length; i ++){
            String eachPath = splits[i];
            // 当前路径是文件
            if(tmp.childInode == null){
                throw new FileAlreadyExistsException("Illegal Path!");
            }
            // 当前目录存在名为eachPath的子文件/目录
            // ！！！当前还没有判断对应的儿子INode节点是目录还是文件
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
                    // 继续往下遍历
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
                        // 目录需要有childInode引用
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
                        // 新建目录
                        DFSINode newInode = new DFSINode();
                        newInode.name = eachPath;
                        // 目录需要有childInode引用
                        newInode.childInode = new HashMap<String, DFSINode>();
                        tmp.childInode.put(eachPath, newInode);
                        // 继续沿着新建目录往下遍历
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
