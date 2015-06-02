package com.zzp.simpledfs.namenode;

import com.zzp.simpledfs.common.ClientNameNodeRPCInterface;
import com.zzp.simpledfs.common.DFSDataNodeState;
import com.zzp.simpledfs.common.DFSINode;
import com.zzp.simpledfs.common.DataNodeNameNodeRPCInterface;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class DFSNameNodeRPC extends UnicastRemoteObject implements DataNodeNameNodeRPCInterface,ClientNameNodeRPCInterface,Runnable{
    HashMap<String, DFSUser> dfsUsers;  // 用户列表，需要永久化存储

    DFSINode inode;    // DFS文件目录, 需要永久化存储
    HashMap<String, DFSFileBlockMapping> fileBlockMapping; // 文件-数据块映射, 需要永久化存储

    final HashMap<String, String> blockDataNodeMappings = new HashMap<String, String>();    // 数据块-数据节点映射, 由数据节点发送
    final HashMap<String, DFSDataNodeState> datanodeStates = new HashMap<String, DFSDataNodeState>();     // 当前活跃数据节点状态

    // 白名单include, exclude
    final ArrayList<String> includeNodes = new ArrayList<String>();
    final ArrayList<String> excludeNodes = new ArrayList<String>();
    // 一致性哈希
    final DFSConsistentHashing consistentHash = new DFSConsistentHashing();

    Registry registry;  // RMI Registry
    int rmiPort;        // RMI 服务端口号

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

        readConfigFile();
    }
    /**
     * 载入永久化数据
     */
    public void loadState(){
        File inodeFile = new File("inode");
        File filemapFile = new File("filemap");
        File usersFile = new File("users");
        if(!inodeFile.exists()){
            // 初始化DFS INode根目录
            inode = DFSINode.getInstance("root", true);
        }
        else{
            try {
                ObjectInputStream inodeIs = new ObjectInputStream(new FileInputStream(inodeFile));
                inode = (DFSINode) inodeIs.readObject();
            }
            catch (Exception e) {
                System.out.println("[ERROR!] Load inode file failed!");
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
                System.out.println("[ERROR!] Load filemap file failed!");
            }
        }

        if(!usersFile.exists()){
            dfsUsers = new HashMap<String, DFSUser>();
        }
        else{
            try {
                ObjectInputStream usersIs = new ObjectInputStream(new FileInputStream(usersFile));
                dfsUsers = (HashMap<String, DFSUser>) usersIs.readObject();
            }
            catch (Exception e){
                System.out.println("[ERROR!] Load users file failed!");
            }
        }
    }

    /**
     * Inode 和 File-Block Mapping 数据永久化
     */
    public void storeState(){
        File inodeFile = new File("inode");
        File filemapFile = new File("filemap");
        File usersFile = new File("users");
        if(inodeFile.exists()){
            inodeFile.delete();
        }
        if(filemapFile.exists()){
            filemapFile.delete();
        }
        if(usersFile.exists()){
            usersFile.delete();
        }
        try{
            ObjectOutputStream usersOos = new ObjectOutputStream(new FileOutputStream(usersFile));
            usersOos.writeObject(dfsUsers);

            ObjectOutputStream inodeOos = new ObjectOutputStream(new FileOutputStream(inodeFile));
            inodeOos.writeObject(inode);

            ObjectOutputStream filemapOos = new ObjectOutputStream(new FileOutputStream(filemapFile));
            filemapOos.writeObject(fileBlockMapping);
        }
        catch (Exception e){
            System.out.println("[ERROR!] Stored data file failed!");
        }
    }
    /**
     * 读取 Properties 配置文件:namenode.xml
     */
    public void readConfigFile(){
        Properties props = new Properties();
        try{
            InputStream fin = new FileInputStream("namenode.xml");
            props.loadFromXML(fin);
            fin.close();
            rmiPort = Integer.valueOf(props.getProperty("port"));
            DFSUser.initUserSpace = Integer.valueOf(props.getProperty("inituserspace"));
            DFSUser.initUserSpace *= 1024*1024; // 转化为Byte
        }
        catch (Exception e){
            System.out.println("[ERROR!] Read config file error!");
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
        for (Map.Entry entry : blockDataNodeMappings.entrySet()){
            System.out.println(entry.getKey() + "    Affiliated with DataNode: " + entry.getValue());
        }
    }

    /**
     * 列出文件及其数据块映射列表
     */
    public void listFileBlockMappings(){
        // foreach
        for (Map.Entry entry : fileBlockMapping.entrySet()){
            System.out.println("---"+entry.getKey());
            DFSFileBlockMapping thefileBlockMapping = (DFSFileBlockMapping)entry.getValue();
            for (String block:thefileBlockMapping.blocks){
                System.out.println("          ---"+block);
            }
        }
    }

    /**
     * 判断DataNode服务器连接状态
     * @param datanodeID
     * @return 0:node不在白名单中; 1:node未连接; 2:node已连接
     */
    public int checkNodeConnection(String datanodeID){
        if(includeNodes.contains(datanodeID)){
            if(excludeNodes.contains(datanodeID)){
                return 1;
            }
            else{
                return 2;
            }
        }
        else{
            return 0;
        }
    }
    /**
     * DataNodes RMI - sendDataNodeJump
     * @param datanodeID
     * @throws RemoteException
     */
    @Override
    public void sendDataNodeJump(String datanodeID) throws RemoteException{
        // 心跳监控
        if(checkNodeConnection(datanodeID) == 2){

        }
    }

    /**
     * DataNodes RMI - sendDataNodeStates
     * @param datanode
     * @throws RemoteException
     */
    @Override
    public void sendDataNodeStates(DFSDataNodeState datanode) throws RemoteException{
        // 节点状态监控
        if(checkNodeConnection(datanode.getDatanodeID()) == 2){
            // 更新一致性哈希
            consistentHash.updateNode(datanode.getDatanodeID(), datanode.getFreeSpace());
        }
    }

    /**
     * DataNodes RMI - sendDataNodeBlockList
     * @param datanodeID
     * @param blocks
     * @throws RemoteException
     */
    @Override
    public void sendDataNodeBlockList(String datanodeID, ArrayList<String> blocks)throws RemoteException{
        if(checkNodeConnection(datanodeID) == 2){
            for(String block:blocks){
                // 加入数据块-数据节点映射中
                blockDataNodeMappings.put(block, datanodeID);
            }
        }
    }

    /**
     * DataNodes RMI - registerDataNode
     * @param datanode
     * @return
     * @throws RemoteException
     */
    @Override
    public boolean registerDataNode(DFSDataNodeState datanode) throws RemoteException{
        if(checkNodeConnection(datanode.getDatanodeID()) == 1){
            // 标记服务器已连接
            excludeNodes.remove(datanode.getDatanodeID());
            datanodeStates.put(datanode.getDatanodeID(), datanode);
            // 注册一致性哈希
            consistentHash.addNode(datanode.getDatanodeID(), datanode.getFreeSpace());
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * DataNodes RMI - unRegisterDataNode
     * @param datanodeID
     * @return
     * @throws RemoteException
     */
    @Override
    public boolean unRegisterDataNode(String datanodeID) throws RemoteException{
        if(checkNodeConnection(datanodeID) == 2){
            // 删除数据节点下的映射
            Iterator iter = blockDataNodeMappings.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry entry = (Map.Entry)iter.next();
                if(entry.getValue().equals(datanodeID)){
                    // 循环中删除调用Iterator的remove才是安全的
                    iter.remove();
                }
            }
            // 删除当前活跃数据节点状态
            datanodeStates.remove(datanodeID);
            // 删除一致性哈希中的节点
            consistentHash.removeNode(datanodeID);
            // 重新标记为未连接
            excludeNodes.add(datanodeID);
            return true;
        }
        return false;
    }

    /**
     * 客户端用户注册RPC
     * @param userName
     * @param password
     * @return false:user name has exists, true:accept
     * @throws RemoteException
     */
    @Override
    public boolean registerUser(String userName, String password) throws RemoteException{
        if(dfsUsers.containsKey(userName)){
            return false;
        }
        // 新建DFSUser类，加入到用户列表中
        dfsUsers.put(userName, new DFSUser(userName, password));
        // 分配INode的User目录
        DFSINode userINode = DFSINode.getInstance(userName, true);
        inode.childInode.put(userName, userINode);
        return true;
    }

    /**
     * 客户端用户注销RPC
     * @param userName
     * @return
     * @throws RemoteException
     */
    @Override
    public boolean unRegisterUser(String userName, String password) throws RemoteException{
        if(dfsUsers.containsKey(userName)){
            if(password.equals(dfsUsers.get(userName).base64Password)){
                // 从用户列表中删除
                dfsUsers.remove(userName);
                // 从INode节点中删除用户目录

                // 删除用户文件及数据块
                return true;
            }
            else{
                return false;
            }
        }
        else{
            return false;
        }
    }

    /**
     * @param userName
     * @param password
     * @return
     * @throws RemoteException
     */
    public boolean login(String userName, String password) throws RemoteException{
        if(dfsUsers.containsKey(userName)){
            DFSUser theUser = dfsUsers.get(userName);
            if(password.equals(theUser.base64Password)){
                // 标记该userID登陆
                // theUser.userID.add(userID);
                return true;
            }
            else{
                return false;
            }
        }
        else{
            return false;
        }
    }

    /**
     * @param userName
     * @return
     * @throws RemoteException
     */
    public long getUserTotalSpace(String userName) throws RemoteException{
        if(dfsUsers.containsKey(userName)){
            return dfsUsers.get(userName).maxSpace;
        }
        else{
            return -1;
        }
    }

    public long getUserUsedSpace(String userName) throws RemoteException{
        if(dfsUsers.containsKey(userName)){
            return dfsUsers.get(userName).usedSpace;
        }
        else{
            return -1;
        }
    }

    public boolean setUserTotalSpace(String userName, long totalSpace) throws RemoteException{
        if(dfsUsers.containsKey(userName)){
            dfsUsers.get(userName).maxSpace = totalSpace;
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * Clients RMI - getDFSINode
     * @param userName
     * @param path
     * @return
     *          null: 找不到该节点(ifLogin == true)，或者未登录(ifLogin == false)
     *          DFSINode: 找到的DFSINode
     * @throws RemoteException
     * @throws FileNotFoundException
     */
    @Override
    public DFSINode getDFSINode(String userName, String path) throws RemoteException, FileNotFoundException{
        DFSINode theInode = null;
        String inodePath = getINodePath(userName, path);
        try {
            theInode = updateDFSINode(inodePath, 0);
        } catch (FileAlreadyExistsException e) {
            System.out.println("updateDFSInode function error!");
        }
        return theInode;
    }

    /**
     * Clients RMI - newDFSFileMapping
     * @param filePath
     * @param blockNum
     * @return
     *
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    @Override
    public ArrayList< Map.Entry<String, String> > newDFSFileMapping(String userName, String filePath, long fileSize, int blockNum) throws RemoteException, FileNotFoundException, FileAlreadyExistsException {
        ArrayList< Map.Entry<String, String> > blockDatanodes = new ArrayList<Map.Entry<String, String>>();
        String inodePath = getINodePath(userName, filePath);
        // 新建File-Block Mapping节点
        DFSFileBlockMapping newFileBlockMapping = new DFSFileBlockMapping();
        newFileBlockMapping.filePath = inodePath;
        newFileBlockMapping.ifLittleFile = false;
        newFileBlockMapping.blocks = new ArrayList<String>();

        // 生成block标识
        for (int i = 0; i < blockNum; ++i) {
            UUID newUUID = UUID.randomUUID();
            // 加入到file的block列表中
            newFileBlockMapping.blocks.add(newUUID.toString());

            // 为数据块分配数据节点-负载均衡
            String datanodeID = consistentHash.getNode(newUUID.toString());
            if(datanodeID == null){
                return null;
            }
            DFSDataNodeState dataNodeState = datanodeStates.get(datanodeID);

            blockDatanodes.add(new AbstractMap.SimpleEntry<String, String>(newUUID.toString(), dataNodeState.getIp()));
            // 添加Block-DataNode映射
            blockDataNodeMappings.put(newUUID.toString(), datanodeID);
        }

        // 将File添加进Inode文件节点
        updateDFSINode(inodePath, 11);

        // 添加本次的File-Block Mapping
        fileBlockMapping.put(inodePath, newFileBlockMapping);

        return blockDatanodes;
    }

    /**
     * Clients RMI - lookupFileBlocks
     * @param filePath
     * @return
     * @throws RemoteException
     * @throws FileNotFoundException
     */
    @Override
    public ArrayList< Map.Entry<String, String> > lookupFileBlocks(String userName, String filePath) throws RemoteException, FileNotFoundException{
        ArrayList< Map.Entry<String, String> > blockDatanodes = new ArrayList<Map.Entry<String, String>>();
        String inodePath = getINodePath(userName, filePath);
        if(fileBlockMapping.containsKey(inodePath)){
            // 获取文件对应的数据块映射对象
            DFSFileBlockMapping fileBlocks = fileBlockMapping.get(inodePath);
            for (String block:fileBlocks.blocks){
                // 在数据块-数据节点映射中查询数据块所属数据节点标识
                String datanode = blockDataNodeMappings.get(block);
                // 获取当前该数据节点的ip
                String datanodeip = datanodeStates.get(datanode).getIp();
                blockDatanodes.add(new AbstractMap.SimpleEntry<String, String>(block, datanodeip));
            }
        }
        else{
            throw new FileNotFoundException();
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
    @Override
    public ArrayList< Map.Entry<String, String> > removeDFSFile(String userName, String filePath) throws RemoteException, FileNotFoundException{
        ArrayList< Map.Entry<String, String> > blockDatanodes = new ArrayList<Map.Entry<String, String>>();
        String inodePath = getINodePath(userName, filePath);
        try{
            // del the origin file
            updateDFSINode(inodePath, 12);
        }
        catch (FileAlreadyExistsException e){
            System.out.println("updateDFSInode function error!");
        }
        if(fileBlockMapping.containsKey(inodePath)){
            // get the file mapping
            DFSFileBlockMapping fileBlocks = fileBlockMapping.get(inodePath);
            // delete the file-block mapping
            fileBlockMapping.remove(inodePath);
            for (String block:fileBlocks.blocks){
                // 在数据块-数据节点映射中查询数据块所属数据节点标识
                String datanode = blockDataNodeMappings.get(block);
                // 获取当前该数据节点的ip
                String datanodeip = datanodeStates.get(datanode).getIp();
                blockDatanodes.add(new AbstractMap.SimpleEntry<String, String>(block, datanodeip));
            }
        }
        else{
            throw new FileNotFoundException();
        }

        return blockDatanodes;
    }

    /**
     * Clients RMI - renameDFSFile
     * @param filePath
     * @param newFilePath
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    @Override
    public void renameDFSFile(String userName, String filePath, String newFilePath) throws RemoteException, FileNotFoundException, FileAlreadyExistsException{
        String inodePath = getINodePath(userName, filePath);
        String newINodePath = getINodePath(userName, newFilePath);
        // 检查新文件名是否已经存在
        if(ifExistsDFSINode(userName, newFilePath)){
            throw new FileAlreadyExistsException("");
        }
        // del the origin file
        try{
            updateDFSINode(inodePath, 12);
        }
        catch(FileAlreadyExistsException e){
            System.out.println("updateDFSInode function error!");
        }
        // get the file mapping
        DFSFileBlockMapping tmpFileBlockMapping = fileBlockMapping.get(inodePath);
        // add the new file
        try{
            updateDFSINode(newINodePath, 11);
        }
        catch(FileNotFoundException e){
            System.out.println("updateDFSInode function error!");
        }
        // remove and put the new file mapping
        fileBlockMapping.remove(inodePath);
        tmpFileBlockMapping.filePath = newINodePath;
        fileBlockMapping.put(newINodePath, tmpFileBlockMapping);
    }

    /**
     * Clients RMI - addDFSDirectory
     * @param path
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    @Override
    public void addDFSDirectory(String userName, String path) throws RemoteException, FileNotFoundException, FileAlreadyExistsException{
        String inodePath = getINodePath(userName, path);
        try{
            updateDFSINode(inodePath, 1);
        }
        catch(FileNotFoundException e){
            System.out.println("updateDFSInode function error!");
        }
    }

    /**
     * Clients RMI - delDFSDirectory
     * @param path
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    @Override
    public void delDFSDirectory(String userName, String path) throws RemoteException, FileNotFoundException{
        String inodePath = getINodePath(userName, path);
        try{
            updateDFSINode(inodePath, 2);
        }
        catch(FileAlreadyExistsException e){
            System.out.println("updateDFSInode function error!");
        }
    }

    /**
     * Clients RMI - ifExistsDFSDirectory
     * @param path
     * @return
     * @throws RemoteException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     */
    @Override
    public boolean ifExistsDFSINode(String userName, String path) throws RemoteException{
        boolean res = false;
        try{
            String inodePath = getINodePath(userName, path);
            res = (updateDFSINode(inodePath, 0) != null);
        }
        catch (FileNotFoundException e){
            return false;
        }
        catch (FileAlreadyExistsException e){
            System.out.println("updateDFSInode function error!");
        }
        return res;
    }

//    private boolean checkLogin(String userID, String userName){
//        return users.get(userName).userID.contains(userID);
//    }

    /**
     * @param userName
     * @param dfspath
     * @return
     * @throws FileNotFoundException
     */
    private String getINodePath(String userName, String dfspath) throws FileNotFoundException{
        String res = new String("");
        res += userName;
        res += '\\';
        if(dfspath.length() > 0) {
            if (dfspath.charAt(0) == '\\')
                dfspath = dfspath.substring(1, dfspath.length());
            res += dfspath;
        }
        // 消除末尾'\'
        if(res.charAt(res.length()-1) == '\\'){
            res = res.substring(0, res.length()-1);
        }
        return res;
    }
    /**
     * Inode 核心操作函数
     * @param inodePath DFSINode节点绝对路径
     * @param method
     *        11: add new file
     *        12: delete a file
     *         1: add new dir
     *         2: delete a dir
     *         0: return the exists dir INode. If not exists, return null.
     * @return method == 0 且存在该节点时返回对应DFSINode，其他情况返回null
     */
    public DFSINode updateDFSINode(String inodePath, int method) throws RemoteException, FileNotFoundException, FileAlreadyExistsException {
        DFSINode tmp = inode;
        // 将路径按"\"符分隔出各级目录来
        String[] splits = inodePath.split("\\\\");   // 分隔符"\"
        int length = splits.length-1;

        for (int i = 0; i <= length; i ++){
            String eachPath = splits[i];
            // 当前路径是文件
            if(tmp.childInode == null){
                throw new FileAlreadyExistsException("");
            }
            // 当前目录存在名为eachPath的子文件/目录
            // ！！！当前还没有判断对应的儿子INode节点是目录还是文件
            else if(tmp.childInode.containsKey(eachPath)){
                if(i == length){
                    if(method == 11){
                        throw new FileAlreadyExistsException("");
                    }
                    else if(method == 1){
                        throw new FileAlreadyExistsException("");
                    }
                    else if(method == 12){
                        DFSINode tmpChild = tmp.childInode.get(eachPath);
                        if(tmpChild.childInode == null)
                            tmp.childInode.remove(eachPath);
                        else
                            // It's a Directory!
                            throw new FileNotFoundException();
                    }
                    else if(method == 2){
                        DFSINode tmpChild = tmp.childInode.get(eachPath);
                        if(tmpChild.childInode != null)
                            tmp.childInode.remove(eachPath);
                        else
                            // It's a File!
                            throw new FileNotFoundException();
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
                        DFSINode newInode = DFSINode.getInstance(eachPath, false);
                        tmp.childInode.put(eachPath, newInode);
                    }
                    else if(method == 1){
                        DFSINode newInode = DFSINode.getInstance(eachPath, true);
                        tmp.childInode.put(eachPath, newInode);
                    }
                    else if(method == 2 || method == 12){
                        throw new FileNotFoundException();
                    }
                    else if(method == 0){
                        return null;
                    }
                }
                else{
                    if(method == 1){
                        // 新建目录
                        DFSINode newInode = DFSINode.getInstance(eachPath, true);
                        tmp.childInode.put(eachPath, newInode);
                        // 继续沿着新建目录往下遍历
                        tmp = newInode;
                    }
                    else{
                        throw new FileNotFoundException();
                    }
                }
            }
        }
        return null;
    }
}