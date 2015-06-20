package com.zzp.lightdfs.namenode;

import com.zzp.lightdfs.common.*;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.time.LocalDateTime;
import java.util.*;

public class DFSNameNode extends UnicastRemoteObject implements DataNodeNameNodeRPCInterface,ClientNameNodeRPCInterface,Runnable{
    public static DFSNameNode getInstance(String hostIp) throws RemoteException{
        // 设定RMI服务器ip地址
        System.setProperty("java.rmi.server.hostname",hostIp);
        return new DFSNameNode();
    }

    private DFSNameNode() throws RemoteException {
        // Necessary to call UnicastRemoteObject();
        super();
    };

    public HashMap<String, DFSUser> dfsUsers;  // 用户列表，需要永久化存储
    public DFSINode inode;    // DFS文件目录, 需要永久化存储
    HashMap<String, DFSFileBlockMapping> fileBlockMapping; // 文件-数据块映射, 需要永久化存储
    long intervalTime;  // 数据节点检查间隔时间

    public final HashMap<String, DFSBlock> blockDataNodeMappings = new HashMap<>();    // 数据块-数据节点映射, 由数据节点发送
    public final HashMap<String, DFSDataNodeState> activeDatanodes = new HashMap<>();   // 当前活跃数据节点状态
    public final HashMap<String, ArrayList<String> > toDelBlocks = new HashMap<>();

    // 白名单include, exclude
    public final ArrayList<String> includeNodes = new ArrayList<>();
    public final ArrayList<String> excludeNodes = new ArrayList<>();

    // 一致性哈希
    final DFSConsistentHashing consistentHash = new DFSConsistentHashing();

    DFSDataNodeMonitor myDataNodeMonitor;
    Registry registry;  // RMI Registry
    String rpcIp, rpcPort;  // RMI 服务端口号

    public void initialize(){
        // 读取永久化存储的Inode和FileBlock Mapping
        loadState();

        // 读取数据服务器白名单
        try {
            readIncludeFile();
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed to read include/exclude file! Check if the file is exists!");
        }

        readConfigFile();
    }

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
            fileBlockMapping = new HashMap<>();
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
            dfsUsers = new HashMap<>();
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

    public void readConfigFile(){
        Properties props = new Properties();
        try{
            InputStream fin = new FileInputStream("namenode.xml");
            props.loadFromXML(fin);
            fin.close();
            rpcIp = props.getProperty("ip");
            rpcPort = props.getProperty("port");
            DFSUser.initUserSpace = Integer.valueOf(props.getProperty("inituserspace"));
            DFSUser.initUserSpace *= 1024*1024; // 转化为Byte
            intervalTime = Integer.valueOf(props.getProperty("intervaltime"));
        }
        catch (Exception e){
            System.out.println("[ERROR!] Read config file error!");
        }
    }

    public void run(){
        try{
            // 启动 rmiregistry
            registry = LocateRegistry.createRegistry(Integer.valueOf(rpcPort));
            // 绑定 RMI 服务
            Naming.rebind("rmi://"+rpcIp+":"+rpcPort+"/DFSNameNode", this);
            myDataNodeMonitor = new DFSDataNodeMonitor(activeDatanodes, excludeNodes, consistentHash, blockDataNodeMappings, intervalTime);
            myDataNodeMonitor.start();
            System.out.println("[INFO] The NameNode is running...");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void close(){
        try{
            // 数据永久化
            storeState();
            writeIncludeFile();
            // 关闭 rmiregistry
            if(registry!=null)
                UnicastRemoteObject.unexportObject(registry,true);
            myDataNodeMonitor.ifRun = false;
            System.out.println("[INFO] The NameNode RMI is closed.");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void readIncludeFile() throws FileNotFoundException {
        File includeFile = new File("include");
        File excludeFile = new File("exclude");
        try{
            if(!includeFile.exists()){
                includeFile.createNewFile();
            }
            else{
                FileReader fReader = new FileReader(includeFile);
                BufferedReader bfReader = new BufferedReader(fReader);
                String tmpS;
                while((tmpS = bfReader.readLine())!=null){
                    includeNodes.add(tmpS);
                }
            }
            if(!excludeFile.exists()){
                excludeFile.createNewFile();
            }
            else{
                FileReader fReader = new FileReader(excludeFile);
                BufferedReader bfReader = new BufferedReader(fReader);
                String tmpS;
                while((tmpS = bfReader.readLine())!=null){
                    excludeNodes.add(tmpS);
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void writeIncludeFile(){
        File includeFile = new File("include");
        File excludeFile = new File("exclude");
        try{
            BufferedWriter includeWriter = new BufferedWriter(new FileWriter(includeFile));
            BufferedWriter excludeWriter = new BufferedWriter(new FileWriter(excludeFile));
            for(String datanodeID:includeNodes){
                includeWriter.write(datanodeID + "\n");
                excludeWriter.write(datanodeID + "\n");
            }
            includeWriter.close();
            excludeWriter.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void listBlocks(){
        for (Map.Entry<String, DFSBlock> entry : blockDataNodeMappings.entrySet()){
            System.out.println(entry.getKey() + "    Affiliated with DataNode: " + entry.getValue().getDatanodeID());
        }
    }

    public void listDataNodes(){
        for(Map.Entry<String, DFSDataNodeState> entry : activeDatanodes.entrySet()){
            System.out.println(entry.getKey());
        }
    }

    public void listFileBlockMappings(){
        for (Map.Entry<String, DFSFileBlockMapping> entry : fileBlockMapping.entrySet()){
            System.out.println("---"+entry.getKey());
            DFSFileBlockMapping thefileBlockMapping = entry.getValue();
            for (String block:thefileBlockMapping.blocks){
                System.out.println("          ---"+block);
            }
        }
    }

    /**
     * @param datanodeID 数据节点标识符
     * @return 如果数据节点未连接返回1；如果数据节点正在连接返回2；如果数据节点不在白名单中返回0
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

    @Override
    public ArrayList<String> sendDataNodeJump(String datanodeID) throws RemoteException{
        // 心跳监控
        if(checkNodeConnection(datanodeID) == 2){
            if(!activeDatanodes.containsKey(datanodeID))
                throw new RemoteException();
            DFSDataNodeState datanode = activeDatanodes.get(datanodeID);
            datanode.setLastJumpTime(LocalDateTime.now());
            if(toDelBlocks.containsKey(datanodeID)){
                ArrayList<String> res = toDelBlocks.get(datanodeID);
                toDelBlocks.remove(datanodeID);
                return res;
            }
            else
                return null;
        }
        else{
            return null;
        }
    }

    @Override
    public void sendDataNodeStates(DFSDataNodeState datanode) throws RemoteException{
        // 节点状态监控
        if(checkNodeConnection(datanode.getDatanodeID()) == 2){
            // 更新数据节点信息
            datanode.setLastJumpTime(LocalDateTime.now());
            activeDatanodes.put(datanode.getDatanodeID(), datanode);
            // 更新一致性哈希
            consistentHash.updateNode(datanode.getDatanodeID(), datanode.getFreeSpace());
        }
    }

    @Override
    public void sendDataNodeBlockList(String datanodeID, ArrayList<DFSBlock> blocks) throws RemoteException{
        if(checkNodeConnection(datanodeID) == 2){
            // 清空该DataNode原有的映射
            Iterator it = blockDataNodeMappings.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<String, DFSBlock> entry = (Map.Entry<String, DFSBlock>)it.next();
                DFSBlock block = entry.getValue();
                if (block.getDatanodeID().equals(datanodeID))
                    it.remove();
            }
            for(DFSBlock block:blocks){
                // 加入数据块-数据节点映射中
                blockDataNodeMappings.put(block.getBlockName(), block);
            }
//            if(!toDelBlocks.containsKey(datanodeID)){
//                return;
//            }
//            ArrayList<String> canDel = new ArrayList<>();
//            for(String toDelblock:toDelBlocks.get(datanodeID)){
//                canDel.add(toDelblock);
//            }
//            for(String canDelBlock:canDel){
//                toDelBlocks.remove(canDelBlock);
//            }
        }
    }

    @Override
    public boolean registerDataNode(DFSDataNodeState datanode) throws RemoteException{
        if(checkNodeConnection(datanode.getDatanodeID()) == 1){
            // 标记服务器已连接
            excludeNodes.remove(datanode.getDatanodeID());
            // 加入活跃数据节点列表
            datanode.setLastJumpTime(LocalDateTime.now());
            activeDatanodes.put(datanode.getDatanodeID(), datanode);
            // 注册一致性哈希节点
            consistentHash.addNode(datanode.getDatanodeID());
            // 数据迁移
            blocksTransToThisDataNode(datanode.getDatanodeID());
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public boolean unRegisterDataNode(String datanodeID) throws RemoteException{
        if(checkNodeConnection(datanodeID) == 2){
            // 删除数据节点下的映射
//            Iterator< Map.Entry<String, DFSBlock> > iter = blockDataNodeMappings.entrySet().iterator();
//            while(iter.hasNext()){
//                Map.Entry<String, DFSBlock> entry = iter.next();
//                if(entry.getValue().getDatanodeID().equals(datanodeID)){
//                    // 循环中删除调用Iterator的remove才是安全的
//                    iter.remove();
//                }
//            }
            // 删除一致性哈希节点
            consistentHash.removeNode(datanodeID);
            blocksTransToOtherDataNode(datanodeID);
            // 从活跃数据节点列表中删除。（顺序很重要，要在传输完数据块后删除）
            if(activeDatanodes.containsKey(datanodeID))
                activeDatanodes.remove(datanodeID);
            // 重新标记为未连接
            excludeNodes.add(datanodeID);
            return true;
        }
        return false;
    }

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

    @Override
    public boolean login(String userName, String password) throws RemoteException{
        if(dfsUsers.containsKey(userName) && password.equals(dfsUsers.get(userName).base64Password)){
            DFSUser theUser = dfsUsers.get(userName);
            return password.equals(theUser.base64Password);
        }
        else{
            return false;
        }
    }

    @Override
    public boolean changePassword(String userName, String password, String newPassword) throws RemoteException{
        if(dfsUsers.containsKey(userName)){
            DFSUser theUser = dfsUsers.get(userName);
            if(password.equals(theUser.base64Password)){
                theUser.base64Password = newPassword;
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

    @Override
    public long getUserTotalSpace(String userName) throws RemoteException, UserNotFoundException{
        if(dfsUsers.containsKey(userName)){
            return dfsUsers.get(userName).maxSpace;
        }
        else{
            throw new UserNotFoundException();
        }
    }

    @Override
    public long getUserUsedSpace(String userName) throws RemoteException, UserNotFoundException{
        if(dfsUsers.containsKey(userName)){
            return dfsUsers.get(userName).usedSpace;
        }
        else{
            throw new UserNotFoundException();
        }
    }

    @Override
    public void setUserTotalSpace(String userName, long totalSpace) throws RemoteException, UserNotFoundException{
        if(dfsUsers.containsKey(userName)){
            dfsUsers.get(userName).maxSpace = totalSpace;
        }
        else{
            throw new UserNotFoundException();
        }
    }

    @Override
    public DFSINode getDFSINode(String userName, String path) throws RemoteException, UserNotFoundException, FileNotFoundException{
        DFSINode theInode = null;
        String inodePath = getINodePath(userName, path);
        try {
            theInode = DFSINode.updateDFSINode(inode, inodePath, 0);
        } catch (FileAlreadyExistsException e) {
            System.out.println("updateDFSInode function error!");
        }
        return theInode;
    }

    @Override
    public ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> >
    addDFSFile(String userName,
               String filePath,
               long fileSize,
               int blockNum)
            throws RemoteException, NotBoundException, UserNotFoundException, NoEnoughSpaceException, FileNotFoundException, FileAlreadyExistsException {

        if(!dfsUsers.containsKey(userName)){
            throw new UserNotFoundException();
        }
        if(dfsUsers.get(userName).getFreeSpace() < fileSize){
            throw new NoEnoughSpaceException();
        }

        ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> > blockDatanodes = new ArrayList<>();
        String inodePath = getINodePath(userName, filePath);
        // 新建File-Block Mapping节点
        DFSFileBlockMapping newFileBlockMapping = new DFSFileBlockMapping();
        newFileBlockMapping.filePath = inodePath;
        newFileBlockMapping.fileSize = fileSize;
        newFileBlockMapping.ifLittleFile = false;
        newFileBlockMapping.blocks = new ArrayList<>();

        // 生成block标识
        for (int i = 0; i < blockNum; ++i) {
            UUID newUUID = UUID.randomUUID();
            // 加入到file的block列表中
            newFileBlockMapping.blocks.add(newUUID.toString());

            // 为数据块分配数据节点-负载均衡
            String datanodeID = consistentHash.getNode(newUUID.toString());
            if(datanodeID == null){
                throw new NotBoundException();
            }

            // 从当前活跃数据节点中查找对应ID的节点ip
            if(!activeDatanodes.containsKey(datanodeID)){
                throw new NotBoundException();
            }
            DFSDataNodeState dataNodeState = activeDatanodes.get(datanodeID);

            blockDatanodes.add(new AbstractMap.SimpleEntry<>(newUUID.toString(), new DFSDataNodeRPCAddress(dataNodeState.addr)));

            // 由数据节点向主控节点发送Block-DataNode映射
        }

        // 将File添加进Inode文件节点
        DFSINode.updateDFSINode(inode, inodePath, 11);

        // 添加本次的File-Block Mapping
        fileBlockMapping.put(inodePath, newFileBlockMapping);

        // 更新用户已占用空间
        dfsUsers.get(userName).usedSpace += fileSize;

        return blockDatanodes;
    }

    @Override
    public ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> >
    lookupFileBlocks(String userName, String filePath)
            throws RemoteException, NotBoundException, UserNotFoundException, FileNotFoundException{

        ArrayList< Map.Entry<String, DFSDataNodeRPCAddress> > blockDatanodes = new ArrayList<>();
        String inodePath = getINodePath(userName, filePath);
        // 获取文件对应的数据块映射对象
        if(!fileBlockMapping.containsKey(inodePath)){
            throw new FileNotFoundException();
        }
        DFSFileBlockMapping fileBlocks = fileBlockMapping.get(inodePath);
        // 遍历数据块列表
        for (String block:fileBlocks.blocks){
            // 在数据块-数据节点映射中查询数据块所属数据节点标识
            if(!blockDataNodeMappings.containsKey(block)){
                throw new NotBoundException();
            }
            String datanodeID = blockDataNodeMappings.get(block).getDatanodeID();
            // 从当前活跃数据节点中查找对应ID的节点ip
            if(!activeDatanodes.containsKey(datanodeID))
                throw new NotBoundException();
            DFSDataNodeState datanodeState = activeDatanodes.get(datanodeID);

            blockDatanodes.add(new AbstractMap.SimpleEntry<>(block, new DFSDataNodeRPCAddress(datanodeState.addr)));
        }
        return blockDatanodes;
    }

    @Override
    public void
    deleteDFSFile(String userName, String filePath)
            throws RemoteException, UserNotFoundException, FileNotFoundException{
        if(!dfsUsers.containsKey(userName)){
            throw new UserNotFoundException();
        }
        String inodePath = getINodePath(userName, filePath);

        long fileSize = deleteDFSFile(inodePath);

        // 更新用户空间
        dfsUsers.get(userName).usedSpace -= fileSize;
    }

    @Override
    public void renameDFSINode(String userName, String nodePath, String newNodePath, boolean ifFile) throws RemoteException, UserNotFoundException, FileNotFoundException, FileAlreadyExistsException{
        String inodePath = getINodePath(userName, nodePath);
        String newInodePath = getINodePath(userName, newNodePath);
        // 检查新文件名是否已经存在
        if(ifExistsDFSINode(userName, newInodePath)){
            throw new FileAlreadyExistsException("");
        }
        // 如果是目录，取得其INode节点
        if(!ifFile){
            // add the new Dir
            try{
                DFSINode.updateDFSINode(inode, newInodePath, 1);
            }
            catch(FileNotFoundException e){
                System.out.println("updateDFSInode function error!");
            }
            // del the origin Dir
            DFSINode theDirINode = DFSINode.updateDFSINode(inode, inodePath, 0);
            try{
                DFSINode.updateDFSINode(inode, inodePath, 2);
            }
            catch(FileAlreadyExistsException e){
                System.out.println("updateDFSInode function error!");
            }

            DFSINode newDirINode = DFSINode.updateDFSINode(inode, newInodePath, 0);
            if (newDirINode != null && theDirINode != null)
                newDirINode.childInode = theDirINode.childInode;
        }
        else{
            // add the new file
            try{
                DFSINode.updateDFSINode(inode, newInodePath, 11);
            }
            catch(FileNotFoundException e){
                System.out.println("updateDFSInode function error!");
            }
            // del the origin file
            try{
                DFSINode.updateDFSINode(inode, inodePath, 12);
            }
            catch(FileAlreadyExistsException e){
                System.out.println("updateDFSInode function error!");
            }
            // 改变文件-数据块映射中的路径
            // get the file mapping
            DFSFileBlockMapping tmpFileBlockMapping = fileBlockMapping.get(inodePath);
            // remove and put the new file mapping
            fileBlockMapping.remove(inodePath);
            tmpFileBlockMapping.filePath = newInodePath;
            fileBlockMapping.put(newInodePath, tmpFileBlockMapping);
        }
    }

    @Override
    public void addDFSDirectory(String userName, String dirPath) throws RemoteException, UserNotFoundException, FileNotFoundException, FileAlreadyExistsException{
        String inodePath = getINodePath(userName, dirPath);
        try{
            DFSINode.updateDFSINode(inode, inodePath, 1);
        }
        catch(FileNotFoundException e){
            System.out.println("updateDFSInode function error!");
        }
    }

    @Override
    public void deleteDFSDirectory(String userName, String dirPath) throws RemoteException, UserNotFoundException, FileNotFoundException{
        String inodePath = getINodePath(userName, dirPath);
        try{
            // 获取目录下的所有文件
            DFSINode theInode = DFSINode.updateDFSINode(inode, inodePath, 0);
            long fileSize = deleteFilesFromDirectory(theInode, inodePath);
            // 更新用户空间
            dfsUsers.get(userName).usedSpace -= fileSize;
            // 删除目录
            DFSINode.updateDFSINode(inode, inodePath, 2);
        }
        catch(FileAlreadyExistsException e){
            System.out.println("updateDFSInode function error!");
        }
    }

    @Override
    public void clearDFSDirectory(String userName, String dirPath) throws RemoteException, FileNotFoundException, UserNotFoundException{
        String inodePath = getINodePath(userName, dirPath);
        try{
            // 获取目录下的所有文件
            DFSINode theInode = DFSINode.updateDFSINode(inode, inodePath, 0);
            long fileSize = deleteFilesFromDirectory(theInode, inodePath);
            if(theInode != null && theInode.childInode != null){
                theInode.childInode = new HashMap<>();
            }
            // 更新用户空间
            dfsUsers.get(userName).usedSpace -= fileSize;
        }
        catch(FileAlreadyExistsException e){
            System.out.println("updateDFSInode function error!");
        }
    }

    @Override
    public boolean ifExistsDFSINode(String userName, String path) throws RemoteException, UserNotFoundException{
        boolean res = false;
        try{
            String inodePath = getINodePath(userName, path);
            res = (DFSINode.updateDFSINode(inode, inodePath, 0) != null);
        }
        catch (FileNotFoundException e){
            return false;
        }
        catch (FileAlreadyExistsException e){
            System.out.println("updateDFSInode function error!");
        }
        return res;
    }

    private String getINodePath(String userName, String dfsPath) throws FileNotFoundException, UserNotFoundException{
        if(!dfsUsers.containsKey(userName)){
            throw new UserNotFoundException();
        }
        String res = userName + dfsPath;
        // 消除末尾分隔符
        if(res.charAt(res.length()-1) == DFSINode.splitChar)
            res = res.substring(0, res.length()-1);
        return res;
    }

    private long deleteDFSFile(String inodeFilePath) throws FileNotFoundException{
        // 删除对应的文件-数据块映射
        long fileSize;
        if(fileBlockMapping.containsKey(inodeFilePath)){
            // 获取文件对应的数据块列表
            DFSFileBlockMapping fileBlocks = fileBlockMapping.get(inodeFilePath);
            fileSize = fileBlocks.fileSize;
            // 获取所有数据块对应的数据节点
            for (String block:fileBlocks.blocks){
                if(!blockDataNodeMappings.containsKey(block)){
                    continue;
                }
                String datanodeID = blockDataNodeMappings.get(block).getDatanodeID();
                if(!toDelBlocks.containsKey(datanodeID)){
                    toDelBlocks.put(datanodeID, new ArrayList<>());
                }
                // 将数据块加入节点的待删除列表
                toDelBlocks.get(datanodeID).add(block);
            }
            // 删除对应INode节点信息
            try{
                DFSINode.updateDFSINode(inode, inodeFilePath, 12);
            }
            catch (FileAlreadyExistsException e){
                System.out.println("updateDFSInode function error!");
            }
            // 删除对应的文件-数据块列表
            fileBlockMapping.remove(inodeFilePath);
        }
        else{
            throw new FileNotFoundException();
        }
        return fileSize;
    }

    private long deleteFilesFromDirectory(DFSINode inodePath, String dfsPath){
        if(inodePath == null || inodePath.childInode == null)
            return 0;
        long fileSize = 0;
        ArrayList<String> toDelFiles = new ArrayList<>();
        for(Map.Entry<String, DFSINode> entry : inodePath.childInode.entrySet()){
            DFSINode child = entry.getValue();
            // 文件
            if (child.childInode == null){
                toDelFiles.add(dfsPath+DFSINode.splitStr+child.name);
            }
            else{
                fileSize += deleteFilesFromDirectory(child, dfsPath + DFSINode.splitStr + child.name);
            }
        }
        for (String toDelFilePath : toDelFiles){
            try{
                fileSize += deleteDFSFile(toDelFilePath);
            }
            catch (FileNotFoundException e){
                System.out.println("deleteFilesFromDirectory function error!");
            }
        }
        return fileSize;
    }

    private void blocksTransToThisDataNode(String datanodeID){
        ArrayList<String>blocks = new ArrayList<>();
        String blocksDatanodeIp = null, blocksDatanodePort = null;
        for(Map.Entry<String, DFSBlock> entry : blockDataNodeMappings.entrySet()){
            DFSBlock block = entry.getValue();
            String blockName = block.getBlockName();
            String newNodeID = consistentHash.getNode(blockName);
            if (newNodeID.equals(datanodeID) && !newNodeID.equals(block.getDatanodeID())){
                if(!activeDatanodes.containsKey(block.getDatanodeID()))
                    continue;
                DFSDataNodeState blockDataNode = activeDatanodes.get(block.getDatanodeID());
                blocksDatanodeIp = blockDataNode.addr.getIp();
                blocksDatanodePort = blockDataNode.addr.getNamenoderpcport();
                blocks.add(blockName);
            }
        }
        if(blocksDatanodeIp == null || blocksDatanodePort == null || blocks.size() == 0)
            return;
        DFSDataNodeRPCAddress toDataNodeAddr = activeDatanodes.get(datanodeID).addr;
        try{
            NameNodeDataNodeRPCInterface nameNodeDataNodeRPC = (NameNodeDataNodeRPCInterface)Naming.lookup("rmi://"+blocksDatanodeIp+":"+blocksDatanodePort+"/DFSDataNodeToNameNode");
            nameNodeDataNodeRPC.sendBlocksTo(toDataNodeAddr.getIp(), toDataNodeAddr.getDatanoderpcport(), blocks);
        }
        catch (Exception e){
            System.out.println("[ERROR!] blocksTransToThisDataNode Function Error!");
        }
    }

    private void blocksTransToOtherDataNode(String datanodeID){
        ArrayList<String>blocks = new ArrayList<>();
        String toDatanodeIp = null, toDatanodePort = null;
        for(Map.Entry<String, DFSBlock> entry : blockDataNodeMappings.entrySet()){
            DFSBlock block = entry.getValue();
            String blockName = block.getBlockName();
            if(block.getDatanodeID().equals(datanodeID)){
                String newNodeID = consistentHash.getNode(blockName);
                if(newNodeID == null){
                    return;
                }
                DFSDataNodeState blockDataNode = activeDatanodes.get(newNodeID);
                toDatanodeIp = blockDataNode.addr.getIp();
                toDatanodePort = blockDataNode.addr.getDatanoderpcport();
                blocks.add(blockName);
            }
        }
        if(toDatanodeIp == null || toDatanodePort == null || blocks.size() == 0)
            return;
        DFSDataNodeRPCAddress fromDataNodeAddr = activeDatanodes.get(datanodeID).addr;
        try{
            NameNodeDataNodeRPCInterface nameNodeDataNodeRPC = (NameNodeDataNodeRPCInterface)Naming.lookup("rmi://"+fromDataNodeAddr.getIp()+":"+fromDataNodeAddr.getNamenoderpcport()+"/DFSDataNodeToNameNode");
            nameNodeDataNodeRPC.sendBlocksTo(toDatanodeIp, toDatanodePort, blocks);
        }
        catch (Exception e){
            System.out.println("[ERROR!] blocksTransToThisDataNode Function Error!");
        }
    }
}
