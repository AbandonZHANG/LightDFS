package main.java.com.zzp.app;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.*;

/**
 * Created by Zhipeng Zhang on 15/05/26 0026.
 */
public class DFSClient {
    ClientNameNodeRPCInterface clientRmi;
    Stack<String> dir;  // 当前client所在DFS目录，初始为根目录root
    String namenodeIp, namenodePort;  // 连接的NameNode ip, port
    int blockSize;
    public static void main(String[] args){
        DFSClient client = new DFSClient();
        client.printHello();
        client.initialize();
        client.readUserCommand();
    }
    public void printHello(){
        System.out.println("***************************************************************************");
        System.out.println("**                                                                       **");
        System.out.println("**                                                                       **");
        System.out.println("**                Welcome to use SimpleDFS! (version 0.1.0)              **");
        System.out.println("**                                                                       **");
        System.out.println("**                         Author: Zhipeng Zhang                         **");
        System.out.println("**                                                                       **");
        System.out.println("**                                 Client                                **");
        System.out.println("**                                                                       **");
        System.out.println("**                   Run 'help' to display the help index.               **");
        System.out.println("**                                                                       **");
        System.out.println("**                                                                       **");
        System.out.println("***************************************************************************");
    }
    public void initialize(){
        dir = new Stack<String>();
        dir.clear();
        dir.push("root");    // 根目录是root

        // 读取配置文件client.xml
        readConfigFile();

        try {
            clientRmi = (ClientNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
            //clientRmi = (ClientNameNodeRmiInterface) Naming.lookup("rmi://"+namenodeIp+":"+namenodePort+"/DFSNameNode");
        }
        catch (Exception e){
            System.out.println("[ERROR!] Can't link to the NameNode RMI Server!");
            System.exit(0);
        }
    }
    /**
     * 从控制台读取用户命令
     */
    public void readUserCommand(){
        String userCmd;
        Scanner in = new Scanner(System.in);
        System.out.print("$["+getCurrentPath()+"]:");
        while(in.hasNext()){
            userCmd = in.nextLine();
            String[] cmdArray = userCmd.split(" ");
            if(cmdArray[0].equals("help")){
                printHelp();
            }
            else if(cmdArray[0].equals("ls")){

            }
            else if(cmdArray[0].equals("cd")){
                if(cmdArray.length < 2){
                    System.out.println("Wrong command format!");
                }
                else if(cmdArray[1].equals(".."))
                    dir.pop();
                else{
                    try{
                        if(clientRmi.ifExistsDFSDirectory(getAbsolutelyPath(cmdArray[1]))){
                            String[] paths = cmdArray[1].split("\\\\");
                            if(paths[0].equals("") || paths[0].equals("root")){
                                // 绝对路径
                                dir.clear();
                            }
                            for(String eachpath:paths){
                                if(eachpath.equals(""))
                                    dir.push("root");
                                else
                                    dir.push(eachpath);
                            }
                        }
                        else{
                            System.out.println("The DFS doesn't have this directory!");
                        }
                    }
                    catch (Exception e){
                        System.out.println("Can't link to the NameNode RMI Server!");
                    }
                }
            }
            else if(cmdArray[0].equals("mkdir")){
                if(cmdArray.length != 2){
                    System.out.println("Wrong command format!");
                }
                else{
                    mkdir(cmdArray[1]);
                }
            }
            else if(cmdArray[0].equals("rmdir")){
                if(cmdArray.length != 2){
                    System.out.println("Wrong command format!");
                }
                else{
                    rmdir(cmdArray[1]);
                }
            }
            else if(cmdArray[0].equals("upload")){
                if(cmdArray.length != 3){
                    System.out.println("Wrong command format!");
                }
                else{
                    uploadFile(cmdArray[1], cmdArray[2]);
                }
            }
            else if(cmdArray[0].equals("download")){
                if(cmdArray.length != 3){
                    System.out.println("Wrong command format!");
                }
                else{
                    downloadFile(cmdArray[1], cmdArray[2]);
                }
            }
            else if(cmdArray[0].equals("renamefile")){
                if(cmdArray.length != 3){
                    System.out.println("Wrong command format!");
                }
                else{
                    renameFile(cmdArray[1], cmdArray[2]);
                }
            }
            else if(cmdArray[0].equals("removefile")){
                if(cmdArray.length != 2){
                    System.out.println("Wrong command format!");
                }
                else{
                    removeFile(cmdArray[1]);
                }
            }
            else if(cmdArray[0].equals("quit")){
                break;
            }
            else{
                System.out.println("[ERROR!] Command Not Found!");
                printHelp();
            }
            System.out.print("$["+getCurrentPath()+"]:");
        }
    }
    /**
     * 读取 Properties 配置文件:client.xml
     */
    public void readConfigFile(){
        Properties props = new Properties();
        try{
            InputStream fin = new FileInputStream("client.xml");
            props.loadFromXML(fin);
            fin.close();
            namenodeIp = props.getProperty("namenodeip");
            namenodePort = props.getProperty("namenodeport");
            blockSize = Integer.valueOf(props.getProperty("blocksize"));
            blockSize *= 1024*1024;
        }
        catch (Exception e){
            System.out.println("[ERROR!] Read config file error!");
        }
    }
    public void printHelp(){
        System.out.println("");
        System.out.println("The SimpleDFS Client commands are:");
        System.out.println("    ls [path]                         List the DFS directory.");
        System.out.println("    mkdir [path]                      Make a new DFS directory.");
        System.out.println("    rmdir [path]                      Remove a designated DFS directory.");
        System.out.println("    cd [path]                         Enter a designated DFS directory.");
        System.out.println("    upload [localpath] [dfspath]      Upload a local file to the DFS.");
        System.out.println("    download [dfspath] [localpath]    Download a DFS file.");
        System.out.println("    removefile [dfspath]              Remove a DFS file.");
        System.out.println("    renamefile [oripath] [newpath]    Rename a DFS file.");
        System.out.println("    quit                              Quit the system.");
        System.out.println("");
    }
    /**
     * 将当前目录各级dir转化为String
     * @return
     */
    public String getCurrentPath(){
        String res = new String();
        for(String eachpath:dir){
            res += eachpath+"\\";
        }
        return res;
    }
    /**
     * 将参数的路径转换为绝对路径
     * @param pathName
     * @return
     */
    public String getAbsolutelyPath(String pathName){
        String res = new String();
        String[] paths = pathName.split("\\\\");
        if(!paths[0].equals("") && !paths[0].equals("root")){
            /**
             * 如果不是绝对路径，前缀加上
             */
            for(String eachdir:dir){
                res += eachdir + "\\";
            }
        }
        for(int i = 0; i < paths.length; ++i){
            String eachpath = paths[i];
            if(i == paths.length-1){
                if(eachpath.equals(""))
                    res += "root";
                else
                    res += eachpath;
            }
            else{
                if(eachpath.equals(""))
                    res += "root"+"\\";
                else
                    res += eachpath+"\\";
            }

        }
        return res;
    }
    public void mkdir(String name){
        String path = getAbsolutelyPath(name);
        try{
            clientRmi.addDFSDirectory(path);
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] Illegal path!");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("[ERROR!] Directory has exists!");
        }
        catch (Exception e){
            System.out.println("[ERROR!] The namenode RMI serve is not found!");
        }
    }
    public void rmdir(String name){
        String path = getAbsolutelyPath(name);
        // ！！！需要检查是否为当前目录或当前父目录，不能删除
        try{
            clientRmi.delDFSDirectory(path);
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] Illegal path!");
        }
        catch (Exception e){
            System.out.println("[ERROR!] The namenode RMI serve is not found!");
        }
    }
    public void renameFile(String originPath, String newPath){
        String theNewPath = getAbsolutelyPath(newPath);
        try{
            clientRmi.renameDFSFile(originPath, theNewPath);
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] Illegal origin file path!");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("[ERROR!] The new file path has exists!");
        }
        catch (Exception e){
            System.out.println("[ERROR!] The namenode RMI serve is not found!");
        }
    }
    public void uploadFile(String localFilePath, String path){
        String DFSPath = getAbsolutelyPath(path);
        try{
            // 读取本地文件返回字节流
            ArrayList<byte[]> byteblocks = transToByte(new File(localFilePath));
            // 向NameNode发送请求，新建Inode文件节点，分配数据块标识，分配数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.newDFSFileMapping(DFSPath, byteblocks.size(), false);
            if(blockDatanodes == null){
                System.out.println("[ERROR!] File block not found!");
                return ;
            }
            System.out.println("[INFO] File is devided into "+blockDatanodes.size()+" blocks, begin to upload...");
            int i = 0;
            for(Map.Entry<String, String> eachTrans:blockDatanodes){
                String block = eachTrans.getKey();
                String datanodeip = eachTrans.getValue();
                ClientDataNodeRPCInterface transRmi = (ClientDataNodeRPCInterface)Naming.lookup("rmi://localhost:2021/DFSDataNode");
                //ClientDataNodeRmiInterface transRmi = (ClientDataNodeRmiInterface)Naming.lookup("rmi://"+datanodeip+":2021/DFSDataNode");
                transRmi.uploadBlock(byteblocks.get(i), block);
                System.out.println("[INFO] upload "+(i+1)+" blocks ...    ["+((i+1)*100/blockDatanodes.size())+"%]");
                i ++;
            }
            System.out.println("[SUCCESS!] Upload File Successful!");
        }
        catch (RemoteException e){
            System.out.println("[ERROR!] The namenode RMI serve is not found!");
        }
        catch (IOException e){
            System.out.println("[ERROR!] File path error!");
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed upload!");
        }
    }
    public void downloadFile(String path, String localPath){
        String DFSPath = getAbsolutelyPath(path);
        try{
            // 向NameNode询问某文件的数据块标识及数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.lookupFileBlocks(DFSPath);
            System.out.println("[INFO] Find "+blockDatanodes.size()+" blocks, begin to download...");
            File wfile = new File(localPath);
            BufferedOutputStream bufOut = new BufferedOutputStream(new FileOutputStream(wfile));
            int i = 1;
            for (Map.Entry<String, String> blockDatanode:blockDatanodes){
                String block = blockDatanode.getKey();
                String datanodeip = blockDatanode.getValue();
                ClientDataNodeRPCInterface transRmi = (ClientDataNodeRPCInterface)Naming.lookup("rmi://localhost:2021/DFSDataNode");
                //ClientDataNodeRmiInterface transRmi = (ClientDataNodeRmiInterface)Naming.lookup("rmi://"+datanodeip+":2021/DFSDataNode");
                byte[] content = transRmi.downloadBlock(block);
                bufOut.write(content);
                System.out.println("[INFO] download "+(i)+" blocks ...    ["+((i++)*100/blockDatanodes.size())+"%]");
            }
            bufOut.close();
            System.out.println("[SUCCESS!] Download File Successful!");
        }
        catch (RemoteException e){
            System.out.println("[ERROR!] The namenode RMI serve is not found!");
        }
        catch (IOException e){
            System.out.println("[ERROR!] File path error!");
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed download!");
        }
    }
    public void removeFile(String path){
        String DFSPath = getAbsolutelyPath(path);
        try{
            // 向NameNode询问某文件的数据块标识及数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.removeDFSFile(DFSPath);
            int i = 1;
            for (Map.Entry<String, String> blockDatanode:blockDatanodes){
                String block = blockDatanode.getKey();
                String datanodeip = blockDatanode.getValue();
                ClientDataNodeRPCInterface transRmi = (ClientDataNodeRPCInterface)Naming.lookup("rmi://localhost:2021/DFSDataNode");
                //ClientDataNodeRmiInterface transRmi = (ClientDataNodeRmiInterface)Naming.lookup("rmi://"+datanodeip+":2021/DFSDataNode");
                transRmi.deleteBlock(block);
            }
            System.out.println("[SUCCESS!] Remove File Successful!");
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed remove!");
        }
    }
    /**
     * 读入本地文件，将字节流存在byte[]中
     * @param localFile
     * @return
     */
    public ArrayList<byte[]> transToByte(File localFile) throws IOException{
        ArrayList<byte[]> byteBlocks = new ArrayList<byte[]>();
        // 计算切割blocks个数
        long bytelength = localFile.length();
        int blocks_num = (int)(bytelength / blockSize);   // block大小为64M
        if (localFile.length() % blockSize != 0)
            blocks_num++;
        BufferedInputStream bufIn = new BufferedInputStream(new FileInputStream(localFile));
        for(int i = 0; i < blocks_num; ++i){
            int blockLength = (int)Math.min((long)blockSize, bytelength);
            byte[] content = new byte[blockLength];    // 每个byte 64M
            bufIn.read(content, 0, blockLength);
            byteBlocks.add(content);
            bytelength -= blockSize;
        }
        bufIn.close();
        return byteBlocks;
    }
}
