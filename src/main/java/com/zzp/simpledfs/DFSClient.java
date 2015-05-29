package main.java.com.zzp.simpledfs;

import java.io.*;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Zhipeng Zhang on 15/05/29 0029.
 */
public class DFSClient {
    ClientNameNodeRPCInterface clientRmi;
    String namenodeIp, namenodePort;  // 连接的NameNode ip, port
    int blockSize;
    DFSClient(){
        // 读取配置文件client.xml
        readConfigFile("client.xml");

        try {
            clientRmi = (ClientNameNodeRPCInterface) Naming.lookup("rmi://localhost:2020/DFSNameNode");
            //clientRmi = (ClientNameNodeRmiInterface) Naming.lookup("rmi://"+namenodeIp+":"+namenodePort+"/DFSNameNode");
        }
        catch (Exception e){
            System.out.println("[ERROR!] Can't link to the NameNode RMI Server!");
            System.exit(0);
        }
    }
    private void readConfigFile(String file){
        Properties props = new Properties();
        try{
            InputStream fin = new FileInputStream(file);
            props.loadFromXML(fin);
            fin.close();
            namenodeIp = props.getProperty("namenodeip");
            namenodePort = props.getProperty("namenodeport");
            blockSize = Integer.valueOf(props.getProperty("blocksize"));
            blockSize *= 1024*1024;     // MB换算成Byte
        }
        catch (Exception e){
            System.out.println("[ERROR!] Read config file error!");
        }
    }

    public boolean registerUser(String userName, String password){
        
        return false;
    }
    /**
     * @param dfsPath DFS目录绝对路径
     */
    public void mkdir(String dfsPath){
        try{
            clientRmi.addDFSDirectory(dfsPath);
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

    /**
     * @param dfsPath DFS目录绝对路径
     */
    public void rmdir(String dfsPath){
        // ！！！需要检查是否为当前目录或当前父目录，不能删除
        try{
            clientRmi.delDFSDirectory(dfsPath);
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] Illegal path!");
        }
        catch (Exception e){
            System.out.println("[ERROR!] The namenode RMI serve is not found!");
        }
    }

    /**
     * 检查该DFS路径是否存在
     * @param dfsPath DFS目录绝对路径
     */
    public boolean checkdir(String dfsPath) throws RemoteException{
        boolean res = false;
        res = clientRmi.ifExistsDFSDirectory(dfsPath);
        return res;
    }

    /**
     * @param originPath 原DFS目录绝对路径
     * @param newPath 新DFS目录绝对路径
     */
    public void renameFile(String originPath, String newPath){
        try{
            clientRmi.renameDFSFile(originPath, newPath);
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

    /**
     * @param localFilePath 本地文件绝对路径或者相对路径
     * @param dfsPath DFS目录绝对路径
     */
    public void uploadFile(String localFilePath, String dfsPath){
        try{
            // 读取本地文件返回字节流
            ArrayList<byte[]> byteblocks = transToByte(new File(localFilePath));
            // 向NameNode发送请求，新建Inode文件节点，分配数据块标识，分配数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.newDFSFileMapping(dfsPath, byteblocks.size(), false);
            if(blockDatanodes == null){
                System.out.println("[ERROR!] File block not found!");
                return ;
            }
            System.out.println("[INFO] File is devided into "+blockDatanodes.size()+" blocks, begin to upload...");
            int i = 0;
            for(Map.Entry<String, String> eachTrans:blockDatanodes){
                String block = eachTrans.getKey();
                //String datanodeip = eachTrans.getValue();
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

    /**
     * @param dfsPath DFS目录绝对路径
     * @param localPath 本地绝对路径或者相对路径
     */
    public void downloadFile(String dfsPath, String localPath){
        try{
            // 向NameNode询问某文件的数据块标识及数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.lookupFileBlocks(dfsPath);
            System.out.println("[INFO] Find "+blockDatanodes.size()+" blocks, begin to download...");
            File wfile = new File(localPath);
            BufferedOutputStream bufOut = new BufferedOutputStream(new FileOutputStream(wfile));
            int i = 1;
            for (Map.Entry<String, String> blockDatanode:blockDatanodes){
                String block = blockDatanode.getKey();
                //String datanodeip = blockDatanode.getValue();
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

    /**
     * @param dfsPath DFS目录绝对路径
     */
    public void removeFile(String dfsPath){
        try{
            // 向NameNode询问某文件的数据块标识及数据节点
            // 返回数据块标识和存储数据节点
            ArrayList<Map.Entry<String, String> > blockDatanodes =  clientRmi.removeDFSFile(dfsPath);
            int i = 1;
            for (Map.Entry<String, String> blockDatanode:blockDatanodes){
                String block = blockDatanode.getKey();
                //String datanodeip = blockDatanode.getValue();
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
     * @param localFile 本地文件的绝对路径或相对路径
     * @return 返回切割后的byte数据块列表
     */
    private ArrayList<byte[]> transToByte(File localFile) throws IOException{
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
