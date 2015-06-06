package com.zzp.simpledfs.namenode;

import com.zzp.simpledfs.common.DFSINode;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

public class DFSNameNodeConsole {
    DFSNameNode rpcService;
    public static void main(String[] args){
        DFSNameNodeConsole namenode = new DFSNameNodeConsole();
        String hostIp = readHostIp();
        try{
            namenode.rpcService = DFSNameNode.getInstance(hostIp);
            namenode.rpcService.initialize();
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed to create the rpcService serve!");
        }
        namenode.printHello();
        namenode.readUserCommand();
    }
    public void printHello(){
        System.out.println("***************************************************************************");
        System.out.println("**                                                                       **");
        System.out.println("**                                                                       **");
        System.out.println("**                Welcome to use SimpleDFS! (version 0.1.0)              **");
        System.out.println("**                                                                       **");
        System.out.println("**                         Author: Zhipeng Zhang                         **");
        System.out.println("**                                                                       **");
        System.out.println("**                               NameNode                                **");
        System.out.println("**                                                                       **");
        System.out.println("**                   Run 'help' to display the help index.               **");
        System.out.println("**                                                                       **");
        System.out.println("**                                                                       **");
        System.out.println("***************************************************************************");
    }
    /**
     * 从控制台读取用户命令
     */
    public void readUserCommand(){
        String userCmd;
        Scanner in = new Scanner(System.in);
        System.out.print("$ ");
        while(in.hasNext()){
            userCmd = in.nextLine();
            String[] cmdArray = userCmd.split(" ");
            if(cmdArray[0].equals("help")){
                printHelp();
            }
            else if(cmdArray[0].equals("start")){
                rpcService.run();
            }
            else if(cmdArray[0].equals("close")){
                rpcService.close();
            }
            else if(cmdArray[0].equals("ls")){
                DFSINode.listDirTree(rpcService.inode, 1);
            }
            else if(cmdArray[0].equals("lsblocks")){
                rpcService.listBlocks();
            }
            else if(cmdArray[0].equals("lsfbmap")){
                rpcService.listFileBlockMappings();
            }
            else{
                System.out.println("[ERROR!] Command Not Found!");
                printHelp();
            }
            System.out.print("$ ");
        }
    }
    public void printHelp(){
        System.out.println("");
        System.out.println("The SimpleDFS NameNode commands are:");
        System.out.println("    start         Start the server job.");
        System.out.println("    close         Close the server job.");
        System.out.println("    ls            List the DFS directory.");
        System.out.println("    lsblocks      List the blocks list.");
        System.out.println("    lsfbmap       List the file-blocks list.");
        System.out.println("");
    }
    static public String readHostIp(){
        Properties props = new Properties();
        String hostIp=null;
        try{
            InputStream fin = new FileInputStream("namenode.xml");
            props.loadFromXML(fin);
            fin.close();
            hostIp = props.getProperty("ip");
        }
        catch (Exception e){
            System.out.println("[ERROR!] Read config file error!");
        }
        return hostIp;
    }
}
