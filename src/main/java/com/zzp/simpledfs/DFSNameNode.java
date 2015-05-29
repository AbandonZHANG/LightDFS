package main.java.com.zzp.simpledfs;

import java.util.Scanner;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSNameNode {
    DFSNameNodeRPC rpcService;
    public static void main(String[] args){
        DFSNameNode namenode = new DFSNameNode();
        try{
            namenode.rpcService = new DFSNameNodeRPC();
            namenode.rpcService.initialize();
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed to create the rpcService serve!");
        }
        namenode.printHello();
        namenode.readUserCommand();
        return ;
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
            else if(cmdArray[0].equals("shutdown")){
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
        System.out.println("    shutdown      Shutdown the server job.");
        System.out.println("    ls            List the DFS directory.");
        System.out.println("    lsblocks      List the blocks list.");
        System.out.println("    lsfbmap       List the file-blocks list.");
        System.out.println("");
    }
}
