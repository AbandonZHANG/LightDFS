package main.java.com.zzp.simpledfs;

import java.util.Scanner;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSDataNode {
    DFSDataNodeRPC dataNodeRPC;

    public static void main(String[] args){
        DFSDataNode datanode = new DFSDataNode();
        try {
            datanode.dataNodeRPC = new DFSDataNodeRPC();
            // 初始化
            datanode.dataNodeRPC.initialize();
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed to create the RMI serve!");
        }
        datanode.printHello();
        datanode.readUserCommand();
    }
    public void printHello(){
        System.out.println("***************************************************************************");
        System.out.println("**                                                                       **");
        System.out.println("**                                                                       **");
        System.out.println("**                Welcome to use SimpleDFS! (version 0.1.0)              **");
        System.out.println("**                                                                       **");
        System.out.println("**                         Author: Zhipeng Zhang                         **");
        System.out.println("**                                                                       **");
        System.out.println("**                               DataNode                                **");
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
                start();
            }
            else if(cmdArray[0].equals("shutdown")){
                close();
            }
            else if(cmdArray[0].equals("ls")){
                dataNodeRPC.listBlocks();
            }
            else if(cmdArray[0].equals("quit")){
                break;
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
        System.out.println("The SimpleDFS DataNode commands are:");
        System.out.println("    start         start the serve.");
        System.out.println("    shutdown      close the serve.");
        System.out.println("    ls            list the local blocks list.");
        System.out.println("    quit          quit the system.");
        System.out.println("");
    }
    public void start(){
        // 开启 DataNode RMI 服务
        try{
            System.out.println("[INFO] Starting block transfer rmi serve.");
            dataNodeRPC.run();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public void close(){
        // 关闭 DataNode RMI 服务
        dataNodeRPC.close();
        System.out.println("[INFO] The DataNode serve is closed!");
    }
}
