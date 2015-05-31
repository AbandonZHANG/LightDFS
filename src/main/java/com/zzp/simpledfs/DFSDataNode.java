package main.java.com.zzp.simpledfs;

import java.util.Scanner;

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
            System.out.println("[ERROR!] Failed to create the RPC serve!");
        }
        datanode.printHello();
        datanode.dataNodeRPC.run();
    }
    public void printHello(){
        System.out.println("***************************************************************************");
        System.out.println("**                                                                       **");
        System.out.println("**                Welcome to use SimpleDFS! (version 0.1.0)              **");
        System.out.println("**                                                                       **");
        System.out.println("**                         Author: Zhipeng Zhang                         **");
        System.out.println("**                                                                       **");
        System.out.println("**                               DataNode                                **");
        System.out.println("**                                                                       **");
        System.out.println("***************************************************************************");
        System.out.println("DataNode server running...");
    }
}
