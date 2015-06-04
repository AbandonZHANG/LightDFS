package com.zzp.simpledfs.datanode;

public class DFSDataNodeConsole {
    DFSDataNode dataNodeRPC;

    public static void main(String[] args){
        DFSDataNodeConsole datanode = new DFSDataNodeConsole();
        try {
            datanode.dataNodeRPC = new DFSDataNode();
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
