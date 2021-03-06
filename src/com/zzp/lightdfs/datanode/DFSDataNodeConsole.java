package com.zzp.lightdfs.datanode;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

public class DFSDataNodeConsole {
    DFSDataNode dataNodeRPC;

    public static void main(String[] args){
        DFSDataNodeConsole datanode = new DFSDataNodeConsole();
        String hostIp = readHostIp();
        try {
            datanode.dataNodeRPC = DFSDataNode.getInstance(hostIp);
            datanode.dataNodeRPC.initialize();
        }
        catch (Exception e){
            System.out.println("[ERROR!] Failed to create the RPC serve!");
        }
        datanode.printHello();
        datanode.dataNodeRPC.run();

        Scanner in = new Scanner(System.in);
        while(in.hasNext()){
            String comm = in.next();
            if(comm.equals("exit")){
                datanode.dataNodeRPC.close();
                System.exit(0);
            }
        }
    }
    public void printHello(){
        System.out.println("***************************************************************************");
        System.out.println("**                                                                       **");
        System.out.println("**                Welcome to use LightDFS! (version 0.1.0)              **");
        System.out.println("**                                                                       **");
        System.out.println("**                         Author: Zhipeng Zhang                         **");
        System.out.println("**                                                                       **");
        System.out.println("**                               DataNode                                **");
        System.out.println("**                                                                       **");
        System.out.println("***************************************************************************");
        System.out.println("Data node ID: "+dataNodeRPC.datanodeID);
        System.out.println("DataNode server running...");
    }
    static public String readHostIp(){
        Properties props = new Properties();
        String hostIp=null;
        try{
            InputStream fin = new FileInputStream("datanode.xml");
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
