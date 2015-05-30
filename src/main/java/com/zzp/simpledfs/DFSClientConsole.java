package main.java.com.zzp.simpledfs;

import java.util.*;

/**
 * Created by Zhipeng Zhang on 15/05/26 0026.
 */
public class DFSClientConsole {
    DFSClient dfsClient;
    final Stack<String> dir = new Stack<String>();  // 当前client所在DFS目录，初始为根目录root

    public static void main(String[] args){
        DFSClientConsole client = new DFSClientConsole();
        client.printHello();
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
                        if(dfsClient.checkdir(getAbsolutelyPath(cmdArray[1]))){
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
                    dfsClient.mkdir(cmdArray[1]);
                }
            }
            else if(cmdArray[0].equals("rmdir")){
                if(cmdArray.length != 2){
                    System.out.println("Wrong command format!");
                }
                else{
                    dfsClient.rmdir(cmdArray[1]);
                }
            }
            else if(cmdArray[0].equals("upload")){
                if(cmdArray.length != 3){
                    System.out.println("Wrong command format!");
                }
                else{
                    dfsClient.uploadFile(cmdArray[1], cmdArray[2]);
                }
            }
            else if(cmdArray[0].equals("download")){
                if(cmdArray.length != 3){
                    System.out.println("Wrong command format!");
                }
                else{
                    dfsClient.downloadFile(cmdArray[1], cmdArray[2]);
                }
            }
            else if(cmdArray[0].equals("renamefile")){
                if(cmdArray.length != 3){
                    System.out.println("Wrong command format!");
                }
                else{
                    dfsClient.renameFile(cmdArray[1], cmdArray[2]);
                }
            }
            else if(cmdArray[0].equals("removefile")){
                if(cmdArray.length != 2){
                    System.out.println("Wrong command format!");
                }
                else{
                    dfsClient.removeFile(cmdArray[1]);
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

}
