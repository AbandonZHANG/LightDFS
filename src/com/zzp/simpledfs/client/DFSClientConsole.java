package com.zzp.simpledfs.client;

import com.zzp.simpledfs.common.DFSINode;
import com.zzp.simpledfs.common.NoEnoughSpaceException;
import com.zzp.simpledfs.common.UserNotFoundException;

import java.io.FileNotFoundException;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.*;

public class DFSClientConsole {
    DFSClient dfsClient;
    String loginUserName = new String();
    final Stack<String> dir = new Stack<String>();  // 当前client所在DFS目录，初始为根目录用户名

    public static void main(String[] args){
        DFSClientConsole client = new DFSClientConsole();
        try{
            client.dfsClient = DFSClient.getInstance();
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
            return;
        }
        client.printHello();
        client.userLogin();
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
        System.out.println("");
    }
    public void userLogin(){
        System.out.print("[Login] User name( input '#' to regiter ):");
        Scanner in = new Scanner(System.in);
        String tmpInput = in.nextLine();
        if(tmpInput.equals("#")){
            registerUser();
            userLogin();
        }
        else{
            String userName = tmpInput;
            //String password = new String(System.console().readPassword("[Login] Password:"));
            System.out.print("[Login] Password:");
            String password = in.next();
            try{
                if(dfsClient.login(userName, password)){
                    loginUserName = userName;
                    return;
                }
                else{
                    System.out.println("[ERROR!] User name or password wrong!");
                    userLogin();
                }
            }
            catch (RemoteException e){
                System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
            }
        }
    }
    public void readUserCommand(){
        String userCmd;
        Scanner in = new Scanner(System.in);
        System.out.print("$["+loginUserName+":"+getCurrentPath()+"]");

        userCmd = in.nextLine();
        String[] cmdArray = userCmd.split(" ");
        if(cmdArray[0].equals("help")){
            printHelp();
        }
        else if(cmdArray[0].equals("ls")){
            listDirectory();
        }
        else if(cmdArray[0].equals("cd")){
            if(cmdArray.length < 2){
                System.out.println("Wrong command format!");
            }
            else if(cmdArray[1].equals(".."))
                dir.pop();
            else{
                try{
                    if(dfsClient.checkdir(getAbsolutelyDFSLocalPath(cmdArray[1]))){
                        changeCurrentPath(cmdArray[1]);
                    }
                    else{
                        System.out.println("The DFS doesn't have this directory!");
                    }
                }
                catch (Exception e){
                    System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
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
                uploadFile(cmdArray[1], getAbsolutelyDFSLocalPath(cmdArray[2]));
            }
        }
        else if(cmdArray[0].equals("download")){
            if(cmdArray.length != 3){
                System.out.println("Wrong command format!");
            }
            else{
                downloadFile(getAbsolutelyDFSLocalPath(cmdArray[1]), cmdArray[2]);
            }
        }
        else if(cmdArray[0].equals("remove")){
            if(cmdArray.length != 2){
                System.out.println("Wrong command format!");
            }
            else{
                removeFile(getAbsolutelyDFSLocalPath(cmdArray[1]));
            }
        }
        else if(cmdArray[0].equals("renamefile")){
            if(cmdArray.length != 3){
                System.out.println("Wrong command format!");
            }
            else{
                //dfsClient.renameFile(getAbsolutelyDFSLocalPath(cmdArray[1]), getAbsolutelyDFSLocalPath(cmdArray[2]));
            }
        }
        else if(cmdArray[0].equals("removefile")){
            if(cmdArray.length != 2){
                System.out.println("Wrong command format!");
            }
            else{
                //dfsClient.removeFile(getAbsolutelyDFSLocalPath(cmdArray[1]));
            }
        }
        else if(cmdArray[0].equals("quit")){
            dfsClient.logout();
            return;
        }
        else{
            System.out.println("[ERROR!] Command Not Found!");
            printHelp();
        }
        readUserCommand();
    }
    public void registerUser(){
        Scanner in = new Scanner(System.in);
        System.out.print("[Register] User name:");
        String userName = in.next();
        // System.console()只能在控制台使用，IDEA中启动不可用
        //String password = new String(System.console().readPassword("[Register] Password:"));
        System.out.print("[Login] Password:");
        String password = in.next();
        //String rePassword = new String(System.console().readPassword("[Register] Password again:"));
        System.out.print("[Login] Password again:");
        String rePassword = in.next();

        try {
            if(password.equals(rePassword)){
                boolean res = DFSClient.registerUser(userName, password);
                if(res){
                    System.out.println("[SUCCESS!] Register-Success!");
                }
                else{
                    System.out.println("[ERROR!] User has existed!");
                }
            }
            else{
                System.out.println("[ERROR!] No consistent password!");
            }
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
        }
    }
    public void listDirectory(){
        String curPath = getCurrentPath();
        try{
            DFSINode theINode = dfsClient.getINode(curPath);
            DFSINode.listDirTree(theINode, 1);
        }
        catch (FileNotFoundException e){
            System.out.println("[LINK-ERROR!] Illegal path!");
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
        }
        catch (UserNotFoundException e){
            System.out.println("[ERROR!] User not found!");
        }
    }
    public void mkdir(String dfsPath){
        try{
            dfsClient.mkdir(getAbsolutelyDFSLocalPath(dfsPath));
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("[ERROR!] The DFS directory already exists!");
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] Illegal Path!");
        }
        catch (UserNotFoundException e){
            System.out.println("[ERROR!] User not found!");
        }
    }
    public void rmdir(String dfsPath){
        try{
            dfsClient.rmdir(getAbsolutelyDFSLocalPath(dfsPath));
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] File not found!");
        }
        catch (UserNotFoundException e){
            System.out.println("[ERROR!] User not found!");
        }
    }
    public void removeFile(String dfsFilePath){
        try{
            dfsClient.removeFile(dfsFilePath);
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
        }
        catch (NotBoundException e){
            System.out.println("[LINK-ERROR!] Can't link to the DataNode!");
        }
        catch (UserNotFoundException e){
            System.out.println("[ERROR!] User not found!");
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] Illegal DFS Path!");
        }
    }
    public void uploadFile(String localFilePath, String dfsFilePath){
        try{
            dfsClient.uploadFile(localFilePath, dfsFilePath);
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
        }
        catch (NotBoundException e){
            System.out.println("[LINK-ERROR!] Can't link to the DataNode!");
        }
        catch (UserNotFoundException e){
            System.out.println("[ERROR!] User not found!");
        }
        catch (NoEnoughSpaceException e){
            System.out.println("[ERROR!] User space is not enough!");
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] Illegal Path!");
        }
        catch (FileAlreadyExistsException e){
            System.out.println("[ERROR!] The DFS file name already exists!");
        }
    }
    public void downloadFile(String dfsFilePath, String localFilePath){
        try{
            dfsClient.downloadFile(dfsFilePath, localFilePath);
        }
        catch (RemoteException e){
            System.out.println("[LINK-ERROR!] Can't link to the NameNode!");
        }
        catch (NotBoundException e){
            System.out.println("[LINK-ERROR!] Can't link to the DataNode!");
        }
        catch (UserNotFoundException e){
            System.out.println("[ERROR!] User not found!");
        }
        catch (FileNotFoundException e){
            System.out.println("[ERROR!] File not found!");
        }
    }
    public void changeCurrentPath(String path){
        String[] paths = path.split("\\\\");
        if(paths[0].equals("")){
            // 绝对路径
            dir.clear();
        }
        for(String eachpath:paths){
            if(eachpath.equals(""))
                continue;
            else
                dir.push(eachpath);
        }
    }
    /**
     * 将当前目录各级dir转化为String
     * @return 返回当前目录绝对路径：\dir1\dir2\...\
     */
    public String getCurrentPath(){
        String res = new String("\\");
        for(String eachpath:dir){
            res += eachpath+"\\";
        }
        return res;
    }
    /**
     * 将参数的路径转换为绝对路径
     * @param pathName  绝对路径: \dir1\dir2\...\, 相对路径: dir1\dir2\...\
     * @return 返回绝对路径：\dir1\dir2\...\
     */
    public String getAbsolutelyDFSLocalPath(String pathName){
        String res = new String();
        String[] paths = pathName.split("\\\\");
        for(int i = 0; i < paths.length; i ++){
            if(i == 0){
                // 如果是绝对路径
                if(!paths[i].equals("")){
                    res += getCurrentPath();
                }
                else {
                    res += "\\";
                    continue;
                }
            }
            res += paths[i] + "\\";
        }
        return res;
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
