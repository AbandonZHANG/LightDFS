package main.java.com.zzp.simpledfs;

import java.io.Serializable;

public class DFSUser implements Serializable{
    static long initUserSpace;
    String userName;
    String base64Password;    // 经过客户端Base64加密
    //HashSet<String> userID;     // 用户设备登陆唯一识别码，推荐用UUID
    long maxSpace;       // 最大可用空间 MB
    long usedSpace;      // 已用空间 MB
    DFSUser(String _userName, String _password){
        userName = _userName;
        base64Password = _password;
        maxSpace = initUserSpace;
        usedSpace = 0;
    }
}