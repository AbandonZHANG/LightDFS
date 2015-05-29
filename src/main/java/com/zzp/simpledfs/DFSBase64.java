package main.java.com.zzp.simpledfs;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;

/**
 * Created by Zhipeng Zhang on 15/05/28 0028.
 */
public class DFSBase64 {
    public static String Base64Encode(String plainText){
        byte[] plainBytes = plainText.getBytes();
        return new BASE64Encoder().encode(plainBytes);
    }
    public static String Base64Decode(String cipherText) throws IOException{
        byte[] plainBytes = new BASE64Decoder().decodeBuffer(cipherText);
        return new String(plainBytes);
    }
}
