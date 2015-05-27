package main.java.com.zzp.simpledfs;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSFileBlockMapping implements Serializable {
    String filePath;    // �ļ���DFSĿ¼�µľ���·��
    boolean ifLittleFile;   // �Ƿ���С�ļ���С�ļ��ϲ������ļ��и�
    ArrayList<String> blocks;   // ���ļ����ݿ��б�, ��˳��
    long fileSize;      // �ļ���С,���2^63-1 byte��С
    int offset;         // С�ļ��������ݿ�ƫ�Ƶ�ַ
}
