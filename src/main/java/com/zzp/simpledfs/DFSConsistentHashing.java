package main.java.com.zzp.simpledfs;

import java.util.Comparator;
import java.util.TreeMap;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class DFSConsistentHashing<T> {
    private final TreeMap<DFSHashFunction.LongPair, T> circle = new TreeMap<DFSHashFunction.LongPair, T>(new LongPairComparator());
    private DFSHashFunction.LongPair hashcode;
    private byte virtualNodeNumber = 5;
    public class LongPairComparator implements Comparator<DFSHashFunction.LongPair> {
        @Override
        public int compare(DFSHashFunction.LongPair n1, DFSHashFunction.LongPair n2){
            long res = n1.val1 - n2.val2;
            if(res != 0){
                return (int)res;
            }
            else{
                return (int)(n1.val2 - n2.val2);
            }
        }
    }
    public void addNode(String key, T node){
        byte[] keyBytes = key.getBytes();
        // 添加虚拟节点
        for(byte i = 0; i < virtualNodeNumber; i ++){
            keyBytes[keyBytes.length-1] = (byte)((keyBytes[keyBytes.length-1] + 1) % 128);
            hashcode = DFSHashFunction.hash(keyBytes);
            circle.put(hashcode, node);
        }
    }
    public void deleteNode(String key){
        byte[] keyBytes = key.getBytes();
        // 删除虚拟节点
        for(byte i = 0; i < virtualNodeNumber; i ++){
            keyBytes[keyBytes.length-1] = (byte)((keyBytes[keyBytes.length-1] + 1) % 128);
            hashcode = DFSHashFunction.hash(keyBytes);
            circle.remove(hashcode);
        }
    }
    public T get(String key){
        hashcode = DFSHashFunction.hash(key);
        if(circle.containsKey(hashcode)){
            return circle.get(hashcode);
        }
        else{
            T node = circle.higherEntry(hashcode).getValue();
            if(node == null){
                return circle.firstEntry().getValue();
            }
            else{
                return node;
            }
        }

    }
    public int getVirtualNodeNumber() {
        return virtualNodeNumber;
    }

    public void setVirtualNodeNumber(byte virtualNodeNumber) {
        this.virtualNodeNumber = virtualNodeNumber;
    }
}
