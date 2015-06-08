import com.zzp.simpledfs.namenode.DFSConsistentHashing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TestConsistentHashing {
    static HashMap<String, Integer> ct;
    public static void main(String[] args){
        DFSConsistentHashing testConsistentHash = new DFSConsistentHashing();
        ct = new HashMap<>();
        int nodeNum = 20;
        for(int i = 1; i <= nodeNum; i ++){
            testConsistentHash.addNode("datanode"+i);
            ct.put("datanode"+i, 0);
        }
        File oFile = new File("result.txt");
        try{
            BufferedWriter bW = new BufferedWriter(new FileWriter(oFile));
            for(int i = 0; i < 100000; i ++){
                String datanode = testConsistentHash.getNode(UUID.randomUUID().toString());
                if(datanode==null)
                    break;
                ct.put(datanode, ct.get(datanode)+1);
            }
            for(Map.Entry<String, Integer> entry : ct.entrySet()){
                bW.write(entry.getValue()+"\n");
                System.out.println(entry.getValue());
            }
            bW.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
