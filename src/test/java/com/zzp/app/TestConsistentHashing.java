package test.java.com.zzp.app;

import main.java.com.zzp.app.DFSConsistentHashing;
import main.java.com.zzp.app.DFSDataNodeState;

import java.util.UUID;

/**
 * Created by Zhipeng Zhang on 15/05/25 0025.
 */
public class TestConsistentHashing {
    public static void main(String[] args){
        DFSConsistentHashing<DFSDataNodeState> hash = new DFSConsistentHashing<DFSDataNodeState>();
        DFSDataNodeState[] myState = new DFSDataNodeState[4];
        // 1
        myState[0] = new DFSDataNodeState();
        myState[0].ip = "172.30.59.21";
        myState[0].port = "5003";
        myState[0].datanodeID = "DN-" + myState[0].ip + "-" + myState[0].port;
        hash.addNode(myState[0].datanodeID, myState[0]);
        // 2
        myState[1] = new DFSDataNodeState();
        myState[1].ip = "198.30.59.22";   // 这个节点几率高
        myState[1].port = "5003";
        myState[1].datanodeID = "DN-" + myState[1].ip + "-" + myState[1].port;
        hash.addNode(myState[1].datanodeID, myState[1]);
        // 3
        myState[2] = new DFSDataNodeState();
        myState[2].ip = "128.30.59.23";
        myState[2].port = "5003";
        myState[2].datanodeID = "DN-" + myState[2].ip + "-" + myState[2].port;
        hash.addNode(myState[2].datanodeID, myState[2]);
        // 4
        myState[3] = new DFSDataNodeState();
        myState[3].ip = "135.30.59.24";
        myState[3].port = "5003";
        myState[3].datanodeID = "DN-" + myState[3].ip + "-" + myState[3].port;
        hash.addNode(myState[3].datanodeID, myState[3]);

        String blockname = UUID.randomUUID().toString();
        System.out.println(blockname+",    "+hash.get(blockname).ip);

        blockname = UUID.randomUUID().toString();
        System.out.println(blockname+",    "+hash.get(blockname).ip);

        blockname = UUID.randomUUID().toString();
        System.out.println(blockname+",    "+hash.get(blockname).ip);

        blockname = UUID.randomUUID().toString();
        System.out.println(blockname+",    "+hash.get(blockname).ip);
    }
}
