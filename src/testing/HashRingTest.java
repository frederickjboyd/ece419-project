package testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.TestCase;
import ecs.ECSNode;
import ecs.HashRing;
import ecs.ECSNode.NodeStatus;

public class HashRingTest extends TestCase {
    private HashRing hashRingClass = new HashRing();
    private static final String CONFIG_PATH = "ecs.config";
    private HashMap<String, NodeStatus> serverStatusInfo = new HashMap<String, NodeStatus>();

    public void setUp() {
        try {
            // Read configuration file
            BufferedReader reader = new BufferedReader(new FileReader(CONFIG_PATH));
            String l;

            while ((l = reader.readLine()) != null) {
                String[] config = l.split("\\s+", 3);
                serverStatusInfo.put(String.format("%s:%s:%s", config[0], config[1], config[2]), NodeStatus.OFFLINE);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Put server info in a list
        Set<String> serverInfoSet = serverStatusInfo.keySet();
        List<String> serverInfoList = new ArrayList<String>(serverInfoSet);

        hashRingClass.initHashRing(serverInfoList, CacheStrategy.None, 0);
    }

    @Test
    /**
     * Check that size of hash ring matches number of possible servers.
     */
    public void testHashRingSize() {
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();
        assertTrue(hashRing.size() == serverStatusInfo.size());
    }

    @Test
    public void testHashRingOrder() {
        boolean orderValid = true;
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();

        for (BigInteger currNodeID : hashRing.keySet()) {
            ECSNode currNode = hashRing.get(currNodeID);
        }
    }
}
