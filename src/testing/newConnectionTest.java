package testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import app_kvECS.ECSClient;
import client.KVStore;

import org.junit.Test;

import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.TestCase;
import ecs.*;

public class newConnectionTest extends TestCase {
    private HashRing hashRingClass = new HashRing();
    private static final String CONFIG_PATH = "ecs.config";
    // private HashMap<String, NodeStatus> serverStatusInfo = new HashMap<String, NodeStatus>();
    private int numServersToAdd;
    private KVStore kvClient;


    public void setUp() {

        Exception ex = null;
        // CacheStrategy cacheStrategy = CacheStrategy.FIFO;
        String cacheStrategy = "FIFO";

        int cacheSize = 500;
        String host;
        int port;
        int numServers = 5;
        String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";

        List<ECSNode> nodesAdded;
        ECSClient ecs;

        // List<ECSNode> nodesAdded = new ArrayList<ECSNode>()
        ecs = new ECSClient(ECSConfigPath);
        nodesAdded = ecs.addNodes(numServers, cacheStrategy, cacheSize);
        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        
        host = nodesAdded.get(0).getNodeHost();
        port = nodesAdded.get(0).getNodePort();

        // kvClient = new KVStore("localhost", 50000);
        kvClient = new KVStore(host, port);
        try {
            kvClient.connect();
        } catch (Exception e) {
            System.out.println("not working");
            
        }
    }

    public void testMoreNodeAndConnection() {
        Exception ex = null;
        // CacheStrategy cacheStrategy = CacheStrategy.FIFO;
        String cacheStrategy = "FIFO";

        int cacheSize = 500;
        String host;
        int port;
        int numServers = 5;
        String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";

        List<ECSNode> nodesAdded;
        ECSClient ecs;

        // List<ECSNode> nodesAdded = new ArrayList<ECSNode>()
        ecs = new ECSClient(ECSConfigPath);
        nodesAdded = ecs.addNodes(numServers, cacheStrategy, cacheSize);
        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        
        host = nodesAdded.get(0).getNodeHost();
        port = nodesAdded.get(0).getNodePort();

        // kvClient = new KVStore("localhost", 50000);
        kvClient = new KVStore(host, port);
        try {
            kvClient.connect();
        } catch (Exception e) {
            System.out.println("not working");
            
        }
    }


    @Test
    /**
     * Check that size of hash ring matches number of possible servers.
     */
    public void testtwoNodesSetup() {
        String cacheStrategy = "FIFO";

        int cacheSize = 500;
        String host;
        int port;
        int numServers = 2;
        String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";

        List<ECSNode> nodesAdded;
        ECSClient ecs;
        String host2;
        int port2;
        ecs = new ECSClient(ECSConfigPath);
        
        nodesAdded = ecs.addNodes(numServers, cacheStrategy, cacheSize);
        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        host = nodesAdded.get(0).getNodeHost();
        port = nodesAdded.get(0).getNodePort();
        host2 = nodesAdded.get(1).getNodeHost();
        port2 = nodesAdded.get(1).getNodePort();
        assertTrue(host2 != host);
        
    }

    @Test
    /**
     * Check that each node's placement in the hash ring makes sense.
     */
    public void testPut() {
        KVMessage r1 = null;
        KVMessage r2 = null;
        KVMessage r3 = null;

        try {
            r1 = kvClient.put("1", "3");
            r2 = kvClient.put("2", "2");
            r3 = kvClient.put("3", "1");
        } catch (Exception e) {
            ex = e;
        }

        assertEquals(KVMessage.StatusType.PUT_SUCCESS, r1.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, r2.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, r3.getStatus());
        assertNull(ex);
        assertNotNull(r1);
        assertNotNull(r2);
        assertNotNull(r3);
    
    }
    

    @Test
    public void testGet() {
        KVMessage r1 = null;
        KVMessage r2 = null;
        KVMessage r3 = null;

        try {
            r1 = kvClient.get("1");
            r2 = kvClient.get("2");
            r3 = kvClient.get("3");
        } catch (Exception e) {
            ex = e;
        }

        assertEquals(KVMessage.StatusType.GET_SUCCESS, r1.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, r2.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, r3.getStatus());
        assertNull(ex);
        assertNotNull(r1);
        assertNotNull(r2);
        assertNotNull(r3);
    
    }

    

    @Test
    public void testDelete() {
        KVMessage r1 = null;
        KVMessage r2 = null;
        KVMessage r3 = null;

        try {
            r1 = kvClient.put("1", "");
            r2 = kvClient.put("2", "");
            r3 = kvClient.put("3", "");
        } catch (Exception e) {
            ex = e;
        }

        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, r1.getStatus());
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, r2.getStatus());
        assertEquals(KVMessage.StatusType.DELETE_SUCCESS, r3.getStatus());
        assertNull(ex);
        assertNotNull(r1);
        assertNotNull(r2);
        assertNotNull(r3);
    
    }


        
}
