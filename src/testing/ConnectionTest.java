package testing;

import java.net.UnknownHostException;

import client.KVStore;
import app_kvECS.*;
import app_kvServer.IKVServer;
import app_kvServer.IKVServer.CacheStrategy;
import ecs.ECSNode;
import junit.framework.TestCase;
import java.util.List;


public class ConnectionTest extends TestCase {

    public void testConnectionSuccess() {
        
        Exception ex = null;
        // CacheStrategy cacheStrategy = CacheStrategy.FIFO;
        String cacheStrategy = "FIFO";

        int cacheSize = 500;
        String host;
        int port;
        int numServers = 5;
        String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
        // System.out.println(ECSConfigPath);
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

        // List<ECSNode> nodesAdded = addNodes(1, cacheStrategy, cacheSize);
        host = nodesAdded.get(0).getNodeHost();
        port = nodesAdded.get(0).getNodePort();

        // kvClient = new KVStore("localhost", 50000);
        KVStore kvClient = new KVStore(host, port);


        // KVStore kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertNull(ex);
    }

    public void testUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", 50000);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }

    public void testIllegalPort() {
        Exception ex = null;
        KVStore kvClient = new KVStore("localhost", 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }

}
