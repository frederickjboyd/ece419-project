package testing;

import java.net.UnknownHostException;

import client.KVStore;
import app_kvECS.ECSClient;
import ecs.ECSNode;
import junit.framework.TestCase;
import java.util.List;

public class ConnectionTest extends TestCase {
    private KVStore kvClient;
    private KVStore kvClient1;
    private KVStore kvClient2;
    private String cacheStrategy = "FIFO";
    private int cacheSize = 500;
    private String host;
    private int port;
    private int numServers = 5;
    private String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private List<ECSNode> nodesAdded;
    private ECSClient ecs;
    private Exception ex;

    public void setUp() {
        ecs = new ECSClient(ECSConfigPath);
        nodesAdded = ecs.addNodes(numServers, cacheStrategy, cacheSize);

        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        
        host = nodesAdded.get(0).getNodeHost();
        port = nodesAdded.get(0).getNodePort();
        
        System.out.println("connection test set up success");
    }


    public void testConnectionSuccess() {
        // kvClient = new KVStore("localhost", 50000);
        KVStore kvClient = new KVStore(host, port);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex=e;
        }

        assertNull(ex);
    }

    public void atestUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", port);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }

    public void atestIllegalPort() {
        Exception ex = null;
        KVStore kvClient = new KVStore(host, 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }

}
