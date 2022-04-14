package testing;

import java.net.UnknownHostException;
import client.KVStore;
import app_kvECS.ECSClient;
import ecs.ECSNode;
import junit.framework.TestCase;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConnectionTest extends TestCase {
    public List<KVStore> kvClientList;
    // ECS Client
    private static final String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecs = null;
    // KVServer
    private static final int numServers = 3;
    private static final String cacheStrategy = "FIFO";
    private static final int cacheSize = 50;
    private String testHost;
    private Exception ex;
    private int testPort;

    public void setUp() {
        List<ECSNode> ecsNodeList = null;

        try {
            System.out.println("Setting up ECS performance test!");
            ecs = new ECSClient(ECSConfigPath);
            ecsNodeList = ecs.addNodes(numServers, cacheStrategy, cacheSize);
            try {
                ecs.awaitNodes(numServers, 2000);
            } catch (Exception e) {
            }
            System.out.println("Starting ECS!");
            ecs.start();
        } catch (Exception e) {
            ex = e;
            System.out.println(" Test failed on ECSClient init: " + e);
        }
        // Pick a random available server to connect to
        testHost = ecsNodeList.get(0).getNodeHost();
        testPort = ecsNodeList.get(0).getNodePort();

        System.out.println("test connecting to: " + testHost + ":" + testPort);

    }

    public void testAllConnection() {
        // Connect all KVClients
        kvClientList = new ArrayList<KVStore>();

        for (int i = 0; i < 1; i++) {
            // KVStore tempKVStore = new KVStore(hostname, port);
            kvClientList.add(new KVStore(testHost, testPort));
        }

        try {
            for (int i = 0; i < 1; i++) {
                kvClientList.get(i).connect();
            }
            System.out.println(" test connection Test SUCCESS: Clients connected!");

            for (int i = 0; i < 1; i++) {
                kvClientList.get(i).disconnect();
            }
        } catch (Exception e) {
            ex = e;
            System.err.println("test connection Test FAILURE: Client Connection Failed!");
        }
        assertNull(ex);

        // unknownhost 
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", testPort);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
        System.out.println(" test unknown host SUCCESS: Unknown host error detected!");

        // illegal port test

        ex = null;
        kvClient = new KVStore(testHost, 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex instanceof IllegalArgumentException);
        
        System.out.println(" test unknown port SUCCESS: Unknown port error detected!");
        System.out.println("All connection test SUCCESS");

    }


}
