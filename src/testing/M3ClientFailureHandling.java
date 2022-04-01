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

public class M3ClientFailureHandling extends TestCase {
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
        kvClientList = new ArrayList<KVStore>();

        for (int i = 0; i < 3; i++) {
            testHost = ecsNodeList.get(i).getNodeHost();
            testPort = ecsNodeList.get(i).getNodePort();
            // KVStore tempKVStore = new KVStore(hostname, port);
            kvClientList.add(new KVStore(testHost, testPort));
            try {
                kvClientList.get(i).connect();
            } catch (Exception e) {
                ex = e;
                System.err.println("test connection Test FAILURE: Client Connection Failed!");
            }
            System.out.println("test connecting to: " + testHost + ":" + testPort);
        }

    }

    public void testFailureHandlingSuccess() {
        // Connect all KVClients
        int i;
    }
}
