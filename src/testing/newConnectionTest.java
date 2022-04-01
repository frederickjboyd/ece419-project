package testing;

import java.util.List;
import app_kvECS.ECSClient;
import client.KVStore;

import org.junit.Test;

import junit.framework.TestCase;
import ecs.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

public class newConnectionTest extends TestCase {
     
    public List<KVStore> kvClientList = new ArrayList<KVStore>();
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
    List<ECSNode> ecsNodeList = null;

    public void setUp() {

        try {
            System.out.println("Setting newconnection test!");
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
        

    }

    public void testM2() {
        // test 1 node connection
        for (int i = 0; i < 3; i++) {
            testHost = ecsNodeList.get(i).getNodeHost();
            testPort = ecsNodeList.get(i).getNodePort();
            System.out.println(testHost);
            System.out.println(testPort);

            // KVStore tempKVStore = new KVStore(hostname, port);
            kvClientList.add(new KVStore(testHost, testPort));
        }

        try {
            kvClientList.get(0).connect();
            System.out.println(" test connection Test SUCCESS: Clients connected!");
            kvClientList.get(0).disconnect();
        } catch (Exception e) {
            ex = e;
            System.err.println("test connection Test FAILURE: Client Connection Failed!");
        }
        System.out.println(" test single connection Test SUCCESS: Clients connected!");

        // test more node connection
        for (int i = 0; i < 3; i++) {
            testHost = ecsNodeList.get(i).getNodeHost();
            testPort = ecsNodeList.get(i).getNodePort();
            // KVStore tempKVStore = new KVStore(hostname, port);
            kvClientList.add(new KVStore(testHost, testPort));
        }

        try {
            for (int i = 0; i < 3; i++) {
                kvClientList.get(i).connect();
            }
            System.out.println(" test connection Test SUCCESS: Clients connected!");
        } catch (Exception e) {
            ex = e;
            System.err.println("test connection Test FAILURE: Client Connection Failed!");
        }
        System.out.println(" test multi node connection Test SUCCESS: Clients connected!");

        // test put in different client 
        KVMessage r1 = null;
        KVMessage r2 = null;
        KVMessage r3 = null;
        Exception ex = null;
       try {
            r1 = kvClientList.get(0).put("1", "3");
            r2 = kvClientList.get(1).put("2", "2");
            r3 = kvClientList.get(2).put("3", "1");
        } catch (Exception e) {
            ex=e;
        }

        assertEquals(KVMessage.StatusType.PUT_SUCCESS, r1.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, r2.getStatus());
        assertEquals(KVMessage.StatusType.PUT_SUCCESS, r3.getStatus());
        assertNull(ex);
        assertNotNull(r1);
        assertNotNull(r2);
        assertNotNull(r3);

        System.out.println("different server client put Test SUCCESS");

        // test get
        try {
            r1 = kvClientList.get(0).get("1");
            r2 = kvClientList.get(1).get("2");
            r3 = kvClientList.get(2).get("3");
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
        System.out.println("different server client get Test SUCCESS");

        // test delete
        try {
            r1 = kvClientList.get(0).put("1", "");
            r2 = kvClientList.get(1).put("2", "");
            r3 = kvClientList.get(2).put("3", "");
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
        System.out.println("different server client delete Test SUCCESS");

        System.out.println("All new connection test SUCCESS");

    }

}