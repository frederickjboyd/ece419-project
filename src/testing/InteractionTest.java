package testing;

import org.junit.Test;
import java.util.List;
import ecs.ECSNode;

import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import app_kvECS.ECSClient;

public class InteractionTest extends TestCase {
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
    private KVStore kvClient;

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

        kvClient = new KVStore(testHost, testPort);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        System.out.println("test connecting to: " + testHost + ":" + testPort);

    }

    @Test
    public void testAllInteraction() {
        // test putting

        String key = "foo2";
        String value = "bar2";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null&& (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));
        System.out.println("test put success");

        kvClient.disconnect();
        key = "foo";
        value = "bar";
        ex = null;

        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(ex);
        System.out.println("test disconnected then put error identification success");

        // test update
        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        key = "foo2";
        value = "updatedValue";
        response = null;
        ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
                && response.getValue().equals("updatedValue"));
        System.out.println("test put update success");

        // test delete
        key = "foo2";
        value = "";
        response = null;
        ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
        System.out.println("test put delete success");

        // test get
        key = "gett";
        value = "test";
        response = null;
        ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getValue().equals(value));
        System.out.println("test get success");

        // test get unstored value
        key = "notstored";
        response = null;
        ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
        System.out.println("test get unstored success");
        System.out.println("All interacation test SUCCESS");

    
    }

}
