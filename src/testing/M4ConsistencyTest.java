package testing;

import org.junit.Test;

import app_kvECS.*;
import ecs.ECSNode;
import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.nio.charset.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class M4ConsistencyTest extends TestCase {
    public List<KVStore> kvClientList;

    // ECS Client
    private static final String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecs;
    // KVServer
    private static final int numServers = 3;
    private static final String cacheStrategy = "FIFO";
    private static final int cacheSize = 50;

    public void setUp(){
        System.out.println("Begin consistency test (single client).....");
    }

    public void tearDown(){
        System.out.println("End consistency test (single client).....");
    }

    public void setUpInternal() {
        List<ECSNode> ecsNodeList = null;
        try {
            System.out.println("Setting up ECS consistency test!");
            ecs = new ECSClient(ECSConfigPath);
            ecsNodeList = ecs.addNodes(numServers, cacheStrategy, cacheSize);
            try {
                ecs.awaitNodes(numServers, 2000);
            } catch (Exception e) {
            }
            System.out.println("Starting ECS!");
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS consistency Test failed on ECSClient init: " + e);
        }
        // Pick a random available server to connect to
        String hostname = ecsNodeList.get(0).getNodeHost();
        int port = ecsNodeList.get(0).getNodePort();

        System.out.println("ECS consistency test connecting to: " + hostname + ":" + port);

        kvClientList = new ArrayList<KVStore>();

        for (int i = 0; i < 1; i++) {
            // KVStore tempKVStore = new KVStore(hostname, port);
            kvClientList.add(new KVStore(hostname, port));
        }

        // Connect all KVClients
        try {
            for (int i = 0; i < 1; i++) {
                kvClientList.get(i).connect();
            }
            System.out.println("ECS consistency Test SUCCESS: Clients connected!");
        } catch (Exception e) {
            System.err.println("ECS consistency Test FAILURE: Client Connection Failed!");
        }

        System.out.println("*** Finished ECS Setup for consistency run ***");
    }

    public void tearDownInternal() {
        System.out.println("Shutdown consistency test - start");
        // kvClient.disconnect();
        try {
            for (int i = 0; i < 1; i++) {
                kvClientList.get(i).disconnect();
            }
            ecs.cleanData();
            ecs.cleanLogs();
            ecs.quit();
            System.out.println("Shutdown consistency test - success");
        } catch (Exception e) {
            System.err.println("Failed to shut down ECS!");
        }
    }

    
    /**
     * Helper for waiting
     */
    public void waitTime(){
        // Wait before sending next
        CountDownLatch latch = new CountDownLatch(1000);
        try {
            latch.await(1000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
        }
    }

    /**
     * Helper function to create data of specific size
     * 
     * @param msgSize Size of desired data
     */
    public static String createDataSize(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append(generateRandomString(1));
        }
        return sb.toString();
    }

    /**
     * Generate a random string
     * 
     * @param size size of desired random string
     */
    protected static String generateRandomString(int size) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < size) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }

    // ************************ Following section tests 1 clients
    // ************************

    @Test
    public void test_consistency() {
        setUpInternal();

        String key = null;
        KVMessage response = null;
        Exception ex = null;
        String output = "";
        double totalBytes = 0;

        KVMessage response1 = null;
        KVMessage response2 = null;
        KVMessage response3 = null;
        KVMessage response4 = null;

        try {
            kvClientList.get(0).put("a", "1");
            kvClientList.get(0).put("b", "2");
            kvClientList.get(0).put("c", "3");
            kvClientList.get(0).put("d", "4");
        } catch (Exception e) {
            ex = e;
        }
        waitTime();
        try {
            response1 = kvClientList.get(0).get("a");
            response2 = kvClientList.get(0).get("a");
            response3 = kvClientList.get(0).get("a");
            response4 = kvClientList.get(0).get("a");
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);

        assertEquals(KVMessage.StatusType.GET_SUCCESS, response1.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response2.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response3.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response4.getStatus());

        System.out.println("\n**** RUNNING TEST: Multiple PUT, single GET on different servers:");

        assertEquals("1", response1.getValue());
        System.out.println("Expect 1, Got: " + response1.getValue());
        assertEquals("1", response2.getValue());
        System.out.println("Expect 1, Got: " + response2.getValue());
        assertEquals("1", response3.getValue());
        System.out.println("Expect 1, Got: " + response3.getValue());
        assertEquals("1", response4.getValue());
        System.out.println("Expect 1, Got: " + response4.getValue());


        // --------------------------------------------------------------------------------- //

        response1 = null;
        response2 = null;
        response3 = null;
        response4 = null;

        try {
            kvClientList.get(0).put("a", "1");
            kvClientList.get(0).put("b", "2");
            kvClientList.get(0).put("c", "3");
            kvClientList.get(0).put("d", "4");
        } catch (Exception e) {
            ex = e;
        }
        waitTime();
        try {
            response1 = kvClientList.get(0).get("a");
            response2 = kvClientList.get(0).get("b");
            response3 = kvClientList.get(0).get("c");
            response4 = kvClientList.get(0).get("d");
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);

        assertEquals(KVMessage.StatusType.GET_SUCCESS, response1.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response2.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response3.getStatus());
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response4.getStatus());

        System.out.println("\n**** RUNNING TEST: Multiple PUT, multiple GET on different servers:");

        assertEquals("1", response1.getValue());
        System.out.println("Expect 1, Got: " + response1.getValue());
        assertEquals("2", response2.getValue());
        System.out.println("Expect 2, Got: " + response2.getValue());
        assertEquals("3", response3.getValue());
        System.out.println("Expect 3, Got: " + response3.getValue());
        assertEquals("4", response4.getValue());
        System.out.println("Expect 4, Got: " + response4.getValue());


        // --------------------------------------------------------------------------------- //


        response1 = null;
        response2 = null;
        response3 = null;
        response4 = null;

        try {
            kvClientList.get(0).put("a", "");
            kvClientList.get(0).put("b", "");
            kvClientList.get(0).put("c", "");
            kvClientList.get(0).put("d", "");
        } catch (Exception e) {
            ex = e;
        }
        waitTime();
        try {
            response1 = kvClientList.get(0).get("a");
            response2 = kvClientList.get(0).get("b");
            response3 = kvClientList.get(0).get("c");
            response4 = kvClientList.get(0).get("d");
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(response1);
        assertNotNull(response2);
        assertNotNull(response3);
        assertNotNull(response4);

        System.out.println("\n**** RUNNING TEST: Multiple DELETE, multiple GET on different servers:");

        assertEquals(KVMessage.StatusType.GET_ERROR, response1.getStatus());
        System.out.println("GET a received GET ERROR");
        assertEquals(KVMessage.StatusType.GET_ERROR, response2.getStatus());
        System.out.println("GET b received GET ERROR");
        assertEquals(KVMessage.StatusType.GET_ERROR, response3.getStatus());
        System.out.println("GET c received GET ERROR");
        assertEquals(KVMessage.StatusType.GET_ERROR, response4.getStatus());
        System.out.println("GET d received GET ERROR");

        // ecs.cleanData();
        // ecs.cleanLogs();    

        assertTrue(ex == null);

        System.out.println("\n************ \nAll Consistency Tests Succesful.\n************");

        tearDownInternal();
    }
}