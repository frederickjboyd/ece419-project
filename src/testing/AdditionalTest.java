package testing;

import org.junit.Test;
import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import java.util.List;
import ecs.ECSNode;

import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import app_kvECS.ECSClient;

public class AdditionalTest extends TestCase {
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
    private KVStore kvClient1;
    private KVStore kvClient2;


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
        kvClient1= new KVStore(testHost, testPort);
        kvClient2= new KVStore(testHost, testPort);


        try {
            kvClient.connect();
            kvClient1.connect();
            kvClient2.connect();
        } catch (Exception e) {
            ex = e;
        }
        System.out.println("test connecting to: " + testHost + ":" + testPort);

    }

    @Test
    public void testPutWithSpaceInMsg() {
        // ** This test mainly test if there exist space in the value the spaces will
        // all be recorded */

        // testPutWithSpaceInMsg
        String key = "foo2";
        String value = "bar     2";
        KVMessage response = null;
        KVMessage getResponse = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        try {
            getResponse = kvClient.get(key);
        } catch (Exception e2) {
            ex = e2;
        }

        System.out.println("test putWithSpaceInMsg success");
        assertTrue(ex == null && getResponse.getValue().equals("bar     2")
                && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));

        // testEmptyKeyAndMsg
        key = "";
        value = "";
        response = null;
        ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        System.out.println("test testEmptyKeyAndMsg success");
        assertTrue(response.getStatus() == StatusType.DELETE_ERROR);

        //testPutWithDiffCap
        key = "foo";
        value = "bar";
        String keyCap = "Foo";
        String valueCap = "Bar";
        response = null;
        KVMessage responseCap = null;
        ex = null;

        try {
            response = kvClient.put(key, value);
            responseCap = kvClient.put(keyCap, valueCap);
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("test putWithDiffCap success");
        assertTrue(ex == null && !response.getValue().equals(responseCap.getValue())
                && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));



        // testMultithreadingPut
        KVMessage response1 = null;
        KVMessage response2 = null;
        ex = null;
        String key1 = "newKey";
        String key2 = "newKeyTwo";


        try {
            response1 = kvClient1.put(key1, "bar");
            response2 = kvClient2.put(key2, "bartwo");
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("test multithreading_put success");
        assertTrue(ex == null &&
                (response1.getStatus() == StatusType.PUT_SUCCESS) &&
                (response2.getStatus() == StatusType.PUT_SUCCESS));

        //testMultithreadingGet
        try {
            response1 = kvClient1.get(key1);
            response2 = kvClient2.get(key2);
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("test multithreading_get success");
        assertTrue(ex == null && (response1.getValue().equals("bar")) && (response2.getValue().equals("bartwo")));

        //testMultiThreadingCombined
        KVMessage responsePut = null;
        KVMessage responseUpdate = null;
        KVMessage responseGet = null;
        KVMessage responseDelete = null;
        KVMessage responseGet2 = null;
        ex = null;

        try {
            // Put
            responsePut = kvClient1.put("testCombined", "hello");
            // Put update
            responseUpdate = kvClient2.put("testCombined", "hellotwo");
            // Get existing value
            responseGet = kvClient1.get("testCombined");
            // Delete
            responseDelete = kvClient2.put("testCombined", "");
            // Get empty
            responseGet2 = kvClient1.get("testCombined");
            } catch (Exception e) {
                ex = e;
            }

        System.out.println("testMultithreadingCombined success");
        assertTrue(ex == null && 
        (responsePut.getStatus() == StatusType.PUT_SUCCESS) &&
        (responseUpdate.getStatus() == StatusType.PUT_UPDATE) &&
        (responseGet.getValue().equals("hellotwo")) && 
        (responseDelete.getStatus() == StatusType.DELETE_SUCCESS) &&
        (responseGet2.getStatus() == StatusType.GET_ERROR));
        System.out.println("All additional  test SUCCESS");
        
    }

}
