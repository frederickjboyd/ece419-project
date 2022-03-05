package testing;

import org.junit.Test;
import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

public class AdditionalTest extends TestCase {

    // TODO add your test cases, at least 3
    private KVStore kvClient;
    private KVStore kvClient1;
    private KVStore kvClient2;

    public void setUp() {
        kvClient = new KVStore("localhost", 25001);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }

        kvClient1 = new KVStore("localhost", 50000);
        try {
            kvClient1.connect();
        } catch (Exception e) {
        }

        kvClient2 = new KVStore("localhost", 50000);
        try {
            kvClient2.connect();
        } catch (Exception e) {
        }
        System.out.println("additional test set up success");
    }

    @Test
    public void testPutWithSpaceInMsg() {
        // ** This test mainly test if there exist space in the value the spaces will
        // all be recorded */

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

    }

    @Test
    public void testEmptyKeyAndMsg() {
        // ** This test mainly test if there exist space in the value the spaces will
        // all be recorded */

        String key = "";
        String value = "";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }
        System.out.println("test testEmptyKeyAndMsg success");
        assertTrue(response.getStatus() == StatusType.DELETE_ERROR);

    }

    @Test
    public void testPutWithDiffCap() {
        // ** This test mainly test that the capitalization of key will matter and be
        // registered as individual query*/
        String key = "foo";
        String value = "bar";
        String keyCap = "Foo";
        String valueCap = "Bar";
        KVMessage response = null;
        KVMessage responseCap = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
            responseCap = kvClient.put(keyCap, valueCap);
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("test putWithDiffCap success");
        assertTrue(ex == null && !response.getValue().equals(responseCap.getValue())
                && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));

    }

    @Test
    public void testMultithreadingPut() {
        // ** This test checks that multiple clients can send PUT requests together*/
        KVMessage response1 = null;
        KVMessage response2 = null;
        Exception ex = null;

        try {
            response1 = kvClient1.put("newKey", "bar");
            response2 = kvClient2.put("newKeyTwo", "bartwo");
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("test multithreading_put success");
        assertTrue(ex == null &&
                (response1.getStatus() == StatusType.PUT_SUCCESS) &&
                (response2.getStatus() == StatusType.PUT_SUCCESS));
    }

    @Test
    public void testMultithreadingGet() {
        // ** This test checks that multiple clients can send GET requests together*/
        String key = "foo";
        KVMessage response1 = null;
        KVMessage response2 = null;
        Exception ex = null;

        try {
            response1 = kvClient1.put(key, "bar");
            response2 = kvClient2.put(key, "secondbar");
        } catch (Exception e) {
            ex = e;
        }

        try {
            response1 = kvClient1.get(key);
            response2 = kvClient2.get(key);
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("test multithreading_get success");
        assertTrue(
                ex == null && (response1.getValue().equals("secondbar")) && (response2.getValue().equals("secondbar")));
    }

    @Test
    public void testMultiThreadingCombined() {
        // ** This test checks that multiple clients can send complex sequences of puts
        // and gets together*/
        KVMessage responsePut = null;
        KVMessage responseUpdate = null;
        KVMessage responseGet = null;
        KVMessage responseDelete = null;
        KVMessage responseGet2 = null;
        Exception ex = null;

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
    }

}
