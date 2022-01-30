package testing;

import org.junit.Test;
import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

public class AdditionalTest extends TestCase {

    // TODO add your test cases, at least 3
    private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
        System.out.println("additional test set up success");

    }

    @Test
    public void testPutWithSpaceInMsg() {
        //** This test mainly test if there exist space in the value the spaces will all be recorded */

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
        assertTrue(ex == null && getResponse.getValue().equals("bar     2") && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));
    
    }

    @Test
    public void testPutWithDiffCap() {
        //** This test mainly test that the capitalization of key will matter and be registered as individual query*/
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
        assertTrue(ex == null && !response.getValue().equals(responseCap.getValue()) && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));
    
    }


}
