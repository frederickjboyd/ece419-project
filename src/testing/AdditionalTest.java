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
        System.out.println("set up success");

    }

    @Test
    public void testStub() {
        assertTrue(true);
    }

    @Test
    public void putWithSpaceInMsg() {
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

        } catch (Exception e) {
            ex = e;
        }

        System.out.println("test putWithSpaceInMsg success");
        assertTrue(ex == null && response.getValue().equals(value) && response.getStatus() == StatusType.PUT_SUCCESS);
    
    }

}
