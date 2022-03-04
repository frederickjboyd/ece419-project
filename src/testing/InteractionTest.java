package testing;
import org.apache.log4j.Logger;

import org.junit.Test;
import java.util.List;
import ecs.ECSNode;

import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import app_kvECS.ECSClient;

public class InteractionTest extends TestCase {

    private KVStore kvClient;

    public void setUp() {
        Exception ex = null;
        // CacheStrategy cacheStrategy = CacheStrategy.FIFO;
        String cacheStrategy = "FIFO";

        int cacheSize = 500;
        String host;
        int port;
        int numServers = 5;
        String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";

        List<ECSNode> nodesAdded;
        ECSClient ecs;

        // List<ECSNode> nodesAdded = new ArrayList<ECSNode>()
        ecs = new ECSClient(ECSConfigPath);
        nodesAdded = ecs.addNodes(numServers, cacheStrategy, cacheSize);
        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        
        host = nodesAdded.get(0).getNodeHost();
        port = nodesAdded.get(0).getNodePort();

        // kvClient = new KVStore("localhost", 50000);
        kvClient = new KVStore(host, port);

        try {
            kvClient.connect();
        } catch (Exception e) {
        }
        System.out.println("set up success");

    }

    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPut() {
        String key = "foo2";
        String value = "bar2";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;

        }
        System.out.println("test put success");
        assertTrue(ex == null
                && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));
    }

    @Test
    public void testPutDisconnected() {
        kvClient.disconnect();
        String key = "foo";
        String value = "bar";
        Exception ex = null;

        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
    }

    @Test
    public void testUpdate() {
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, initialValue);
            response = kvClient.put(key, updatedValue);

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
                && response.getValue().equals(updatedValue));
    }

    @Test
    public void testDelete() {
        String key = "deleteTestValue";
        String value = "toDelete";

        KVMessage response = null;
        Exception ex = null;
        // Logger logger = Logger.getRootLogger();


        try {
            kvClient.put(key, value);
            response = kvClient.put(key, "null");

        } catch (Exception e) {
            ex = e;
            // logger.error(e.getMessage());
            // System.out.println(12342);

            System.out.println(response.getStatus().toString());
        }

        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
    }

    @Test
    public void testGet() {
        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }
}
