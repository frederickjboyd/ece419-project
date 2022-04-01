package testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import app_kvECS.ECSClient;
import app_kvServer.IKVServer.CacheStrategy;
import client.KVStore;
import ecs.ECSNode;
import junit.framework.TestCase;
import shared.communication.KVMessage;

public class ReplicationTest extends TestCase {
    // KVServer
    private static final int numServers = 1;
    private static final String cacheStrategy = "FIFO";
    private static final int cacheSize = 500;
    // Number of clients (current only supports <=5)
    private static final int numClients = 1;
    private static final String CONFIG_PATH = "ecs.config";
    private static final String CACHE_STRATEGY = CacheStrategy.FIFO.toString();
    private static final int CACHE_SIZE = 50;
    private static final String PUT_KEY = "hello";
    private static final String PUT_VALUE = "world";
    private List<String> serverInfo = new ArrayList<String>();
    private ECSClient ecs;
    private List<KVStore> kvClientList;
    private List<ECSNode> ecsNodeList = null;

    public void setUp() {
        System.out.println("Running replication tests...");
    }

    public void setUpInternal() {
        try {
            // Read configuration file
            BufferedReader reader = new BufferedReader(new FileReader(CONFIG_PATH));
            String l;

            while ((l = reader.readLine()) != null) {
                String[] config = l.split("\\s+", 3);
                serverInfo.add(String.format("%s:%s:%s", config[0], config[1], config[2]));
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            System.out.println("Setting up ECS performance test!");
            ecs = new ECSClient(CONFIG_PATH);
            ecsNodeList = ecs.addNodes(numServers, cacheStrategy, cacheSize);
            try {
                ecs.awaitNodes(numServers, 2000);
            } catch (Exception e) {
            }
            System.out.println("Starting ECS!");
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Replication Test failed on ECSClient init: " + e);
        }
        // Pick a random available server to connect to
        String hostname = ecsNodeList.get(0).getNodeHost();
        int port = ecsNodeList.get(0).getNodePort();

        System.out.println("ECS Performance test connecting to: " + hostname + ":" + port);

        kvClientList = new ArrayList<KVStore>();

        for (int i = 0; i < numClients; i++) {
            kvClientList.add(new KVStore(hostname, port));
        }

        // Connect all KVClients
        try {
            for (int i = 0; i < numClients; i++) {
                kvClientList.get(i).connect();
            }
            System.out.println("ECS Replication Test SUCCESS: Clients connected!");
        } catch (Exception e) {
            System.err.println("ECS Replication Test FAILURE: Client Connection Failed!");
        }
    }

    public void tearDownInternal() {
        try {
            for (int i = 0; i < numClients; i++) {
                kvClientList.get(i).disconnect();
            }
            cleanUpECS();
            ecs.quit();
            System.out.println("Shutdown replication - success");
        } catch (Exception e) {
            System.err.println("Failed to shut down ECS!");
        }
    }

    @Test
    /**
     * Check that replication works with 1, 2, and 3 nodes.
     * 
     * Check that GET and DELETE operations work with replication.
     */
    public void testReplication() {
        setUpInternal();

        // Test replication with 1 node
        // Perform put
        performPut();

        // Check replication
        boolean isReplicationValid1 = checkFilesForKV(1);

        // Clean up
        performDelete();
        System.out.println("isReplicationValid1: " + isReplicationValid1);

        // Test replication with 2 nodes
        ecs.addNodes(1, CACHE_STRATEGY, CACHE_SIZE);
        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("Unable to start servers");
            e.printStackTrace();
        }
        performPut();
        ecs.awaitTime(5000); // Account for time to reconnect
        boolean isReplicationValid2 = checkFilesForKV(2);
        performDelete();

        System.out.println("isReplicationValid2: " + isReplicationValid2);

        // Test replication with 3 nodes
        ecs.addNodes(1, CACHE_STRATEGY, CACHE_SIZE);
        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("Unable to start servers");
            e.printStackTrace();
        }
        performPut();
        ecs.awaitTime(2000); // Account for time to reconnect
        boolean isReplicationValid3 = checkFilesForKV(3);

        System.out.println("isReplicationValid3: " + isReplicationValid3);

        // Test GET
        KVMessage msg = null;
        try {
            msg = kvClientList.get(0).get(PUT_KEY);
        } catch (Exception e) {
            System.out.println("Unable to perform GET");
            e.printStackTrace();
        }
        String msgValue = msg.getValue();
        boolean isGetValid = msgValue.equals(PUT_VALUE);

        System.out.println("isGetValid: " + isGetValid);

        // Test DELETE
        performDelete();
        boolean isDeleteValid = !checkFilesForKV(3);

        System.out.println("isDeleteValid: " + isDeleteValid);

        assertTrue(isReplicationValid1 == true && isReplicationValid2 == true && isReplicationValid3 == true
                && isGetValid == true && isDeleteValid == true);

        tearDownInternal();
    }

    /**
     * Put KV pair.
     */
    private void performPut() {
        try {
            kvClientList.get(0).put(PUT_KEY, PUT_VALUE);
        } catch (Exception e) {
            System.out.println("Unable to perform PUT");
            e.printStackTrace();
        }
    }

    /**
     * Delete KV pair.
     */
    private void performDelete() {
        try {
            kvClientList.get(0).put(PUT_KEY, "");
        } catch (Exception e) {
            System.out.println("Unable to perform DELETE");
            e.printStackTrace();
        }
    }

    /**
     * Loop over a given number of server database files. Check if a KV pair is in
     * all of them.
     * 
     * @param numFiles
     * @return
     */
    private boolean checkFilesForKV(int numFiles) {
        // Check replication
        boolean isKVInAllFiles = false;

        for (int i = 0; i < numFiles; i++) {
            String info = serverInfo.get(i);
            String[] infoArray = info.split(":");
            String host = infoArray[1];
            int port = Integer.valueOf(infoArray[2]);
            // host = currNode.getNodeHost();
            // port = currNode.getNodePort();
            String databasePath = constructServerDataPath(host, port);
            isKVInAllFiles = checkFileForKV(databasePath, PUT_KEY, PUT_VALUE);

            if (!isKVInAllFiles) {
                break;
            }
        }

        return isKVInAllFiles;
    }

    /**
     * Delete all log and database files.
     */
    private void cleanUpECS() {
        ecs.cleanLogs();
        ecs.cleanData();
    }

    private String constructServerDataPath(String host, int port) {
        return String.format("./data/%s:%d.database.properties", host, port);
    }

    /**
     * Search through a database file for a given KV pair.
     * 
     * @param file  Database file for a server
     * @param key   Key to search for
     * @param value Value to search for
     * @return True if a matching KV pair was found, false otherwise
     */
    private boolean checkFileForKV(String file, String key, String value) {
        boolean isKVInFile = false;
        System.out.println("Reading file: " + file);
        System.out.println("key: " + key);
        System.out.println("value: " + value);

        try {
            // Read database file
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String l;
            boolean isHeaderRead = false;

            while ((l = reader.readLine()) != null) {
                // Ignore header with timestamp
                if (!isHeaderRead) {
                    isHeaderRead = true;
                    continue;
                }

                String[] KV = l.split("=", 2);
                String keyFile = KV[0];
                String valueFile = KV[1];
                System.out.println("keyFile: " + key);
                System.out.println("valueFile: " + value);

                if (keyFile.equals(key) && valueFile.equals(value)) {
                    System.out.println("FOUND MATCHING KV PAIR");
                    isKVInFile = true;
                    break;
                }
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return isKVInFile;
    }
}
