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

public class M3PerformanceTest extends TestCase {
    public List<KVStore> kvClientList;

    // ECS Client
    private static final String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecs;
    // KVServer
    private static final int numServers = 20;
    private static final String cacheStrategy = "FIFO";
    private static final int cacheSize = 50;

    public void setUp(){
        System.out.println("Begin performance test (single client).....");
    }

    public void tearDown(){
        System.out.println("End performance test (single client).....");
    }

    public void setUpInternal() {
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
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        // Pick a random available server to connect to
        String hostname = ecsNodeList.get(0).getNodeHost();
        int port = ecsNodeList.get(0).getNodePort();

        System.out.println("ECS Performance test connecting to: " + hostname + ":" + port);

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
            System.out.println("ECS Performance Test SUCCESS: Clients connected!");
        } catch (Exception e) {
            System.err.println("ECS Performance Test FAILURE: Client Connection Failed!");
        }

        System.out.println("*** Finished ECS Setup for performance run ***");
    }

    public void tearDownInternal() {
        System.out.println("Shutdown performance - start");
        // kvClient.disconnect();
        try {
            for (int i = 0; i < 1; i++) {
                kvClientList.get(i).disconnect();
            }
            ecs.cleanData();
            ecs.cleanLogs();
            ecs.quit();
            System.out.println("Shutdown performance - success");
        } catch (Exception e) {
            System.err.println("Failed to shut down ECS!");
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

    /** Run performance test with 80% puts, 20% Gets */
    @Test
    public void test_all_performance() {
        setUpInternal();

        String key = null;
        KVMessage response = null;
        Exception ex = null;
        int loops = 20; // normally 150
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            // Generate a random key each time
            // key = generateRandomString(10);
            // key = new String(new char[3]).replace("\0", "abcde");
            key = "abcde" + Integer.toString(i);
            try {
                for (int j = 0; j < 5; j++) {
                    if (j == 4) {
                        output = kvClientList.get(0).get(key).getValue();
                    } else {
                        kvClientList.get(0).put(key, value);
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):" + timeElapsed);

        try {
            totalBytes = loops * ((4 * value.getBytes("UTF-8").length) + output.getBytes("UTF-8").length);
        } catch (Exception e) {
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput1 = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency1 = timeElapsed / (loops * 5);

        // System.out.println("Throughput (80put/20get 3 Servers/FIFO 50/1 client): " + throughput + " KB/s");
        // System.out.println("Latency (80put/20get 3 Servers/FIFO 50/1 client)   : " + latency + " ms");

        ecs.cleanData();
        ecs.cleanLogs();
        // tearDownInternal();

        // kvClientList.get(0).disconnect();
        // assertTrue(ex == null);


        // setUpInternal();

        // Start benchmark
        start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            key = "abcde" + Integer.toString(i);
            try {
                for (int j = 0; j < 5; j++) {
                    if (j == 0 || j == 1) {
                        output = kvClientList.get(0).get(key).getValue();
                    } else if (j == 2 || j == 3) {
                        kvClientList.get(0).put(key, value);
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):" + timeElapsed);

        try {
            totalBytes = loops * ((2 * value.getBytes("UTF-8").length) +
                    2 * (output.getBytes("UTF-8").length));
        } catch (Exception e) {
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput2 = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency2 = timeElapsed / (loops * 4);

        // System.out.println("Throughput (50/50 3 Servers/FIFO 50/1 client): " +
        //         throughput + " KB/s");
        // System.out.println("Latency (50/50 3 Servers/FIFO 50/1 client) : " + latency
        //         + " ms");

        // kvClientList.get(0).disconnect();

        ecs.cleanData();
        ecs.cleanLogs();

        // assertTrue(ex == null);

        // tearDownInternal();

        // setUpInternal();

        // Start benchmark
        start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            key = "abcde" + Integer.toString(i);
            try {
                for (int j = 0; j < 5; j++){
                    if (j == 0){
                        kvClientList.get(0).put(key, value);
                    }
                    else{
                        output = kvClientList.get(0).get(key).getValue();
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((1 * value.getBytes("UTF-8").length) + 4*(output.getBytes("UTF-8").length));
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput3 = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency3 = timeElapsed / (loops * 5);

        // kvClientList.get(0).disconnect();

        ecs.cleanData();
        ecs.cleanLogs();

        assertTrue(ex == null);

        tearDownInternal();

        System.out.println("\n\n\n ***** FINAL RESULTS - PERFORMANCE TEST *****");

        System.out.println("Throughput (80put/20get 3 Servers/FIFO 50/1 client): " + throughput1 + " KB/s");
        System.out.println("Latency (80put/20get 3 Servers/FIFO 50/1 client)   : " + latency1 + " ms");

        System.out.println("Throughput (50put/50get 3 Servers/FIFO 50/1 client): " + throughput2 + " KB/s");
        System.out.println("Latency (50put/50get 3 Servers/FIFO 50/1 client)   : " + latency2 + " ms");

        System.out.println("Throughput (20 puts/80 gets 5 Servers/FIFO 50/1 client): " + throughput3 + " KB/s");
        System.out.println("Latency (20 puts/80 gets 5 Servers/FIFO 50/1 client) : " + latency3 + " ms");
    }




    // /** Run performance test with 50% puts, 50% Gets */
    // @Test
    // public void test_50_50_1client() {
    //     setUpInternal();

    //     String key = null;
    //     KVMessage response = null;
    //     Exception ex = null;
    //     int loops = 5;
    //     String output = "";
    //     double totalBytes = 0;

    //     // 1024 byte array
    //     String value = createDataSize(1024);

    //     // Start benchmark
    //     long start = System.currentTimeMillis();

    //     for (int i = 0; i < loops; i++) {
    //         key = "abcde" + Integer.toString(i);
    //         try {
    //             for (int j = 0; j < 5; j++) {
    //                 if (j == 0 || j == 1) {
    //                     output = kvClientList.get(0).get(key).getValue();
    //                 } else if (j == 2 || j == 3) {
    //                     kvClientList.get(0).put(key, value);
    //                 }
    //             }
    //         } catch (Exception e) {
    //             System.out.println("Failed performance test!");
    //             ex = e;
    //         }
    //     }

    //     long timeElapsed = System.currentTimeMillis() - start;
    //     System.out.println("Time Elapsed (ms):" + timeElapsed);

    //     try {
    //         totalBytes = loops * ((2 * value.getBytes("UTF-8").length) +
    //                 2 * (output.getBytes("UTF-8").length));
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     // time elapsed in ms, so multiply by 1000 to get per second
    //     // totalbytes in bytes, so divide by 1000 to get kilobytes
    //     double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
    //     double latency = timeElapsed / (loops * 4);

    //     System.out.println("Throughput (50/50 3 Servers/FIFO 50/1 client): " +
    //             throughput + " KB/s");
    //     System.out.println("Latency (50/50 3 Servers/FIFO 50/1 client) : " + latency
    //             + " ms");

    //     // kvClientList.get(0).disconnect();

    //     ecs.cleanData();
    //     ecs.cleanLogs();

    //     assertTrue(ex == null);

    //     tearDownInternal();
    // }

    // /** Run performance test with 20% puts, 80% Gets */
    // @Test
    // public void test_20_80_1client() {
    //     setUpInternal();

    //     String key = null;
    //     KVMessage response = null;
    //     Exception ex = null;
    //     int loops = 5;
    //     String output = "";
    //     double totalBytes = 0;

    //     // 1024 byte array
    //     String value = createDataSize(1024);

    //     // Start benchmark
    //     long start = System.currentTimeMillis();

    //     for (int i = 0; i < loops; i++) {
    //         key = "abcde" + Integer.toString(i);
    //         try {
    //             for (int j = 0; j < 5; j++){
    //                 if (j == 0){
    //                     kvClientList.get(0).put(key, value);
    //                 }
    //                 else{
    //                     output = kvClientList.get(0).get(key).getValue();
    //                 }
    //             }
    //         }catch (Exception e) {
    //             System.out.println("Failed performance test!");
    //             ex = e;
    //         }
    //     }

    //     long timeElapsed = System.currentTimeMillis() - start;
    //     System.out.println("Time Elapsed (ms):"+timeElapsed);

    //     try{
    //         totalBytes = loops * ((1 * value.getBytes("UTF-8").length) + 4*(output.getBytes("UTF-8").length));
    //     }
    //     catch (Exception e){
    //         ex = e;
    //     }

    //     // time elapsed in ms, so multiply by 1000 to get per second
    //     // totalbytes in bytes, so divide by 1000 to get kilobytes
    //     double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
    //     double latency = timeElapsed / (loops * 5);

    //     System.out.println("Throughput (20 puts/80 gets 5 Servers/FIFO 50/1 client): " + throughput + " KB/s");
    //     System.out.println("Latency (20 puts/80 gets 5 Servers/FIFO 50/1 client) : " + latency + " ms");

    //     // kvClientList.get(0).disconnect();

    //     ecs.cleanData();
    //     ecs.cleanLogs();

    //     assertTrue(ex == null);

    //     tearDownInternal();
    // }
}