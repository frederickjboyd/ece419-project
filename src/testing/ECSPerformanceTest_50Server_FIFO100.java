package testing;

import org.junit.Test;

import app_kvECS.*;
import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ECSPerformanceTest_50Server_FIFO100 extends TestCase {

    // private KVStore kvClient1;
    // private KVStore kvClient2;
    // private KVStore kvClient3;
    // private KVStore kvClient4;
    // private KVStore kvClient5;
    // private KVStore kvClient6;
    // private KVStore kvClient7;
    // private KVStore kvClient8;
    // private KVStore kvClient9;
    // private KVStore kvClient10;
    // private KVStore kvClient11;
    // private KVStore kvClient12;
    // private KVStore kvClient13;
    // private KVStore kvClient14;
    // private KVStore kvClient15;
    // private KVStore kvClient16;
    // private KVStore kvClient17;
    // private KVStore kvClient18;
    // private KVStore kvClient19;
    // private KVStore kvClient20;

    public List<KVStore> kvClientList;


    // ECS Client
    private static final String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecs;
    // KVServer
    private static final int numServers = 50;
    private static final String cacheStrategy = "FIFO";
    private static final int cacheSize = 100;

    public void setUp() {
        try {
            ecs = new ECSClient(ECSConfigPath);
        	ecs.addNodes(numServers, cacheStrategy, cacheSize);
        	try {
            	ecs.awaitNodes(1, 2000);
        	} catch (Exception e) {}
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        // Pick a random available server to connect to
        List<String> availableServers = ecs.getAvailableServers();
        // System.out.println(availableServers);
        String servername = availableServers.get(0);
        String[] tokens = servername.split(":");
        String hostname = tokens[1];
        int port = Integer.parseInt(tokens[2]);
        System.out.println("ECS Performance test connecting to: " + hostname + ":" + port);
        
        // // Initialize clients
        // kvClient1 = new KVStore(hostname, port);
        // kvClient2 = new KVStore(hostname, port);
        // kvClient3 = new KVStore(hostname, port);
        // kvClient4 = new KVStore(hostname, port);
        // kvClient5 = new KVStore(hostname, port);
        // kvClient6 = new KVStore(hostname, port);
        // kvClient7 = new KVStore(hostname, port);
        // kvClient8 = new KVStore(hostname, port);
        // kvClient9 = new KVStore(hostname, port);
        // kvClient10 = new KVStore(hostname, port);
        // kvClient11 = new KVStore(hostname, port);
        // kvClient12 = new KVStore(hostname, port);
        // kvClient13 = new KVStore(hostname, port);
        // kvClient14 = new KVStore(hostname, port);
        // kvClient15 = new KVStore(hostname, port);
        // kvClient16 = new KVStore(hostname, port);
        // kvClient17 = new KVStore(hostname, port);
        // kvClient18 = new KVStore(hostname, port);
        // kvClient19 = new KVStore(hostname, port);
        // kvClient20 = new KVStore(hostname, port);

        kvClientList = new ArrayList<KVStore>();
        
        for (int i = 0; i < 20; i++){
            // KVStore tempKVStore = new KVStore(hostname, port);
            kvClientList.add(new KVStore(hostname, port));
        }


        System.out.println("*** Finished ECS Setup for performance run ***");
    }

    public void tearDown() {
        // System.out.println("Shutdown performance - start");
        // kvClient.disconnect();
        System.out.println("Shutdown performance - success");
    }

    /**Helper function to create data of specific size 
    * @param msgSize Size of desired data
    */
    public static String createDataSize(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i=0; i<msgSize; i++) {
          sb.append('a');
        }
        return sb.toString();
      }




    // ************************ Following section tests 1 clients ************************
    
    /** Run performance test with 80% puts, 20% Gets */
    @Test
    public void test_80_20_1client() {      
        try {
            kvClientList.get(0).connect();
            // kvClient1.connect();
            // kvClient2.connect();
            // kvClient3.connect();
            // kvClient4.connect();
            // kvClient5.connect();
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 5; j++){
                    if (j == 4){
                        output = kvClientList.get(0).get(key).getValue();
                    }
                    else{
                        kvClientList.get(0).put(key, value);
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((4 * value.getBytes("UTF-8").length) + output.getBytes("UTF-8").length);
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 5);

        System.out.println("Throughput (80put/20get 5 Servers/FIFO 50/1 client): " + throughput + " KB/s");
        System.out.println("Latency (80put/20get 5 Servers/FIFO 50/1 client)   : " + latency + " ms");

        kvClientList.get(0).disconnect();

        // kvClient1.disconnect();
        // kvClient2.disconnect();
        // kvClient3.disconnect();
        // kvClient4.disconnect();
        // kvClient5.disconnect();

        assertTrue(ex == null);
    }



    /** Run performance test with 50% puts, 50% Gets */
    @Test
    public void test_50_50_1client() {
        try {
            kvClientList.get(0).connect();
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 5; j++){
                    if (j == 0 || j == 1){
                        output = kvClientList.get(0).get(key).getValue();
                    }
                    else if (j == 2 || j == 3) {
                        kvClientList.get(0).put(key, value);
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((2 * value.getBytes("UTF-8").length) + 2*(output.getBytes("UTF-8").length));
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 4);

        System.out.println("Throughput (50/50 5 Servers/FIFO 50/1 client): " + throughput + " KB/s");
        System.out.println("Latency (50/50 5 Servers/FIFO 50/1 client)   : " + latency + " ms");

        kvClientList.get(0).disconnect();

        assertTrue(ex == null);
    }


    /** Run performance test with 20% puts, 80% Gets */
    @Test
    public void test_20_80_1client() {
        try {
            kvClientList.get(0).connect();
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
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

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((1 * value.getBytes("UTF-8").length) + 4*(output.getBytes("UTF-8").length));
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 5);

        System.out.println("Throughput (20 puts/80 gets 5 Servers/FIFO 50/1 client): " + throughput + " KB/s");
        System.out.println("Latency (20 puts/80 gets 5 Servers/FIFO 50/1 client)   : " + latency + " ms");

        kvClientList.get(0).disconnect();

        assertTrue(ex == null);
    }




    // ************************ Following section tests 5 clients ************************

        /** Run performance test with 80% puts, 20% Gets */
    @Test
    public void test_80_20_5client() {      
        try {
            for (int i = 0; i < 5; i++){
                kvClientList.get(i).connect();
            }
            // kvClient1.connect();
            // kvClient2.connect();
            // kvClient3.connect();
            // kvClient4.connect();
            // kvClient5.connect();
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 5; j++){
                    if (j == 4){
                        output = kvClientList.get(j).get(key).getValue();
                    }
                    else{
                        kvClientList.get(j).put(key, value);
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((4 * value.getBytes("UTF-8").length) + output.getBytes("UTF-8").length);
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 5);

        System.out.println("Throughput (80put/20get 5 Servers/FIFO 50/5 client): " + throughput + " KB/s");
        System.out.println("Latency (80put/20get 5 Servers/FIFO 50/5 client)   : " + latency + " ms");

        for (int i = 0; i < 5; i++){
            kvClientList.get(i).disconnect();
        }

        // kvClient1.disconnect();
        // kvClient2.disconnect();
        // kvClient3.disconnect();
        // kvClient4.disconnect();
        // kvClient5.disconnect();

        assertTrue(ex == null);
    }


    /** Run performance test with 50% puts, 50% Gets */
    @Test
    public void test_50_50_5client() {
        try {
            for (int i = 0; i < 5; i++){
                kvClientList.get(i).connect();
            }
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 5; j++){
                    if (j == 0 || j == 1){
                        output = kvClientList.get(j).get(key).getValue();
                    }
                    else if (j == 2 || j == 3) {
                        kvClientList.get(j).put(key, value);
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((2 * value.getBytes("UTF-8").length) + 2*(output.getBytes("UTF-8").length));
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 4);

        System.out.println("Throughput (50/50 5 Servers/FIFO 50/5 client): " + throughput + " KB/s");
        System.out.println("Latency (50/50 5 Servers/FIFO 50/5 client)   : " + latency + " ms");

        for (int i = 0; i < 5; i++){
            kvClientList.get(i).disconnect();
        }

        assertTrue(ex == null);
    }


    /** Run performance test with 20% puts, 80% Gets */
    @Test
    public void test_20_80_5client() {
        try {
            for (int i = 0; i < 5; i++){
                kvClientList.get(i).connect();
            }
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 5; j++){
                    if (j == 0){
                        kvClientList.get(j).put(key, value);
                    }
                    else{
                        output = kvClientList.get(j).get(key).getValue();
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((1 * value.getBytes("UTF-8").length) + 4*(output.getBytes("UTF-8").length));
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 5);

        System.out.println("Throughput (20 puts/80 gets 5 Servers/FIFO 50/5 client): " + throughput + " KB/s");
        System.out.println("Latency (20 puts/80 gets 5 Servers/FIFO 50/5 client)   : " + latency + " ms");


        for (int i = 0; i < 5; i++){
            kvClientList.get(i).disconnect();
        }

        assertTrue(ex == null);
    }



    // ************************ Following section tests 20 clients ************************

    /** Run performance test with 80% puts, 20% Gets */
    @Test
    public void test_80_20_20client() {      
        try {
            for (int i = 0; i < 20; i++){
                kvClientList.get(i).connect();
            }
            // kvClient1.connect();
            // kvClient2.connect();
            // kvClient3.connect();
            // kvClient4.connect();
            // kvClient5.connect();
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 20; j++){
                    if (j == 16 || j == 17 || j == 18 || j == 19){
                        output = kvClientList.get(j).get(key).getValue();
                    }
                    else{
                        kvClientList.get(j).put(key, value);
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((4 * value.getBytes("UTF-8").length) + output.getBytes("UTF-8").length);
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 5);

        System.out.println("Throughput (80put/20get 5 Servers/FIFO 50/20 client): " + throughput + " KB/s");
        System.out.println("Latency (80put/20get 5 Servers/FIFO 50/20 client)   : " + latency + " ms");

        for (int i = 0; i < 20; i++){
            kvClientList.get(i).disconnect();
        }

        // kvClient1.disconnect();
        // kvClient2.disconnect();
        // kvClient3.disconnect();
        // kvClient4.disconnect();
        // kvClient5.disconnect();

        assertTrue(ex == null);
    }



    /** Run performance test with 50% puts, 50% Gets */
    @Test
    public void test_50_50_20client() {
        try {
            for (int i = 0; i < 20; i++){
                kvClientList.get(i).connect();
            }
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 5; j++){
                    // 0, 2, 4, 6, 8, 10, 12, 14, 16, 18
                    if (j % 2 == 0){
                        output = kvClientList.get(j).get(key).getValue();
                    }
                    else{
                        kvClientList.get(j).put(key, value);
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((2 * value.getBytes("UTF-8").length) + 2*(output.getBytes("UTF-8").length));
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 4);

        System.out.println("Throughput (50/50 5 Servers/FIFO 50/20 client): " + throughput + " KB/s");
        System.out.println("Latency (50/50 5 Servers/FIFO 50/20 client)   : " + latency + " ms");

        for (int i = 0; i < 20; i++){
            kvClientList.get(i).disconnect();
        }

        assertTrue(ex == null);
    }


    /** Run performance test with 20% puts, 80% Gets */
    @Test
    public void test_20_80_20client() {
        try {
            for (int i = 0; i < 20; i++){
                kvClientList.get(i).connect();
            }
        } catch (Exception e) {
            System.err.println("ECS Performance Test Client Connection Failed!");
        }

        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 150;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                for (int j = 0; j < 20; j++){
                    if (j == 0 || j == 1 || j == 2 || j == 3){
                        kvClientList.get(j).put(key, value);
                    }
                    else{
                        output = kvClientList.get(j).get(key).getValue();
                    }
                }
            }catch (Exception e) {
                System.out.println("Failed performance test!");
                ex = e;
            }
        }

        long timeElapsed = System.currentTimeMillis() - start;
        System.out.println("Time Elapsed (ms):"+timeElapsed);

        try{
            totalBytes = loops * ((1 * value.getBytes("UTF-8").length) + 4*(output.getBytes("UTF-8").length));
        }
        catch (Exception e){
            ex = e;
        }

        // time elapsed in ms, so multiply by 1000 to get per second
        // totalbytes in bytes, so divide by 1000 to get kilobytes
        double throughput = 1000 * (totalBytes / 1000) / timeElapsed;
        double latency = timeElapsed / (loops * 5);

        System.out.println("Throughput (20 puts/80 gets 5 Servers/FIFO 50/20 client): " + throughput + " KB/s");
        System.out.println("Latency (20 puts/80 gets 5 Servers/FIFO 50/20 client)   : " + latency + " ms");


        for (int i = 0; i < 20; i++){
            kvClientList.get(i).disconnect();
        }

        assertTrue(ex == null);
    }

}
