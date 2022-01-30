package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

public class PerformanceTest extends TestCase {

    private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50000);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
        System.out.println("*** Starting Performance run ***");

    }

    public void tearDown() {
        System.out.println("Shutdown performance - start");

        kvClient.disconnect();
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
    
    /** Run performance test with 80% puts, 20% Gets */
    @Test
    public void test_80_20() {
        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 500;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                kvClient.put(key, value);
                kvClient.put(key, value);
                kvClient.put(key, value);
                kvClient.put(key, value);
                output = kvClient.get(key).getValue();
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

        System.out.println("Throughput (80put/20get): " + throughput + " KB/s");
        System.out.println("Latency (80put/20get)   : " + latency + " ms");
        assertTrue(ex == null);
    }



    /** Run performance test with 50% puts, 50% Gets */
    @Test
    public void test_50_50() {
        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 500;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                kvClient.put(key, value);
                kvClient.put(key, value);
 
                output = kvClient.get(key).getValue();
                output = kvClient.get(key).getValue();
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

        System.out.println("Throughput (50/50): " + throughput + " KB/s");
        System.out.println("Latency (50/50)   : " + latency + " ms");
        assertTrue(ex == null);
    }


    /** Run performance test with 20% puts, 80% Gets */
    @Test
    public void test_20_80() {
        String key = "test";
        KVMessage response = null;
        Exception ex = null;
        int loops = 500;
        String output = "";
        double totalBytes = 0;

        // 1024 byte array
        String value = createDataSize(1024);

        // Start benchmark
        long start = System.currentTimeMillis();

        for (int i = 0; i < loops; i++) {
            try {
                kvClient.put(key, value);

                output = kvClient.get(key).getValue();
                output = kvClient.get(key).getValue();
                output = kvClient.get(key).getValue();
                output = kvClient.get(key).getValue();
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

        System.out.println("Throughput (20 puts/80 gets): " + throughput + " KB/s");
        System.out.println("Latency (20 puts/80 gets)   : " + latency + " ms");
        assertTrue(ex == null);
    }

}
