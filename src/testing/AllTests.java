package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ERROR);
            // new KVServer(50000, 10, "FIFO");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Test suite() {
        TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
        clientSuite.addTestSuite(ConnectionTest.class); // worked
        // clientSuite.addTestSuite(InteractionTest.class); // worked
        clientSuite.addTestSuite(MessageTest.class); // worked
        // clientSuite.addTestSuite(AdditionalTest.class); // worked
        clientSuite.addTestSuite(HashRingTest.class); // worked
        clientSuite.addTestSuite(HashRingEvenTest.class);

        // Sequential consistency test
        //clientSuite.addTestSuite(M4ConsistencyTest.class);

        // Special performance testing file
        // clientSuite.addTestSuite(M3PerformanceTest.class); // ??
        // clientSuite.addTestSuite(M3PerformanceTest_multiclient.class);
        // clientSuite.addTestSuite(HashRingTest.class); // ??
        // clientSuite.addTestSuite(newConnectionTest.class); // worked
        // clientSuite.addTestSuite(M3ClientFailureHandling.class);
        clientSuite.addTestSuite(M4ClientTest.class); 

        return clientSuite;
    }
}