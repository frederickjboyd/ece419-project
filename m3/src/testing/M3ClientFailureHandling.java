package testing;

import org.junit.Test;
import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import java.util.List;
import ecs.ECSNode;
import java.util.ArrayList;
import java.util.Arrays;
import app_kvECS.ECSClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class M3ClientFailureHandling extends TestCase {
    private static Logger logger = Logger.getRootLogger();
    public List<KVStore> kvClientList;
    // ECS Client
    private static final String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecs = null;
    // KVServer
    private static final int numServers = 2;
    private static final String cacheStrategy = "FIFO";
    private static final int cacheSize = 50;
    private String testHost;
    private Exception ex;
    private int testPort;
    private List<Integer> initJavaPIDs;
    private List<Integer> curJavaPIDs;
    private List<Integer> javaPIDs;
    private List<ECSNode> ecsNodeList = null;
    private ArrayList<Integer> killableProcess;;

    public void setUp() {
        try {
            System.out.println("Setting up ECS performance test!");
            ecs = new ECSClient(ECSConfigPath);
            initJavaPIDs = ecs.getJavaPIDs();
            for (int j = 0; j < initJavaPIDs.size(); j++) {
                System.out.println(initJavaPIDs.get(j));
            }

            ecsNodeList = ecs.addNodes(numServers, cacheStrategy, cacheSize, false);
            try {
                ecs.awaitNodes(numServers, 2000);
            } catch (Exception e) {
            }
            System.out.println("Starting ECS!");
            ecs.start();

            javaPIDs = ecs.getJavaPIDs();
            for (int i = 0; i < javaPIDs.size(); i++) {
                System.out.println(javaPIDs.get(i));
            }
        } catch (Exception e) {
            ex = e;
            System.out.println(" Test failed on ECSClient init: " + e);
        }
        // Pick a random available server to connect to
        kvClientList = new ArrayList<KVStore>();

        for (int i = 0; i < numServers; i++) {
            testHost = ecsNodeList.get(i).getNodeHost();
            testPort = ecsNodeList.get(i).getNodePort();
            // KVStore tempKVStore = new KVStore(hostname, port);
            kvClientList.add(new KVStore(testHost, testPort));
            try {
                kvClientList.get(i).connect();
            } catch (Exception e) {
                ex = e;
                System.err.println("test connection Test FAILURE: Client Connection Failed!");
            }

            System.out.println("test connecting to: " + testHost + ":" + testPort);
        }
    }

    public void testFailureHandlingSuccess() {
        // Connect all KVClients
        String key = "foo2";
        String value = "bar2";
        String key1 = "qpppppppppppppppp";
        String value1 = "bar1";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClientList.get(0).put(key, value);
            response = kvClientList.get(1).put(key1, value1);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null
                && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));
        System.out.println("initial put success");
        // int initPort1 = kvClientList.get(0).getCurrentPort();
        // int initPort2 = kvClientList.get(1).getCurrentPort();

        System.out.println("Initial");
        // System.out.println(kvClientList.get(0).getCurrentAddress());
        // System.out.println(kvClientList.get(0).getCurrentPort());

        // System.out.println(kvClientList.get(1).getCurrentAddress());
        // System.out.println(kvClientList.get(1).getCurrentPort());

        // crash server
        // server killable
        killableProcess = new ArrayList<Integer>();
        for (Integer pid : javaPIDs) {
            if (!initJavaPIDs.contains(pid)) {
                killableProcess.add(pid);
            }
        }

        StringBuilder killCmd = new StringBuilder();
        killCmd.append("kill ");
        killCmd.append(killableProcess.get(0));
        System.out.println(killCmd);

        try {
            Process p = Runtime.getRuntime().exec(killCmd.toString());
        } catch (Exception e) {
            logger.error("Unable to kill a server Java programs");
            e.printStackTrace();
        }

        // try{
        // ecs.awaitNodes(numServers, 45000);
        // }
        // catch(Exception e){
        // System.out.println("Failed to await nodes!");
        // }

        // try putting
        String newkey = "new";
        String newvalue = "neww";
        try {
            response = kvClientList.get(0).put(newkey, newvalue);
        } catch (Exception e) {
            ex = e;
        }

        System.out.println("recovered put success");
        System.out.println(response.getStatus());

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);

        // try updating
        try {
            response = kvClientList.get(0).put(key1, value);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && (response.getStatus() == StatusType.PUT_UPDATE));
        System.out.println("recovered update success");
        System.out.println(response.getStatus());

        try {
            response = kvClientList.get(0).get(key1);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && (response.getStatus() == StatusType.GET_SUCCESS));
        System.out.println("recovered get success");
        System.out.println(response.getStatus());

        // try {
        // response = kvClientList.get(0).put(key1, value);
        // System.out.println("SWITCHED updaet TO");
        // System.out.println(kvClientList.get(0).getCurrentAddress());
        // System.out.println(kvClientList.get(0).getCurrentPort());

        // System.out.println(kvClientList.get(1).getCurrentAddress());
        // System.out.println(kvClientList.get(1).getCurrentPort());
        // response = kvClientList.get(0).get(key);
        // System.out.println("went");
        // response = kvClientList.get(1).get(key1);
        // System.out.println("SWITCHED TO");
        // System.out.println(kvClientList.get(0).getCurrentAddress());
        // System.out.println(kvClientList.get(0).getCurrentPort());

        // System.out.println(kvClientList.get(1).getCurrentAddress());
        // System.out.println(kvClientList.get(1).getCurrentPort());

        // } catch (Exception e) {
        // ex = e;
        // }

        // assertTrue(ex == null&& (response.getStatus() == StatusType.GET_SUCCESS));
        // System.out.println("get success");

        try {
            ecs.cleanData();
            ecs.cleanLogs();
            ecs.quit();
            System.out.println("Shutdown - success");
        } catch (Exception e) {
            System.err.println("Failed to shut down ECS!");
        }

    }
}
