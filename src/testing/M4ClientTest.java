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

public class M4ClientTest extends TestCase {
    private static Logger logger = Logger.getRootLogger();
    public List<KVStore> kvClientList;
    // ECS Client
    private static final String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecs = null;
    // KVServer
    private static final int numServers = 3;
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

            ecsNodeList = ecs.addNodes(numServers, cacheStrategy, cacheSize);
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

    public void testClientUserFriendlyDesign() {
        // Connect all KVClients
        String key = "foo2";
        String value = "bar2";
        String key1 = "3";
        String value1 = "bar1";
        KVMessage response = null;
        Exception ex = null;
        List<String> serverInUse = new ArrayList<>();
        List<String> serverInUse2 = new ArrayList<>();

        // verifying normal get and put won't affect server list returning

        try {
            response = kvClientList.get(0).put(key, value);
            response = kvClientList.get(1).put(key1, value1);

        } catch (Exception e) {
            ex = e;
        }

        serverInUse = kvClientList.get(0).getServerInUseList();
        assertTrue(numServers == serverInUse.size());
        assertTrue(ex == null
                && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() == StatusType.PUT_UPDATE));

        System.out.println("After put, the returned list remains accurate");

        try {
            response = kvClientList.get(0).get(key1);
            
        } catch (Exception e) {
            ex = e;
        }

        serverInUse = kvClientList.get(0).getServerInUseList();
        assertTrue(numServers == serverInUse.size());
        assertTrue(ex == null && (response.getStatus() == StatusType.GET_SUCCESS));
        System.out.println("SUCCESS: After get, the returned list remains accurate");

        
        // verify that the server list update are insync
        serverInUse = kvClientList.get(0).getServerInUseList();
        serverInUse2 = kvClientList.get(1).getServerInUseList();
        assertTrue(numServers == serverInUse.size() && serverInUse.size()==serverInUse2.size());
        System.out.println("SUCCESS: server lists are in sync across users");

        // 

        try {
            response = kvClientList.get(2).get("3");
            // response = kvClientList.get(0).put("sssssss", value);
            // response = kvClientList.get(3).put("3", value);
            // response = kvClientList.get(3).put("sdf", value);

        } catch (Exception e) {
            ex = e;
        }
        
        // To visualize that the client can still see the list after switching server
        System.out.println("SUCCESS if visual appeared with "+String.valueOf(numServers)+" server appearing in the list");
        serverInUse = kvClientList.get(0).getServerInUseList();
        serverInUse2 = kvClientList.get(1).getServerInUseList();
        System.out.println(serverInUse );
        System.out.println(serverInUse2 );

        


        // assertTrue(serverInUse == serverInUse2);
        // assertTrue(ex == null
        // && (response.getStatus() == StatusType.PUT_SUCCESS || response.getStatus() ==
        // StatusType.PUT_UPDATE));
        // System.out.println("initial put success");
        // int initPort1 = kvClientList.get(0).getCurrentPort();
        // int initPort2 = kvClientList.get(1).getCurrentPort();

        // testClientUpdateServerListAfterCrashing
        // // crash server
        // // server killable
        // killableProcess = new ArrayList<Integer>();
        // for (Integer pid : javaPIDs) {
        //     if (!initJavaPIDs.contains(pid)) {
        //         killableProcess.add(pid);
        //     }
        // }

        // StringBuilder killCmd = new StringBuilder();
        // killCmd.append("kill ");
        // killCmd.append(killableProcess.get(2));
        // System.out.println(killCmd);

        // try {
        //     Process p = Runtime.getRuntime().exec(killCmd.toString());
        // } catch (Exception e) {
        //     logger.error("Unable to kill a server Java programs");
        //     e.printStackTrace();
        // }

        // try {
        //     ecs.awaitNodes(numServers, 45000);
        // } catch (Exception e) {
        //     System.out.println("Failed to await nodes!");
        // }

        // System.out.println("Initial");

        // // try putting
        // String newkey = "new";
        // String newvalue = "neww";
        // try {
        // response = kvClientList.get(0).put(newkey, newvalue);
        // } catch (Exception e) {
        // ex = e;
        // }

        // System.out.println("recovered put success");
        // System.out.println(response.getStatus());

    
        // assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);

        // // try updating
        // try {
        // response = kvClientList.get(0).put(key1, value);
        // } catch (Exception e) {
        // ex = e;
        // }
        // assertTrue(ex == null && (response.getStatus() == StatusType.PUT_UPDATE));
        // System.out.println("recovered update success");
        // System.out.println(response.getStatus());

        // try {
        // response = kvClientList.get(0).get(key1);
        // } catch (Exception e) {
        // ex = e;
        // }
        // assertTrue(ex == null && (response.getStatus() == StatusType.GET_SUCCESS));
        // System.out.println("recovered get success");
        // System.out.println(response.getStatus());

        // try {
        //     ecs.cleanData();
        //     ecs.cleanLogs();
        //     ecs.quit();
        //     System.out.println("Shutdown - success");
        // } catch (Exception e) {
        //     System.err.println("Failed to shut down ECS!");
        // }

    }
}
