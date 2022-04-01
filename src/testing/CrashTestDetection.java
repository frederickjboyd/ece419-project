package testing;

import org.junit.Test;

import app_kvECS.*;
import ecs.ECSNode;
import ecs.ECSNode.NodeStatus;
import ecs.HashRing;
import logger.LogSetup;
import shared.communication.AdminMessage;
import shared.communication.AdminMessage.MessageType;
import shared.DebugHelper;
import shared.Metadata;
import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.nio.charset.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;





public class CrashTestDetection extends TestCase {
    // ECS Client
    private static final String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private ECSClient ecs;
    private List<Integer> initialJavaPIDs;

    //******** USER CONFIGURABLE VARIABLES FOR TESTING */
    // KVServer
    private static final int numServers = 3;
    private static final String cacheStrategy = "FIFO";
    private static final int cacheSize = 500;
    // Number of clients (current only supports <=5)
    private static final int numClients = 1;

    public void setUp(){
        System.out.println("Begin crash detection test.....");
    }

    public void tearDown(){
        System.out.println("End crash detection test.....");
    }

    public void setUpInternal() {
        // Track current running Java PIDs
        // Only want to kill new Java processes that have been created after ECSClient
        // launch (i.e. servers)
        initialJavaPIDs = getJavaPIDs();

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
        System.out.println("*** Finished ECS Setup for performance run ***");
        
    }

    public void tearDownInternal() {
        System.out.println("Shutdown performance - start");
        // kvClient.disconnect();
        try {
            ecs.cleanData();
            ecs.cleanLogs();
            ecs.quit();
            System.out.println("Shutdown performance - success");
        } catch (Exception e) {
            System.err.println("Failed to shut down ECS!");
        }
    }


    /**
     * Helper function to kill all Java processes, regardless of when they were
     * created.
     */
    private void killJavaProcess(int numberToKill) {
        // Manually kill all new "java" processes
        List<Integer> javaPIDs = getJavaPIDs();

        // Construct kill command
        StringBuilder killCmd = new StringBuilder();
        killCmd.append("kill ");

        // Control the number of servers we kill
        int numberKilled = 0;

        for (Integer pid : javaPIDs) {
            if (!initialJavaPIDs.contains(pid)) {
                killCmd.append(pid);
                killCmd.append(" ");
                numberKilled += 1;

                if (numberKilled == numberToKill){
                    break;
                }
            }
        }
        killCmd.append("&");
        // Execute
        try {
            // logger.info("Cleaning up Java programs");
            // logger.info(killCmd.toString());
            Process p = Runtime.getRuntime().exec(killCmd.toString());
            System.out.println("\n********* CRASHED " + Integer.toString(numberKilled) + " SERVERS: \n" + killCmd.toString() + "\n");
        } catch (Exception e) {
            // logger.error("Unable to clean up Java programs");
            e.printStackTrace();
        }
    }

    /**
     * Get list of current Java PIDs a user is running.
     * 
     * @return
     */
    public List<Integer> getJavaPIDs() {
        // Get username
        String homeDir = System.getProperty("user.home");
        String[] homeDirArray = homeDir.split("/");
        String username = homeDirArray[homeDirArray.length - 1];
        // Get all user-specific processes
        String cmd = String.format("ps -u %s", username);
        List<Integer> javaPrograms = null;

        try {
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader stdIn = new BufferedReader(new InputStreamReader(p.getInputStream()));
            javaPrograms = parseProcessList(stdIn);
        } catch (Exception e) {
            // logger.error(String.format("Unable to execute or read output of %s", cmd));
            e.printStackTrace();
        }

        // logger.debug(String.format("Current running Java processes: %s", javaPrograms));

        return javaPrograms;
    }

    /**
     * Parse list of processes a user is currently running and get the PIDs of all
     * Java programs.
     * 
     * @param stdIn
     * @return
     */
    private List<Integer> parseProcessList(BufferedReader stdIn) {
        List<Integer> javaPrograms = new ArrayList<Integer>();
        String line;

        try {
            while ((line = stdIn.readLine()) != null) {
                String[] psArray = line.split("\\s+");

                int pid = -1;
                String cmd;
                try {
                    if (psArray.length == 5) {
                        pid = Integer.parseInt(psArray[1]);
                        cmd = psArray[4];
                    } else {
                        pid = Integer.parseInt(psArray[0]);
                        cmd = psArray[3];
                    }
                } catch (Exception e) {
                    continue;
                }

                if (cmd.equals("java")) {
                    javaPrograms.add(pid);
                }
            }
        } catch (Exception e) {
            // logger.error("Unable to parse list of processes");
            e.printStackTrace();
        }

        return javaPrograms;
    }

    /**
     * Helper function to run the crash loop
     */
    public void crashLoopHelper(int numberToKill){
        int beforeCrashServerCount = 0;
        int afterCrashServerCount = 0;

        HashMap<String, NodeStatus> serverStatusInfo = ecs.serverStatusInfo;
        System.out.println("*********** SERVER STATUS INFO before crash ***********:\n");
        // Count number of alive processes
        for (Map.Entry<String, NodeStatus> entry : serverStatusInfo.entrySet()) {
            if (entry.getValue().equals(NodeStatus.ONLINE)){
                System.out.println(entry);
                beforeCrashServerCount += 1;
            }
        }
        // Kill only 1 server
        killJavaProcess(numberToKill);
        try{
            ecs.awaitNodes(numServers, 30000);
        }
        catch(Exception e){
            System.out.println("Failed to await nodes!");
        }

        serverStatusInfo = ecs.serverStatusInfo;
        System.out.println("*********** SERVER STATUS INFO after crash ***********:\n");
        // Count number of alive processes
        for (Map.Entry<String, NodeStatus> entry : serverStatusInfo.entrySet()) {
            if (entry.getValue().equals(NodeStatus.ONLINE)){
                System.out.println(entry);
                afterCrashServerCount += 1;
            }
        }
        assertTrue(beforeCrashServerCount == afterCrashServerCount);
        System.out.println("\n*** Server crash succesfully detected, recovered! ***\n");
    }


    /** Run performance test **/
    @Test
    public void testCrashDetection() {
        setUpInternal();
        crashLoopHelper(1);
        // crashLoopHelper(2);
        // crashLoopHelper(3);
        tearDownInternal();
    }
}