package testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import app_kvServer.IKVServer.CacheStrategy;
import junit.framework.TestCase;
import ecs.ECSNode;
import ecs.HashRing;

public class HashRingEvenTest extends TestCase {
    private HashRing hashRingClass = new HashRing();
    private static final String CONFIG_PATH = "ecs.config";
    private List<String> allServerInfo = new ArrayList<String>();
    private static final int DEFAULT_NUM_NODES = 10;

    public void setUp() {
        try {
            // Read configuration file
            BufferedReader reader = new BufferedReader(new FileReader(CONFIG_PATH));
            String l;

            while ((l = reader.readLine()) != null) {
                String[] config = l.split("\\s+", 3);
                allServerInfo.add(String.format("%s:%s:%s", config[0], config[1], config[2]));
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    /**
     * Verify that the hash ring can create a hash range for one node that covers
     * all possible hash ranges.
     */
    public void testHashRangeEven1() {
        addNodes(1);

        // Get the one node in the hash ring
        HashMap<BigInteger, ECSNode> hashRing = hashRingClass.getHashRing();
        Set<BigInteger> nodeIDsSet = hashRing.keySet();
        List<BigInteger> nodeIDsList = new ArrayList<BigInteger>(nodeIDsSet);
        ECSNode node = hashRing.get(nodeIDsList.get(0));

        // Check hash range
        BigInteger[] nodeHashRange = node.getNodeHashRange();
        BigInteger nodeHashRangeStart = nodeHashRange[0];
        BigInteger nodeHashRangeStop = nodeHashRange[1];
        // System.out.println(String.format("Start: %x", nodeHashRangeStart));
        // System.out.println(String.format("Stop: %x", nodeHashRangeStop));

        assertTrue(hashRing.size() == 1 && isEqual(nodeHashRangeStart, HashRing.MIN_MD5)
                && isEqual(nodeHashRangeStop, HashRing.MAX_MD5));
        cleanHashRing();
    }

    @Test
    /**
     * Verify that the hash ring creates two evenly spaced hash ranges for two
     * nodes.
     */
    public void testHashRangeEven2() {
        addNodes(2);

        List<BigInteger[]> hashRanges = getHashRanges();
        List<BigInteger> hashRangeSizes = getHashRangeSizes(hashRanges);
        boolean isValid = checkHashRangeSizes(hashRangeSizes);

        assertTrue(isValid == true);
        cleanHashRing();
    }

    @Test
    /**
     * Verify that the hash ring creates evenly spaced hash ranges for all 50 nodes.
     */
    public void testHashRangeEven50() {
        addNodes(50);

        List<BigInteger[]> hashRanges = getHashRanges();
        List<BigInteger> hashRangeSizes = getHashRangeSizes(hashRanges);
        boolean isValid = checkHashRangeSizes(hashRangeSizes);

        assertTrue(isValid == true);
        cleanHashRing();
    }

    @Test
    /**
     * Verify that hash ranges remain even after removing a node from the hash ring.
     */
    public void testHashRangeEvenRemove() {
        addNodes(DEFAULT_NUM_NODES);

        List<BigInteger[]> hashRangesBefore = getHashRanges();
        List<BigInteger> hashRangeSizesBefore = getHashRangeSizes(hashRangesBefore);
        boolean isValidBefore = checkHashRangeSizes(hashRangeSizesBefore);

        // Remove node
        String serverInfo = allServerInfo.get(DEFAULT_NUM_NODES - 1);
        hashRingClass.removeNode(serverInfo);

        List<BigInteger[]> hashRangesAfter = getHashRanges();
        List<BigInteger> hashRangeSizesAfter = getHashRangeSizes(hashRangesAfter);
        boolean isValidAfter = checkHashRangeSizes(hashRangeSizesAfter);

        assertTrue(isValidBefore == true && isValidAfter == true);
        cleanHashRing();
    }

    @Test
    /**
     * Verify that hash ranges remain equal after adding a node to the hash ring.
     */
    public void testHashRangeEvenAdd() {
        addNodes(DEFAULT_NUM_NODES);

        List<BigInteger[]> hashRangesBefore = getHashRanges();
        List<BigInteger> hashRangeSizesBefore = getHashRangeSizes(hashRangesBefore);
        boolean isValidBefore = checkHashRangeSizes(hashRangeSizesBefore);

        // Remove node
        String serverInfo = allServerInfo.get(DEFAULT_NUM_NODES);
        // System.out.println("serverInfo: " + serverInfo);
        ECSNode node = hashRingClass.createECSNode(serverInfo, CacheStrategy.None, 0);
        // System.out.println("node: " + node);
        hashRingClass.addNode(node);

        List<BigInteger[]> hashRangesAfter = getHashRanges();
        List<BigInteger> hashRangeSizesAfter = getHashRangeSizes(hashRangesAfter);
        boolean isValidAfter = checkHashRangeSizes(hashRangeSizesAfter);

        assertTrue(isValidBefore == true && isValidAfter == true);
        cleanHashRing();
    }

    @Test
    /**
     * Verify that the function to calculate even hash ranges can handle invalid
     * inputs.
     */
    public void testCalculateEvenHashRangesInvalid() {
        List<BigInteger> hashRangesEven0 = hashRingClass.calculateEvenHashRanges(0);
        List<BigInteger> hashRangesEven1 = hashRingClass.calculateEvenHashRanges(-1);
        List<BigInteger> hashRangesEven2 = hashRingClass.calculateEvenHashRanges(-10000);

        int hashRangesEven0Size = hashRangesEven0.size();
        int hashRangesEven1Size = hashRangesEven1.size();
        int hashRangesEven2Size = hashRangesEven2.size();

        assertTrue(hashRangesEven0Size == 0 && hashRangesEven1Size == 0 && hashRangesEven2Size == 0);
    }

    /**
     * Helper function to add a specified number of nodes to the hash ring.
     * 
     * NOTE: Assumes that the hash ring is empty.
     * 
     * @param count
     */
    private void addNodes(int count) {
        for (int i = 0; i < count; i++) {
            if (hashRingClass.getHashRing().isEmpty()) {
                List<String> newServerInfoList = new ArrayList<String>();
                newServerInfoList.add(allServerInfo.get(0));
                // Creates and adds node to hash ring
                hashRingClass.initHashRing(newServerInfoList, CacheStrategy.None, 0);
            } else {
                String newServerInfo = allServerInfo.get(i);
                ECSNode newNode = hashRingClass.createECSNode(newServerInfo, CacheStrategy.None, 0);
                hashRingClass.addNode(newNode);
            }
        }
    }

    /**
     * Helper function to remove all nodes from the hash ring.
     */
    private void cleanHashRing() {
        // System.out.println("cleanHashRing enter...");
        int hashRingSize = hashRingClass.getHashRing().size();
        // System.out.println("hashRingSize before: " + hashRingSize);

        for (int i = 0; i < hashRingSize; i++) {
            String serverInfo = allServerInfo.get(i);
            hashRingClass.removeNode(serverInfo);
        }

        // System.out.println("hashRingSize after: " +
        // hashRingClass.getHashRing().size());
    }

    /**
     * Check if two BigIntegers are equal
     * 
     * @param arg1
     * @param arg2
     * @return True if equal, false otherwise
     */
    private boolean isEqual(BigInteger arg1, BigInteger arg2) {
        int result = arg1.compareTo(arg2);

        boolean isEqual = result == 0 ? true : false;
        return isEqual;
    }

    /**
     * Get list of all node hash ranges currently in the hash ring.
     * 
     * @return
     */
    private List<BigInteger[]> getHashRanges() {
        List<BigInteger[]> hashRanges = new ArrayList<BigInteger[]>();
        int hashRingSize = hashRingClass.getHashRing().size();

        for (int i = 0; i < hashRingSize; i++) {
            String serverInfo = allServerInfo.get(0);
            ECSNode currNode = hashRingClass.getNodeByServerInfo(serverInfo);
            BigInteger[] currHashRange = currNode.getNodeHashRange();
            hashRanges.add(currHashRange);
        }

        return hashRanges;
    }

    /**
     * Given a list of each node's hash range, calculate the size of each one.
     * 
     * @param hashRanges
     * @return
     */
    private List<BigInteger> getHashRangeSizes(List<BigInteger[]> hashRanges) {
        List<BigInteger> hashRangeSizes = new ArrayList<BigInteger>();

        for (BigInteger[] currHashRange : hashRanges) {
            BigInteger currHashRangeStart = currHashRange[0];
            BigInteger currHashRangeStop = currHashRange[1];
            BigInteger size = currHashRangeStop.subtract(currHashRangeStart);
            hashRangeSizes.add(size);
        }

        return hashRangeSizes;
    }

    /**
     * Given a list of each node's hash range size, check that they are evenly
     * distributed.
     * 
     * @param hashRangeSizes
     * @return
     */
    private boolean checkHashRangeSizes(List<BigInteger> hashRangeSizes) {
        boolean isValid = true;

        if (hashRangeSizes.size() == 0) {
            return isValid;
        }

        // Get one range to compare everything against
        BigInteger referenceRange = hashRangeSizes.get(0);

        for (BigInteger range : hashRangeSizes) {
            // System.out.println(String.format("%x", range));
            isValid = isEqual(referenceRange, range);
        }

        return isValid;
    }
}
