package testing;
import java.net.UnknownHostException;
import client.KVStore;
import app_kvECS.ECSClient;
import ecs.ECSNode;
import junit.framework.TestCase;
import java.util.List;
import org.apache.log4j.Logger;


public class ConnectionTest extends TestCase {
    private String cacheStrategy = "FIFO";
    private int cacheSize = 500;
    private String host;
    private int port;
    private int numServers = 5;
    private String ECSConfigPath = System.getProperty("user.dir") + "/ecs.config";
    private List<ECSNode> nodesAdded;
    private ECSClient ecs;
    private Exception ex;
    private static Logger logger = Logger.getRootLogger();

    public void setUp() {
        ecs = new ECSClient(ECSConfigPath);
        nodesAdded = ecs.addNodes(numServers, cacheStrategy, cacheSize);

        try {
            ecs.start();
        } catch (Exception e) {
            System.out.println("ECS Performance Test failed on ECSClient init: " + e);
        }
        
        host = nodesAdded.get(0).getNodeHost();
        port = nodesAdded.get(0).getNodePort();
        logger.info(host);
        
        System.out.println("connection test set up success");
    }


    public void testConnectionSuccess() {
        // kvClient = new KVStore("localhost", 50000);

        KVStore kvClient = new KVStore(host, port);
        System.out.println(host);
        System.out.println(port);




        // try {
        //     kvClient.connect();
        // } catch (Exception e) {
        //     ex=e;
        // }

        // assertNull(ex);
    }

    // public void testUnknownHost() {
    //     Exception ex = null;
    //     KVStore kvClient = new KVStore("unknown", port);

    //     try {
    //         kvClient.connect();
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex instanceof UnknownHostException);
    // }

    // public void testIllegalPort() {
    //     Exception ex = null;
    //     KVStore kvClient = new KVStore(host, 123456789);

    //     try {
    //         kvClient.connect();
    //     } catch (Exception e) {
    //         ex = e;
    //     }

    //     assertTrue(ex instanceof IllegalArgumentException);
    // }

}
