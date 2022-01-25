package app_kvServer;

import logger.LogSetup;
import persistent_storage.PersistentStorage;

// Runnable for threading
public class KVServer implements IKVServer, Runnable{

	private static Logger logger = Logger.getRootLogger();

	private int port;
	private int cacheSize;
	private String strategy;
    private ServerSocket serverSocket;
    private boolean running;

    private PersistentStorage storage;
    private ArrayList<Thread> threadList;

    public static String dataDirectory = "./data";
    public static String databaseName = "database.properties"

    /**
     * Start KV Server at given port
     * 
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO",
     *                  "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        // Store list of client threads
        this.threadList = new ArrayList<Thread>();
        this.port = port;
        this.serverSocket = null;
        this.cacheSize = cacheSize;

        // Check if file directory exists
        File testFile = new File(dataDirectory);
		if (!testFile.exists()){
            this.storage = new PersistentStorage(); 
		}
        // if exists, load into persistentStorage
		else {
            this.storage = new PersistentStorage(databaseName);
		}
        
        // Start new client thread
        Thread newThread = new Thread(this);
        newThread.start();
    }


    @Override
    public int getPort() {
        return this.port;
    }

    @Override
    public String getHostname() {
        String hostname = "";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException e) {
			logger.error("The IP address of server host cannot be resolved. \n", e);
		}
		return hostname;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        // Skip for now
        return IKVServer.CacheStrategy.None;
    }

    @Override
    public int getCacheSize() {
        // TODO Auto-generated method stub
        return this.cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        // need method in persistent storage class
        return false;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
    }

    @Override
    public void run() {
        
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }

    @Override
    public void kill() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            for (int i = 0; i < threadList.size(); i++){
				threadList.get(i).interrupt();	// interrupt and stop all threads
			}
			serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }


    /**
     * Main entry point for the echo server application.
     * 
     * @param args contains the port number at args[0], 
     * cacheSize at args[1],
     * strategy at args[2]
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port>!");
            } else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];
                new KVServer(port, cacheSize, strategy);
            }
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) {
            System.out.println("Error! Invalid argument <port>! Not a number!");
            System.out.println("Usage: Server <port>!");
            System.exit(1);
        }
    }



}
