package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;

import client.KVCommInterface;
import client.KVStore;

public class KVClient implements IKVClient {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "Client> ";
    private BufferedReader stdin;
    private boolean stop = false;

    private String _hostname;
    private int _port;

    private KVStore store = null;

    /**
     * Start running the client.
     */
    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        switch (tokens[0]) {
            case "quit":
                stop = true;
                disconnect();
                System.out.println(PROMPT + "Application exit!");
                break;

            case "connect":
                if (tokens.length == 3) {
                    try {
                        _hostname = tokens[1];
                        _port = Integer.parseInt(tokens[2]);
                        newConnection(_hostname, _port);
                    } catch (NumberFormatException nfe) {
                        printError("No valid address. Port must be a number!");
                        logger.info("Unable to parse argument <port>", nfe);
                    } catch (UnknownHostException e) {
                        printError("Unknown Host!");
                        logger.info("Unknown Host!", e);
                    } catch (IOException e) {
                        printError("Could not establish connection!");
                        logger.warn("Could not establish connection!", e);
                    }
                } else {
                    printError("Invalid number of parameters!");
                }
                break;

            case "send":
                if (tokens.length >= 2) {
                    if (store != null && store.isRunning()) {
                        StringBuilder msg = new StringBuilder();
                        for (int i = 1; i < tokens.length; i++) {
                            msg.append(tokens[i]);
                            if (i != tokens.length - 1) {
                                msg.append(" ");
                            }
                        }
                        // sendMessage(msg.toString());
                    } else {
                        printError("Not connected!");
                    }
                } else {
                    printError("No message passed!");
                }
                break;

            case "put":
                if (tokens.length == 2) {
                    // Delete
                } else if (tokens.length > 2) {
                    // Insert/Update
                    if (store != null && store.isRunning()) {
                        StringBuilder msg = new StringBuilder();
                    }
                } else {
                    printError("No key or value passed!");
                }
                break;

            case "get":
                // TODO: implement
                break;

            case "disconnect":
                disconnect();
                break;

            case "loglevel":
                if (tokens.length == 2) {
                    String level = setLevel(tokens[1]);
                    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT +
                                "Log level changed to level " + level);
                    }
                } else {
                    printError("Invalid number of parameters!");
                }
                break;

            case "help":
            case "h":
                printHelp();
                break;

            default:
                printError("Unknown command");
                printHelp();
                break;
        }
    }

    @Override
    public void newConnection(String hostname, int port) throws UnknownHostException, IOException {
        try {
            store = new KVStore(hostname, port);
            store.connect();
        } catch (UnknownHostException uhe) {
            printError("Unknown host!");
            logger.info("Unknown host!");
        } catch (IOException e) {
            printError("Could not establish connection!");
            logger.warn("Could not establish connection!");
        } catch (Exception e) {
            printError("Error with KVStore connect!");
            logger.info("Error with KVStore connect!");
        }
    }

    public void disconnect() {
        if (store != null) {
            store.disconnect();
            store = null;
        }
    }

    @Override
    public KVCommInterface getStore() {
        return store;
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t inserts a key-value pair into the storage server\n");
        sb.append("\t\t\t\t\t ");
        sb.append("updates current value with given value if server already contains key\n");
        sb.append("\t\t\t\t\t ");
        sb.append("deletes entry for given key if <value> is null\n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieves value for the given key from the storage server\n");

        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    /**
     * Main entry point for the echo server application.
     * 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.OFF);
            KVClient store = new KVClient();
            store.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
