package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import logger.LogSetup;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;
import client.KVStore;

import logger.LogSetup;

import java.io.InputStream;
import java.io.OutputStream;

import java.net.Socket;
import org.apache.log4j.Level;

import org.apache.log4j.Logger;

// added 

public class KVClient implements IKVClient {
    private static Logger logger = Logger.getRootLogger();
    private KVStore kvStore = null;
    private BufferedReader stdin;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;
    private static final String PROMPT = "KVClient> ";

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        // TODO Auto-generated method stub
        try {
            kvStore = new KVStore(hostname, port);
            kvStore.connect();
            
            logger.info("kvclient: New connection established");
        } catch (IOException ioe) {
            logger.error("kvclient: failed to establish new connection!");

        }
    }

    @Override
    public KVStore getStore() {
        return kvStore;
    }

    // modify from echo client
    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                try {
                    this.handleCommand(cmdLine);
                } catch (Exception e) {
                    // TODO: add logging to me
                    e.getMessage();
                }
            } catch (IOException e) {
                stop = true;
                // setRunning(false);
                printError("CLI does not respond - Application terminated ");
                logger.error("kvclient: CLI not respond!");
            }
        }
    }

    private void handleCommand(String cmdLine)
            throws NumberFormatException, UnknownHostException, IOException, Exception {
        String[] tokens = cmdLine.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            kvStore.disconnect();
            System.out.println(PROMPT + "Application exit!");
            logger.info("kvclient: Application quit and connection disconnect");

        } else if (tokens[0].equals("Available")) {
            if (tokens[1].equals("server")) {
                kvStore.getAvailableServer();
            }

        }else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);

                } catch (NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.error("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.error("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.error("Could not establish connection!", e);
                } catch (Exception e) {
                    printError("Other exception");
                    logger.error("Other exception", e);
                }
            } else {
                printError("Invalid number of parameters!");
                logger.error("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("put")) {
            if (tokens.length >= 2) {
                if (kvStore != null && kvStore.isRunning()) {
                    System.out.println("Inside put if statement!");

                    // Find the start of the message then transfer everything over
                    String key = tokens[1].toString();
                    String value = "";

                    if (tokens.length == 2) {
                        // msg.append("null");
                        value = "null";
                    } else {
                        int msgKeyIdx = cmdLine.indexOf(tokens[1].toString()) + tokens[1].length();
                        // deal with issue if key and value are the same
                        int msgStartIdx = cmdLine.substring(msgKeyIdx, cmdLine.length()).indexOf(tokens[2].toString())
                                + msgKeyIdx;
                        // int msgStartIdx = cmdLine.indexOf(tokens[2].toString());
                        value = cmdLine.substring(msgStartIdx, cmdLine.length()).toString();
                    }

                    try {
                        // kvStore.put(tokens[1].toString(), msg.toString());
                        KVMessage receivedMsg = kvStore.put(key, value);
                        if (receivedMsg.getStatus() == StatusType.SERVER_STOPPED) {
                            logger.info("server is not functioning, values not stored");
                        } else {
                            logger.info("kvclient Update database " + " with Key: " + key + " and values:" + value);
                            logger.info(receivedMsg.getValue());
                        }

                    } catch (Exception e) {
                        // e.getMessage();
                        logger.error("kvclient put exception", e);
                    }
                } else {
                    printError("Not connected!");
                    // logger.error("kvclient not connected");

                }
            } else {
                printError("Error Missing value or key!");
                // logger.error("kvclient get command missing value or key");

            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length >= 1) {
                if (kvStore != null && kvStore.isRunning()) {
                    StringBuilder msg = new StringBuilder();
                    for (int i = 1; i < tokens.length; i++) {
                        msg.append(tokens[i]);
                        if (i != tokens.length - 1) {
                            msg.append(" ");
                        }
                    }
                    try {

                        KVMessage receivedMsg = kvStore.get(tokens[1]);
                        if (receivedMsg.getStatus() == StatusType.SERVER_STOPPED) {
                            logger.info("server is not functioning, values not fetched");
                        } else {
                            logger.info("kvclient GET on port" + ": retrieve Key: " + tokens[1] + " from server");
                        }

                    } catch (Exception e) {
                        logger.error("kvclient get exception", e);
                    }
                } else {
                    printError("Not connected!");
                    // logger.error("kvclient not connected");

                }
            } else {
                printError("Error Missing value or key!");
                // logger.error("kvclient put command missing key");

            }

        } else if (tokens[0].equals("disconnect")) {
            kvStore.disconnect();

        } else if (tokens[0].equals("logLevel")) {
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

        } else if (tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("send <text message>");
        sb.append("\t\t sends a text message to the server \n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");

        sb.append(PROMPT).append("put <key> <value>");
        sb.append(
                "\t\t\t update the current value with the gven value if key already in server. Or will delete the entry for the given key is <value> is null \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieve storage server value for the given key \n");

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

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.DEBUG);
            KVClient store = new KVClient();
            store.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }

    }
}
