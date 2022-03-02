package persistent_storage;

import java.io.*;
import java.util.*;
import java.math.BigInteger;

import org.apache.log4j.Logger;
import java.io.IOException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PersistentStorage implements IPersistentStorage {

    // Initialize logger
    private static Logger logger = Logger.getRootLogger();
    // Synchronized Hash map
    private Map<String, String> referenceMap;

    private String directory = "./data";
    // Use properties file for easy storage of maps
    private String databaseName = "database.properties";
    private File testFile;

    /**
     * Initializes the database properties file.
     * Check if directory exists - if not, create it.
     * Check if data.properties file exists - if not, create it.
     */
    private synchronized void init() {
        if (this.testFile == null) {
            logger.info("Running database initialization!");
            File newDir = new File(this.directory);

            // If database directory doesn't exist, create
            if (!newDir.exists()) {
                try {
                    newDir.mkdir();
                } catch (Exception e) {
                    logger.error("Failed mkdir of database directory", e);
                }
            }
            // Create properties file itself
            this.testFile = new File(this.directory + '/' + this.databaseName);
            try {
                // Only creates properties file if doesn't already exist
                if (this.testFile.createNewFile()) {
                    logger.info("Created new storage file!");
                } else {
                    logger.info("Existing storage file found!");
                }
            } catch (IOException e) {
                logger.error("Failed to begin new storage file", e);
            }
        }
    }

    // NOTE: Java does not have optional arguments - overload instead
    /** Build a map - no database existing on file. */
    public PersistentStorage(String serverName) {
        this.databaseName = serverName + "." + "database.properties";
        init();
        Map<String, String> tempMap = new HashMap<String, String>();
        // Activate blank synchronized map
        this.referenceMap = Collections.synchronizedMap(tempMap);
    }

    /**
     * Load existing map from storage.
     * 
     * @param databaseName global database name
     */
    public PersistentStorage(String serverName, String fileName) {
        this.databaseName = serverName + "." + fileName;
        // Check for directory/prop file present
        init();

        // Load local map with existing entries in storage
        Map<String, String> tempMap = new HashMap<String, String>();
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(this.directory + '/' + this.databaseName));
            for (String key : properties.stringPropertyNames()) {
                tempMap.put(key, properties.get(key).toString());
            }
        } catch (IOException e) {
            logger.error("Failed to load existing properties file!", e);
        }
        // Activate synchronized map
        this.referenceMap = Collections.synchronizedMap(tempMap);
    }

    // // Saving a map
    // Map<String, String> ldapContent = new HashMap<String, String>();
    // Properties properties = new Properties();

    // for (Map.Entry<String,String> entry : ldapContent.entrySet()) {
    // properties.put(entry.getKey(), entry.getValue());
    // }

    // properties.store(new FileOutputStream("data.properties"), null);

    // // loading a map
    // Map<String, String> ldapContent = new HashMap<String, String>();
    // Properties properties = new Properties();
    // properties.load(new FileInputStream("data.properties"));

    // for (String key : properties.stringPropertyNames()) {
    // ldapContent.put(key, properties.get(key).toString());
    // }

    /** Write the current map to disk */
    private synchronized void writeMap() {
        Properties properties = new Properties();
        // Loop through entries in current map
        for (Map.Entry<String, String> entry : this.referenceMap.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }

        // Debug delete
        // System.out.println("***Show hash map right before writing");
        // for (Map.Entry<String, String> entry : this.referenceMap.entrySet()) {
        // System.out.println(entry.getKey() + ":" + entry.getValue());
        // }

        // Try write to disk
        try {
            properties.store(new FileOutputStream(this.directory + '/' + this.databaseName), null);
        } catch (IOException e) {
            logger.error("Failed to write map to disk", e);
        }
    }

    /**
     * Put new key-val pair into local map, then call write to disk
     * 
     * @param key   Key to put entry under
     * @param value Value to store under the given key
     */
    @Override
    public synchronized boolean put(String key, String value) {
        try {
            this.referenceMap.put(key, value);
            writeMap();
            logger.info("PUT (" + key + ',' + value + ") into map and wrote to disk!");
            return true;
        } catch (Exception e) {
            logger.error("Failed to PUT (" + key + ',' + value + ") into map!", e);
            return false;
        }
    }

    /**
     * Get a value given a key
     * 
     * @param key Search for value under this key
     */
    @Override
    public synchronized String get(String key) {
        try {
            String value = this.referenceMap.get(key);
            if (value == null) {
                logger.info("No value was found for key: " + key);
                return null;
            } else {
                logger.info("Requested key: " + key + " and retrieved value: " + value);
                return value;
            }
        } catch (Exception e) {
            logger.error("Failed GET request for key: " + key, e);
            return null;
        }
    }

    /**
     * Delete the given key and its entry from the map, then write to disk
     * 
     * @param key Key to delete
     */
    @Override
    public synchronized boolean delete(String key) {
        try {
            /**
             * The method returns the value that was previously mapped to
             * the specified key if the key exists,
             * otherwise the method returns NULL.
             */
            String value = this.referenceMap.remove(key);
            // System.out.println("***Show hash map afer deleting key: "+key);
            // for (Map.Entry<String, String> entry : this.referenceMap.entrySet()) {
            // System.out.println(entry.getKey() + ":" + entry.getValue());
            // }

            // Tried to delete something that doesn't have entries
            if (value == null) {
                logger.info("Failed to delete key: " + key + " as no values exist");
                return false;
            }
            // Delete was succesful, write to disk
            else {
                writeMap();
                logger.info("Deleted key and value: " + key + " " + value);
                return true;
            }
        } catch (Exception e) {
            logger.error("Failed to delete key (exception): " + key, e);
            return false;
        }
    }

    /**
     * Check if key exists in our map, return true if found, false if not.
     * 
     * @param key Key to search for
     */
    @Override
    public synchronized boolean existsCheck(String key) {
        if (this.referenceMap.isEmpty()) {
            logger.info("Failed exist check; map is currently empty!");
            return false;
        } else {
            /**
             * The java.util.HashMap.containsKey() method is used to check whether
             * a particular key is being mapped into the HashMap or not. It takes
             * the key element as a parameter and returns True if that element is
             * mapped in the map.
             */
            return this.referenceMap.containsKey(key);
        }
    }

    /** Fully wipe the data.properties file */
    @Override
    public synchronized void wipeStorage() {
        try {
            this.referenceMap.clear();
            writeMap();
            logger.info("Map and disk fully wiped!");
        } catch (Exception e) {
            logger.error("Failed to wipe map!", e);
        }
    }

    /** Milestone 2 Modifications */

    /**
     * Get MD5 hash
     * 
     * @param key String to be hashed in MD5
     * @return Returns big integer MD5 hash
     */
    public BigInteger MD5Hash(String key) {
        MessageDigest md = null;
        byte[] bytesOfMessage = null;

        try {
            bytesOfMessage = key.getBytes("UTF-8");
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.error("Error in generating MD5 hash!");
        } catch (UnsupportedEncodingException e) {
            logger.error("Unsupported encoding exception!");
        }

        byte[] MD5digest = md.digest(bytesOfMessage);
        BigInteger big_md5 = new BigInteger(1, MD5digest);
        return big_md5;
    }

    /**
     * Check if supplied hash is reachable within begin/end bounds
     * 
     * @param begin Beginning of valid range
     * @param end   End of valid range
     * @param key   Key to be checked
     * @return True if in range, False if out of range
     */
    public boolean keyValid(BigInteger begin, BigInteger end, BigInteger key) {
        // Case 1: Begin <= End, key > begin, key < end
        // Case 2: Begin >= End, key < begin, key < end
        // Case 3: Begin >= End, key > begin, key > end
        if ((begin.compareTo(end) != 1) && (key.compareTo(begin) == 1) && (key.compareTo(end) == -1) ||
                (begin.compareTo(end) != -1) && (key.compareTo(begin) == -1) && (key.compareTo(end) == -1) ||
                (begin.compareTo(end) != -1) && (key.compareTo(begin) == 1) && (key.compareTo(end) == 1)) {
            return true;
        } else {
            return false;
        }
    }


    /**
     * Grab KV Pairs from current server db and check if in valid range.
     * If REACHABLE, return in new table.
     * 
     * @param begin Beginning of valid range
     * @param end   End of valid range
     * @return
     */
    public Map<String, String> hashReachable(BigInteger begin, BigInteger end) {
        // Load local map with existing entries in storage
        Map<String, String> tempMap = new HashMap<String, String>();
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(this.directory + '/' + this.databaseName));
            for (String key : properties.stringPropertyNames()) {
                tempMap.put(key, properties.get(key).toString());
            }
        } catch (IOException e) {
            logger.error("Failed to load existing properties file!", e);
        }
        // Activate synchronized map
        Map<String, String> currentTable = Collections.synchronizedMap(tempMap);
        Map<String, String> newTable = new HashMap<String, String>();
        // If key is reachable in current range, add to new table
        for (Map.Entry keyValPair : currentTable.entrySet()) {
            String key = (String) keyValPair.getKey();
            if (keyValid(begin, end, MD5Hash(key)) == true) {
                newTable.put(key, (String) keyValPair.getValue());
            }
        }
        return newTable;
    }


    /**
     * Grab KV Pairs from current server db and check if in valid range.
     * If UNREACHABLE, return in new table.
     * 
     * @param begin Beginning of valid range
     * @param end   End of valid range
     * @return
     */
    public Map<String, String> hashUnreachable(BigInteger begin, BigInteger end) {
        // Load local map with existing entries in storage
        Map<String, String> tempMap = new HashMap<String, String>();
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(this.directory + '/' + this.databaseName));
            for (String key : properties.stringPropertyNames()) {
                tempMap.put(key, properties.get(key).toString());
            }
        } catch (IOException e) {
            logger.error("Failed to load existing properties file!", e);
        }
        // Activate synchronized map
        Map<String, String> currentTable = Collections.synchronizedMap(tempMap);
        Map<String, String> newTable = new HashMap<String, String>();
        // If key is unreachable in current range, add to new table
        for (Map.Entry keyValPair : currentTable.entrySet()) {
            String key = (String) keyValPair.getKey();
            if (keyValid(begin, end, MD5Hash(key)) == false) {
                newTable.put(key, (String) keyValPair.getValue());
            }
        }
        return newTable;
    }
}
