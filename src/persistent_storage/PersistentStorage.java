package persistent_storage;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.List;
import java.util.ArrayList;
import persistent_storage.IPersistentStorage;

// Logging
import org.apache.log4j.Logger;
import java.io.IOException;

public class PersistentStorage implements IPersistentStorage {

    // Initialize logger
    private static Logger logger = Logger.getRootLogger();
    // Synchronized Hash map
    private Map<String, String> referenceMap;

    private String directory = "./data";
    // Use properties file for easy storage of maps
    private String databaseName = "database.properties";
    private File testFile;

    // Database initialization
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
                logger.error("Failed to start new storage file", e);
            }
        }
    }

    // NOTE: Java does not have optional arguments - overload instead
    // Build a map - no database existing
    public PersistentStorage() {
        init();
        Map<String, String> tempMap = new HashMap<String, String>();
        this.referenceMap = Collections.synchronizedMap(tempMap);
    }

    /**
     * Load existing map from storage
     * 
     * @param databaseName global database name
     */
    public PersistentStorage(String databaseName) {
        this.databaseName = databaseName;
        init();

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

    // Writes the current map to disk
    private synchronized void writeMap() {
        Properties properties = new Properties();
        // Loop through entries in current map
        for (Map.Entry<String, String> entry : this.referenceMap.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }
        // Try write to disk
        try {
            properties.store(new FileOutputStream(this.directory + '/' + this.databaseName), null);
        } catch (IOException e) {
            logger.error("Failed to write map to disk", e);
        }
    }

    // Put new key-val pair into map, write to disk
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

    // Get value given key
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

    // Delete key and value from storage
    @Override
    public synchronized boolean delete(String key) {
        try {
            /**
             * The method returns the value that was previously mapped to
             * the specified key if the key exists,
             * otherwise the method returns NULL.
             */
            String value = this.referenceMap.remove(key);

            if (value == null) {
                logger.info("Failed to delete key: " + key + " as no values exist");
                return false;
            } else {
                writeMap();
                logger.info("Deleted key and value: " + key + " " + value);
                return true;
            }
        } catch (Exception e) {
            logger.error("Failed to delete key (exception): " + key, e);
            return false;
        }
    }

    // boolean - check if key-val exists in map
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

    // Fully wipe storage
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
}
