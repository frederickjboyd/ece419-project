package app_kvServer.kvCache;

import app_kvServer.kvCache.LRUCache;
import app_kvServer.kvCache.LFUCache;
import app_kvServer.kvCache.FIFOCache;
import org.apache.log4j.Logger;

public class kvCacheOperator {
    private boolean cacheActive = false;
    private kvCacheTypes cache;
    private static Logger logger = Logger.getRootLogger();

    /**
     * Initialize cache
     * @param size Desired cache size 
     * @param strategy Desired cache strategy
     */
    public kvCacheOperator (int size, String strategy) {
        // Least recently used
        if (strategy == "LRU") {
            cache = new LRUCache(size);
            cacheActive = true;
        }
        // Least frequently used
        else if (strategy == "LFU") {
            cache = new LFUCache(size);
            cacheActive = true;
        }
        // Simple first in - first out cache
        else if (strategy == "FIFO") {
            cache = new FIFOCache(size);
            cacheActive = true;
        }
        // Strategy was none
        else {
            cache = null;
            cacheActive = false;
            logger.error("Failed to initialize cache type: " + strategy);
        }
    }

    /**
     * Return status of cache
     * @return If active, return true, else false
     */
    public boolean cacheActiveStatus() {
        return cacheActive;
    }

    /**
     * Try and retrieve a value from the cache
     * @param key Key for desired value
     * @return
     */
    public String getCache(String key) {
        String value = null;
        try {
            if (cache != null) {
                value = cache.read(key);
                if (value != null) {
                    logger.info("Succesfully retrieved key-val from cache: K:" + key + ",V:" + value);
                    return value;
                } else {
                    logger.info("Key not in cache: K:" + key);
                    return null;
                }
            }
            else
                logger.error("Cache is not initialized, can't read!");
        }
        catch (Exception e) {
            logger.error("Cache read failure!", e);
        }
        return value;
    }

    /**
     * Put KV pair in cache
     * @param key key to be used
     * @param value value to be placed
     */
    public void putCache(String key, String value) {
        try {
            if (cache != null) {
                cache.write(key, value);
                logger.info("Succesfully wrote key-val to cache: K:" + key + ",V:" + value);
            }
            else
                logger.error("Cache is not initialized, can't write!");
        }
        catch (Exception e) {
            logger.error("Cache write failure!", e);
        }
    }

    /**
     * Remove entry from cache
     * @param key
     */
    public void delete(String key) {
        if (cache != null) {
            cache.delete(key);
        }
        else{
            logger.error("Cache is not initialized, can't delete!");
        }
    }

    public boolean inCache(String key) {
        if (cache != null) {
            return cache.inCache(key);
        }
        else {
            logger.error("Cache is not initialized, can't check exists!");
            return false;
        }
    }

    public void clearCache() {
        if (cache != null) {
            cache.clearCache();
        }
        else{
            logger.error("Cache is not initialized, can't clear!");
        }
    }

    public static void main(String [] args) {
        System.out.println("Cache test");
    }
}
