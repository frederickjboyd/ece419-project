package app_kvServer.kvCache;

import app_kvServer.kvCache.kvCacheTypes;
import java.util.*;

public class FIFOCache extends kvCacheTypes{

    // Cache map itself
    private Map<String, String> cache;

    /**
     * Initialize FIFO cache
     * @param size desired cache size
     */
    public FIFOCache(int size) {
        super(size);
        // Leave at default - insertion order rather than access
        this.cache = Collections.synchronizedMap(new LinkedHashMap<String, String>(size));
    }

    @Override
    public String read(String key) {
        synchronized (cache) {
            if (cache.containsKey(key)) {
                return cache.get(key);
            }
            else {
                return null;
            }
        }
    }

    @Override
    public void write(String key, String value) {
        synchronized (cache) {
            // We are full, kick out some entries
            if (currentLoad == cacheCapacity) {
                synchronized (cache) {
                    // Remove first entry
                    cache.remove(cache.keySet().iterator().next());
                    currentLoad = currentLoad - 1;
                }
            }
            cache.put(key, value);
            currentLoad = currentLoad + 1;
        }
    }

    @Override
    public void delete(String key) {
        synchronized (cache) {
            cache.remove(key);
            currentLoad = currentLoad - 1;
        }
    }

    @Override
    public boolean inCache(String key) {
        synchronized (cache) {
            Set cacheSet = cache.keySet();
            if (cacheSet.contains(key) == true){
                return true;
            }
            else{
                return false;
            }
        }
    }

    /**
     * Clear all load in cache
     */
    @Override
    public void clearCache() {
        cache.clear();
        // Reset current load
        currentLoad = 0;
    }

    @Override
    public int getCurrentLoad() {
        return currentLoad;
    }

    @Override
    public int getCacheCapacity() {
        return cacheCapacity;
    }
}