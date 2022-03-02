package app_kvServer.kvCache;

import app_kvServer.kvCache.kvCacheTypes;
import java.util.*;
import java.util.Collections;

public class LFUCache extends kvCacheTypes{
    // Frequency list for cache entries
    private Map<String, Integer> frequency;
    // Cache map itself
    private Map<String, String> cache;

    /**
     * Initialize FIFO cache
     * @param size desired cache size
     */
    public LFUCache(int size) {
        super(size);

        // Maintain frequency of accesses
        this.frequency = Collections.synchronizedMap(new TreeMap<String, Integer>());
        // Access order = true rather than insertion order
        this.cache = Collections.synchronizedMap(new LinkedHashMap<String, String>(size, 0.75f, true));
    }

    @Override
    public String read(String key) {
        synchronized (cache) {
            synchronized (frequency){
                if (cache.containsKey(key)) {
                    int tempFreq = frequency.get(key) + 1;
                    // Accumulate another access on this key
                    frequency.put(key, tempFreq);
                    return cache.get(key);
                }
                else {
                    return null;
                }
            }
        }
    }

    /**
     * Sort Treemap helper
     * See https://stackoverflow.com/questions/2864840/treemap-sort-by-value
     * @param <K> Key   
     * @param <V> Value
     * @param map
     * @return Map sorted by frequency
     */
    public static <K, V extends Comparable<V>> Map<K, V> entriesSortedByValues(final Map<K, V> map) {
        // Comparator for comparing frequencies in map
        Comparator<K> comparison = new Comparator<K>() {
                    public int compare(K key1, K key2) {
                        int compare = map.get(key1).compareTo(map.get(key2));
                        if (compare == 0){
                            return 1;
                        }
                        else{
                            return compare;
                        }
                    }
                };
        Map<K, V> sortedFreq = new TreeMap<K, V>(comparison);
        sortedFreq.putAll(map);
        return sortedFreq;
    }


    @Override
    public void write(String key, String value) {
        synchronized (cache) {
            synchronized (frequency){
                // We are full, kick out least frequently used entry
                if (currentLoad == cacheCapacity) {
                    synchronized (cache) {
                        Map sortedByFrequency = entriesSortedByValues(frequency);
                        Map.Entry lowestFreqEntry = (Map.Entry) sortedByFrequency.entrySet().iterator().next();
                        // Remove first entry
                        cache.remove(lowestFreqEntry.getKey());
                        frequency.remove(lowestFreqEntry.getKey());
                        currentLoad = currentLoad - 1;
                    }
                }
                cache.put(key, value);
                // First occurence of new key
                frequency.put(key, 1);
                currentLoad = currentLoad + 1;
            }
        }
    }

    @Override
    public void delete(String key) {
        synchronized (cache) {
            synchronized (frequency){
                cache.remove(key);
                frequency.remove(key);
                currentLoad = currentLoad - 1;
            }
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
        frequency.clear();
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