package app_kvServer.kvCache;

public abstract class kvCacheTypes {
    public int cacheCapacity;
	public int currentLoad = 0;

	public kvCacheTypes (int size) {
        // Keep track of current cache load
        this.currentLoad = 0;
        // Max capacity of cache
        this.cacheCapacity = size;
	}

	public abstract String read(String key);
	public abstract void write(String key, String value);
	public abstract void delete(String key);
    public abstract void clearCache();
	public abstract boolean inCache(String key);
    public abstract int getCurrentLoad();
	public abstract int getCacheCapacity();
}
