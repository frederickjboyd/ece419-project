package persistent_storage;

public interface IPersistentStorage {
    // Confirm put with bool
    boolean put(String key, String value) throws Exception;
    // If no val provided, delete key
    boolean delete(String key);
    // Return desired val based on key
    String get(String key) throws Exception;
    // Total storage wipe
    void wipeStorage();
    // Check if key exists in storage
    boolean existsCheck(String key);
}