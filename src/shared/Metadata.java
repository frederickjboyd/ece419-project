package shared;

import java.math.BigInteger;

import app_kvServer.IKVServer.CacheStrategy;
import ecs.ECSNode;

/**
 * Server-specific metadata.
 */
public class Metadata {
    private String host;
    private Integer port;
    private BigInteger hashStart;
    private BigInteger hashStop;
    private CacheStrategy cacheStrategy = CacheStrategy.None;
    private int cacheSize = 0;
    private ECSNode prevNode;
    private ECSNode nextNode;

    /**
     * Instantiate without cache information.
     * 
     * @param host
     * @param port
     * @param hashStart
     * @param hashStop
     */
    public Metadata(String host, Integer port, BigInteger hashStart, BigInteger hashStop) {
        this.host = host;
        this.port = port;
        this.hashStart = hashStart;
        this.hashStop = hashStop;
    }

    /**
     * Instantiate with cache information.
     * 
     * @param host
     * @param port
     * @param hashStart
     * @param hashStop
     * @param cacheStrategy
     * @param cacheSize
     */
    public Metadata(String host, Integer port, BigInteger hashStart, BigInteger hashStop, CacheStrategy cacheStrategy,
            int cacheSize, ECSNode prevNode, ECSNode nextNode) {
        this.host = host;
        this.port = port;
        this.hashStart = hashStart;
        this.hashStop = hashStop;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.prevNode = prevNode;
        this.nextNode = nextNode;
    }

    public String getHost() {
        return this.host;
    }

    public Integer getPort() {
        return this.port;
    }

    public BigInteger getHashStart() {
        return this.hashStart;
    }

    public BigInteger getHashStop() {
        return this.hashStop;
    }

    public CacheStrategy getCacheStrategy() {
        return this.cacheStrategy;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public ECSNode getPrevNode() {
        return this.prevNode;
    }

    public ECSNode getNextNode() {
        return this.nextNode;
    }
}
