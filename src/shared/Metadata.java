package shared;

import java.math.BigInteger;

public class Metadata {
    private String host;
    private Integer port;
    private BigInteger hashStart;
    private BigInteger hashStop;

    public Metadata(String host, Integer port, BigInteger hashStart, BigInteger hashStop) {
        this.host = host;
        this.port = port;
        this.hashStart = hashStart;
        this.hashStop = hashStop;
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
}
