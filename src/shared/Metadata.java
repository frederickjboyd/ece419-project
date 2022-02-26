package shared;

import java.math.BigInteger;
<<<<<<< HEAD

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
=======
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;
import java.security.DigestException;

public class Metadata {
    /*
     * set up dynamic array as metadata
     */
    public String address;
    public Integer port;
    public BigInteger start;
    public BigInteger end;

    //creating a constructor of the class that initializes the values  
    public Metadata(String _address, Integer _port, BigInteger _start, BigInteger _end)   
    {   
        _address = _address;
        _port = _port;
        _start = _start;
        _end = _end;   
    }  
}
>>>>>>> origin/m2-client-1
