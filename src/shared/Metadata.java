package shared;

import java.math.BigInteger;
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