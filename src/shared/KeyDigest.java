package shared;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.MessageDigest;
import java.security.DigestException;

public final class KeyDigest {
    /*
     * get the hash value of a key
     */
    public static BigInteger getHashedKey(String key) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try {
            // byte[] byteKey = key.getBytes("UTF-8"); // 128 bits so need bigint long is 64
            byte[] byteKey = key.getBytes(); // 128 bits so need bigint long is 64

            md.update(byteKey);
            byte[] hexKey = md.digest();
            BigInteger hexkeyInt = new BigInteger(1, hexKey);
			logger.debug("hashkey generated");
            return hexkeyInt;

        } catch (NoSuchAlgorithmException e) {
			logger.error("No Such Algorithm Exception occured when trying to create hash for key!", key);
			throw new NoSuchAlgorithmException();
		}
    }
}