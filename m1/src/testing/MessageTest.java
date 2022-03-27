package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import shared.communication.KVMessage;
import shared.communication.IKVMessage.StatusType;

public class MessageTest extends TestCase {
    @Test
    public void testLongKey() {
        StatusType status = StatusType.GET;
        String key = "a";
        String value = "";
        KVMessage msg = null;
        Exception ex = null;

        try {
            // Max key size should be 20b
            msg = new KVMessage(status, key.repeat(21), value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex != null && msg == null);
    }

    @Test
    public void testLongValue() {
        StatusType status = StatusType.PUT;
        String key = "a";
        String value = "b";
        KVMessage msg = null;
        Exception ex = null;

        try {
            // Max value size should be 128kB
            msg = new KVMessage(status, key, value.repeat(1024 * 121));
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex != null && msg == null);
    }

    @Test
    public void testNonASCIIKey() {
        StatusType status = StatusType.GET;
        String key = "天上太阳红呀红彤彤诶";
        String value = "";
        KVMessage msg = null;
        Exception ex = null;

        try {
            msg = new KVMessage(status, key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex != null && msg == null);
    }

    @Test
    public void testNonASCIIValue() {
        StatusType status = StatusType.PUT;
        String key = "a";
        String value = "Союз нерушимый республик свободных"
                .concat("Сплотила навеки Великая Русь.")
                .concat("Да здравствует созданный волей народов")
                .concat("Единый, могучий Советский Союз!");
        KVMessage msg = null;
        Exception ex = null;

        try {
            msg = new KVMessage(status, key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex != null && msg == null);
    }
}
