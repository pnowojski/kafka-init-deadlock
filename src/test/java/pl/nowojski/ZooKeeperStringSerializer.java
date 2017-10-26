package pl.nowojski;

import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.nio.charset.StandardCharsets;

/**
 * Created by pnowojski on 26/10/2017.
 */
public class ZooKeeperStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) {
        if (data instanceof String) {
            return ((String) data).getBytes(StandardCharsets.UTF_8);
        } else {
            throw new IllegalArgumentException("ZooKeeperStringSerializer can only serialize strings.");
        }
    }

    @Override
    public Object deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }
}
