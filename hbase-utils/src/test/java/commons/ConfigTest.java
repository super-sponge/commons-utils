package commons;

import junit.framework.TestCase;

import java.util.Map;
import java.util.Properties;

/**
 * Created by sponge on 2017/6/15.
 */
public class ConfigTest extends TestCase {
    public void testGetHbaseProperties() throws Exception {
        Config.setConfDir("src/main/conf");
        Properties props = Config.getHbaseProperties();
        for(Map.Entry<Object, Object>  entry : props.entrySet()) {
            String key = (String) entry.getKey();
            String value  = (String) entry.getValue();
            System.out.println("key : " + key + " value : " + value);
        }
    }

}