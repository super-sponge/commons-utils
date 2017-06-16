package commons;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.EnvironmentConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Created by sponge on 2017/6/9.
 */
public class Config {

    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    private static String CONF_DIR_KEY = "CONF_DIR";
    private static String DEFAULT_CONF_DIR_VALUE= "conf";
    private static String HBASE_CONF="hbase-client.properties";

    private static String confDir = null;

    private static EnvironmentConfiguration envProperties = new EnvironmentConfiguration();



    public static String getConfDir() {
        if (confDir == null) {
            confDir= envProperties.getString(CONF_DIR_KEY, DEFAULT_CONF_DIR_VALUE);
        }
        return confDir;
    }

    public static void setConfDir(String confDir) {
        Config.confDir = confDir;
    }

    public static Properties getHbaseProperties() {
        return getProperties(HBASE_CONF);
    }

    public static Properties getProperties(String confFile){
        PropertiesConfiguration properties = new PropertiesConfiguration();
        Properties props = new Properties();
        String confPath = getConfDir() + File.separator + confFile;
        try {
            properties.load(confPath);
            Iterator<String> iter = properties.getKeys();
            while(iter.hasNext()) {
                String key = iter.next();
                if("hbase.zookeeper.quorum".equals(key)) {
                    List<Object> values =  properties.getList(key);
                    String value ="";
                    for(Object item : values) {
                        value = (String) item + ","  + value;
                    }
                    props.setProperty(key, value.substring(0, value.length() - 1));
                } else {
                    props.setProperty(key, properties.getString(key));
                }
            }

        } catch (ConfigurationException e) {
            e.printStackTrace();
            logger.error("Load " + confPath + " Error" + e.getMessage());

        }

        return props;
    }
}
