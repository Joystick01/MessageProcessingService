package dev.knoepfle;

import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ConfigurationManager {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(ConfigurationManager.class);

    private static ConfigurationManager instance;
    private final Map<String, Object> configuration = new HashMap<>();

    private final String[] parameters = {
        "APPLICATION_ID_CONFIG",
        "BOOTSTRAP_SERVERS_CONFIG",
        "INPUT_TOPIC",
        "OUTPUT_TOPIC"
    };

    private ConfigurationManager() {
        for (String parameter : parameters) {
            configuration.put(parameter, System.getenv(parameter));
        }

        logger.info("Configuration loaded: {}", configuration);
    }

    public static synchronized ConfigurationManager getInstance() {
        if (instance == null) {
            instance = new ConfigurationManager();
        }
        return instance;
    }

    public String getString(String key) {
        if (configuration.get(key) == null) {
            throw new IllegalArgumentException("Configuration key not found: " + key);
        }
        return (String) configuration.get(key);
    }

    public int getInt(String key) {
        if (configuration.get(key) == null) {
            throw new IllegalArgumentException("Configuration key not found: " + key);
        }
        return Integer.parseInt((String) configuration.get(key));
    }
}