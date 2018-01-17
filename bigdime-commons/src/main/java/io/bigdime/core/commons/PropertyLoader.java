package io.bigdime.core.commons;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier("propertyLoader")
public class PropertyLoader {

    @Autowired(required = false)
    private ClassLoader loader;

    public Properties loadEnvProperties(String envPropertyName) throws IOException {
        String envProperties = null;

        if (envPropertyName != null) {
            envProperties = System.getProperty(envPropertyName);
        }

        System.out.println(getClass().getCanonicalName() + ", envProperties=:" + envProperties + ":");
        if (envProperties == null) {
            envProperties = "application.properties";
        }
        System.out.println(getClass().getCanonicalName() + ", envProperties=" + envProperties);
        if (loader == null) {
            loader = PropertyLoader.class.getClassLoader();
        }
        try (final InputStream is = loader.getResourceAsStream(envProperties)) {
            if (is == null) {
                throw new IllegalArgumentException("unable to find properties:" + envProperties + ":");
            }
            Properties props = new Properties();
            props.load(is);
            return props;
        }
    }
}
