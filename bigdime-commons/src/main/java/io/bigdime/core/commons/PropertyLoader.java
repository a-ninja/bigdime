package io.bigdime.core.commons;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyLoader {

	public Properties loadEnvProperties(String envPropertyName) throws IOException {
		String envProperties = System.getProperty(envPropertyName);
		System.out.println("envProperties=" + envProperties);
		if (envProperties == null) {
			envProperties = "application.properties";
		}
		System.out.println("envProperties=" + envProperties);
		try (final InputStream is = PropertyLoader.class.getClassLoader().getResourceAsStream(envProperties)) {
			Properties props = new Properties();
			props.load(is);
			return props;
		}
	}
}
