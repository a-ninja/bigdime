/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to get the property from the map.
 * 
 * @author Neeraj Jain
 *
 */
public final class PropertyHelper {
	private static final Logger logger = LoggerFactory.getLogger(PropertyHelper.class);
	private static final PropertyHelper instance = new PropertyHelper();

	private PropertyHelper() {
	}

	public static PropertyHelper getInstance() {
		return instance;
	}

	/**
	 * Get the property from propertyMap return an int value for it. If the
	 * property does not exist, throw a RuntimeException.
	 * 
	 * @param propertyMap
	 *            map to for the property in
	 * @param name
	 *            name of the property
	 * @return int value of of the property if the property is found and has a
	 *         value in int range.
	 * @throws RuntimeException
	 *             if the property with name specified by argument name does not
	 *             exist in propertyMap.
	 */
	public static int getIntProperty(Map<? extends String, ? extends Object> propertyMap, String name) {
		return getIntProperty(propertyMap.get(name));
	}

	/**
	 * Get the property from propertyMap return an integer value for it.
	 * 
	 * @param propertyMap
	 *            map to look for the property in
	 * @param name
	 *            name of the property to look for
	 * @param defaultValue
	 *            value to be returned in case property with name argument is
	 *            not present in the map
	 * @return integer value of the property if the property is found and has a
	 *         value in integer range. Returns value specified by defalutValue
	 *         argument if the property with name argument doesn't exist.
	 * @throws NumberFormatException
	 *             if the property's value cannot be parsed as an integer.
	 */
	public static int getIntProperty(Map<? extends String, ? extends Object> propertyMap, String name,
			int defaultValue) {
		return getIntProperty(propertyMap.get(name), defaultValue);
	}

	/**
	 * Get the integer value of Object value and return the same. If the value
	 * is null or is not a number, the default value is returned.
	 * 
	 * @param value
	 *            Object with integer value
	 * @param defaultValue
	 *            value to be returned if the value argument is null or not a
	 *            number
	 * @return
	 */
	public static int getIntProperty(Object value, int defaultValue) {
		try {
			return getIntProperty(value);
		} catch (Exception ex) {
			return defaultValue;
		}
	}

	/**
	 * Get the integer value of Object value and return the same. If the value
	 * is null or is not a number, an exception is thrown.
	 * 
	 * @param value
	 *            Object with integer value
	 */
	public static int getIntProperty(Object value) {
		if (value == null) {
			throw new RuntimeException("value is null");
		} else {
			return Integer.valueOf(value.toString());
		}
	}

	/**
	 * Get the long value of Object value and return the same. If the value is
	 * null or is not a number, the default value is returned.
	 * 
	 * @param value
	 *            Object with long value
	 * @param defaultValue
	 *            value to be returned if the value argument is null or not a
	 *            number
	 * @return
	 */
	public static long getLongProperty(Object value, long defaultValue) {
		try {
			return getLongProperty(value);
		} catch (Exception ex) {
			return defaultValue;
		}
	}

	/**
	 * Get the long value of Object value and return the same. If the value is
	 * null or is not a number, an exception is thrown.
	 * 
	 * @param value
	 *            Object with long value
	 */
	public static long getLongProperty(Object value) {
		if (value == null) {
			throw new RuntimeException("value is null");
		} else {
			return Long.valueOf(value.toString());
		}
	}

	/**
	 * Get the property from propertyMap return a long value for it.
	 * 
	 * @param propertyMap
	 *            map to look for the property in
	 * @param name
	 *            name of the property to look for
	 * @param defaultValue
	 *            value to be returned in case property with name argument is
	 *            not present in the map
	 * @return long value of the property if the property is found and has a
	 *         value in long range. Returns value specified by defalutValue
	 *         argument if the property with name argument doesn't exist.
	 * @throws NumberFormatException
	 *             if the property's value cannot be parsed as a long.
	 */
	public static long getLongProperty(Map<? extends String, ? extends Object> propertyMap, String name,
			long defaultValue) {
		final Object value = propertyMap.get(name);
		return getLongProperty(value, defaultValue);
	}

	/**
	 * Get the property from propertyMap return it's value.
	 * 
	 * @param propertyMap
	 *            map to look for the property in
	 * @param name
	 *            name of the property to look for
	 * @return value of the property if the property is found, null otherwise
	 */
	public static String getStringProperty(Map<? extends String, ? extends Object> propertyMap, String name) {
		final Object value = propertyMap.get(name);
		if (value == null)
			return null;
		return String.valueOf(value);
	}

	/**
	 * Get the property from propertyMap return it's value,if it not found
	 * return the default value
	 * 
	 * @param propertyMap
	 *            map to look for the property in
	 * @param name
	 *            name of the property to look for
	 * @return value of the property if the property is found, null otherwise
	 */
	public static String getStringProperty(Map<? extends String, ? extends Object> propertyMap, String name,
			String defaultValue) {
		String value = getStringProperty(propertyMap, name);
		if (value == null)
			return defaultValue;
		else
			return value;
	}

	public static boolean getBooleanProperty(Map<? extends String, ? extends Object> propertyMap, String name) {
		final Object value = String.valueOf(propertyMap.get(name));
		if (value == null || value.equals("null"))
			return false;
		return Boolean.parseBoolean(String.valueOf(value));
	}

	@SuppressWarnings("rawtypes")
	public static Map getMapProperty(Map<? extends String, ? extends Object> propertyMap, String name) {
		final Object value = propertyMap.get(name);
		if (value == null || value.equals("null"))
			return null;
		if (value instanceof Map) {
			return (Map) value;
		} else {
			throw new IllegalArgumentException(
					name + " field in given map is not of type Map, rather is of type: " + value.getClass());
		}
	}

	public static void redeemTokensFromAppProperties(final Map<? super String, ? super Object> properties,
			final Properties applicationProperties) {
		for (Entry<? super String, ? super Object> property : properties.entrySet()) {
			logger.info("property_name=\"{}\" value=\"{}\" isString=\"{}\"", property.getKey(), property.getValue(),
					(property.getValue() instanceof String));
			if (property.getValue() instanceof String) {
				String propValue = property.getValue().toString();
				String newValue = StringHelper.getInstance().redeemToken(propValue, applicationProperties);
				if (!propValue.equals(newValue)) {
					property.setValue(newValue);
					logger.info("property_name=\"{}\" old_value=\"{}\" new_value=\"{}\"", property.getKey(), propValue,
							newValue);
				}
			}
		}
	}
}
