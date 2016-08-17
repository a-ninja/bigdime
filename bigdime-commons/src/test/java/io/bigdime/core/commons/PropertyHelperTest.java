/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PropertyHelperTest {
	/**
	 * @formatter:off
	 * Assert that getInstance method returns a not null object. 
	 * Also assert that invoking the getInstance method returns same objects.
	 * @formatter:on
	 */
	@Test
	public void testGetInstance() {
		Assert.assertNotNull(PropertyHelper.getInstance());
		Assert.assertSame(PropertyHelper.getInstance(), PropertyHelper.getInstance());
	}

	/**
	 * Assert that if the map has a property with integer value, PropertyHelper
	 * returns it's correct value.
	 */
	@Test
	public void testGetIntProperty() {
		Map<String, Object> propertyMap = new HashMap<>();
		Random rand = new Random();
		int number = rand.nextInt();
		propertyMap.put("unit-test-testGetIntProperty", number);
		int actualValue = PropertyHelper.getIntProperty(propertyMap, "unit-test-testGetIntProperty", rand.nextInt());
		Assert.assertEquals(actualValue, number);
	}

	@Test(expectedExceptions = NumberFormatException.class)
	public void testGetIntPropertyFromStringObject() {
		Object value = "";
		PropertyHelper.getIntProperty(value);
	}

	@Test
	public void testGetIntPropertyFromIntegerObject() {
		Random rand = new Random();
		int number = rand.nextInt();
		Object value = String.valueOf(number);
		int actualValue = PropertyHelper.getIntProperty(value);
		Assert.assertEquals(actualValue, number);
	}

	/**
	 * Assert PropertyHelper returns a default value of a property if the
	 * property being asked for does not exist in the map.
	 */
	@Test
	public void testGetIntPropertyWithPropertyNotSet() {
		Map<String, Object> propertyMap = new HashMap<>();
		Random rand = new Random();
		int number = rand.nextInt();
		int actualValue = PropertyHelper.getIntProperty(propertyMap, "unit-test-testGetIntPropertyWithPropertyNotSet",
				number);
		Assert.assertEquals(actualValue, number);
	}

	/**
	 * Assert PropertyHelper throws a RuntimeException if the being asked for
	 * does not exist in the map.
	 */
	@Test(expectedExceptions = RuntimeException.class)
	public void testGetIntPropertyWithNoDefaultValue() {
		Map<String, Object> propertyMap = new HashMap<>();
		PropertyHelper.getIntProperty(propertyMap, "unit-test-testGetIntPropertyWithNoDefaultValue");
	}

	/**
	 * Assert that if the map has a property with long value, PropertyHelper
	 * returns it's correct value.
	 */
	@Test
	public void testGetLongProperty() {
		Map<String, Object> propertyMap = new HashMap<>();
		Random rand = new Random();
		long number = rand.nextLong();
		propertyMap.put("unit-test-testGetLongProperty", number);
		long actualValue = PropertyHelper.getLongProperty(propertyMap, "unit-test-testGetLongProperty",
				rand.nextLong());
		Assert.assertEquals(actualValue, number);

		number = rand.nextInt();
		actualValue = PropertyHelper.getLongProperty(propertyMap, "unit-test-key1", number);
		Assert.assertEquals(actualValue, number);
	}

	/**
	 * Assert that if the map has a property, PropertyHelper returns it's
	 * correct value.
	 */
	@Test
	public void testGetStringProperty() {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("unit-test-testGetStringProperty", "unit-test-value");
		String actualValue = PropertyHelper.getStringProperty(propertyMap, "unit-test-testGetStringProperty");
		Assert.assertEquals(actualValue, "unit-test-value");
	}

	/**
	 * Assert that if the map has a property, PropertyHelper returns it's
	 * correct value.
	 */
	@Test
	public void testGetStringPropertyWithPropertyNotSet() {
		Map<String, Object> propertyMap = new HashMap<>();
		String actualValue = PropertyHelper.getStringProperty(propertyMap,
				"unit-test-testGetStringPropertyWithPropertyNotSet");
		Assert.assertNull(actualValue);
	}

	@Test
	public void testGetStringPropertyWithDefaultValue() {
		Map<String, Object> propertyMap = new HashMap<>();
		String actualValue = PropertyHelper.getStringProperty(propertyMap,
				"unit-test-testGetStringPropertyWithDefaultValue-name",
				"unit-test-testGetStringPropertyWithDefaultValue-value");
		Assert.assertEquals(actualValue, "unit-test-testGetStringPropertyWithDefaultValue-value");
	}

	@Test
	public void testGetStringPropertyWithValue() {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("unit-test-testGetStringPropertyWithValue-name",
				"unit-test-testGetStringPropertyWithValue-value");
		String actualValue = PropertyHelper.getStringProperty(propertyMap,
				"unit-test-testGetStringPropertyWithValue-name", "unit-test-testGetStringPropertyWithValue-value");
		Assert.assertEquals(actualValue, "unit-test-testGetStringPropertyWithValue-value");
	}

	/**
	 * Assert that NumberFormatException is thrown if the input object can't be
	 * converted to long.
	 */
	@Test(expectedExceptions = NumberFormatException.class)
	public void testGetLongPropertyFromStringObject() {
		Object value = "";
		PropertyHelper.getLongProperty(value);
	}

	/**
	 * Assert that correct value if returned if the input object contains a long
	 * value.
	 */
	@Test
	public void testGetLongPropertyFromLongObject() {
		Random rand = new Random();
		long number = rand.nextLong();
		Object value = String.valueOf(number);
		long actualValue = PropertyHelper.getLongProperty(value);
		Assert.assertEquals(actualValue, number);
	}

	/**
	 * Assert that NumberFormatException is thrown if the input object can't be
	 * converted to long.
	 */
	@Test
	public void testGetLongPropertyFromStringObjectWithDefaultValue() {
		Object value = "";
		Random rand = new Random();
		long number = rand.nextLong();
		PropertyHelper.getLongProperty(value, number);
	}

	@Test
	public void testGetBooleanPropertyWithNoValue() {
		Map<String, Object> propertyMap = new HashMap<>();
		Assert.assertFalse(PropertyHelper.getBooleanProperty(propertyMap, "unit-test-testGetBooleanProperty"));
	}

	@Test
	public void testGetBooleanPropertyWithTrue() {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("unit-test-testGetBooleanPropertyWithTrue", "true");
		Assert.assertTrue(PropertyHelper.getBooleanProperty(propertyMap, "unit-test-testGetBooleanPropertyWithTrue"));
	}

	@Test
	public void testGetBooleanPropertyWithFalse() {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("unit-test-testGetBooleanPropertyWithFalse", "false");
		Assert.assertFalse(PropertyHelper.getBooleanProperty(propertyMap, "unit-test-testGetBooleanPropertyWithFalse"));
	}

	@Test
	public void testGetBooleanPropertyWithInvalidValue() {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("unit-test-testGetBooleanPropertyWithInvalidValue",
				"unit-test-testGetBooleanPropertyWithInvalidValue");
		Assert.assertFalse(
				PropertyHelper.getBooleanProperty(propertyMap, "unit-test-testGetBooleanPropertyWithInvalidValue"));
	}

	@Test
	public void testRedeemTokensFromAppProperties() {
		Map<String, Object> propertyMap = new HashMap<>();
		Properties applicationProperties = new Properties();

		Map<String, String> yarnConfs = new HashMap<>();
		yarnConfs.put("fs.defaultFS", "${fs.defaultFS}");
		yarnConfs.put("ipc.client.connect.max.retries", "${ipc.client.connect.max.retries}");
		yarnConfs.put("yarn.resourcemanager.principal", "${yarn.resourcemanager.principal}");

		propertyMap.put("yarn-confs", yarnConfs);
		propertyMap.put("hive-jdbc-user-name", "unit-user");

		applicationProperties.put("yarn.resourcemanager.principal", "unit-yarn.resourcemanager.principal");
		applicationProperties.put("ipc.client.connect.max.retries", "87");
		applicationProperties.put("hive-jdbc-user-name", "unit-user");
		PropertyHelper.redeemTokensFromAppProperties(propertyMap, applicationProperties);

		Assert.assertEquals(PropertyHelper.getStringProperty(propertyMap, "hive-jdbc-user-name"), "unit-user");
		Assert.assertEquals(
				PropertyHelper.getMapProperty(propertyMap, "yarn-confs").get("yarn.resourcemanager.principal"),
				"unit-yarn.resourcemanager.principal");
		Assert.assertEquals(
				PropertyHelper.getMapProperty(propertyMap, "yarn-confs").get("ipc.client.connect.max.retries"), "87");

	}

	@Test
	public void testGetPropertyFromPropertiesOrSrcDesc() {
		Map<String, Object> propertyMap = new HashMap<>();
		Map<String, Object> srcDescMap = new HashMap<>();

		propertyMap.put("unit-test-testGetPropertyFromPropertiesOrSrcDesc",
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc-fromProperty");
		srcDescMap.put("unit-test-testGetPropertyFromPropertiesOrSrcDesc",
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc-fromSrcDesc");

		String actualValue = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(propertyMap, srcDescMap,
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc",
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc-default");
		String expectedValue = "unit-test-testGetPropertyFromPropertiesOrSrcDesc-fromSrcDesc";
		Assert.assertEquals(actualValue, expectedValue);
	}

	@Test
	public void testGetPropertyFromPropertiesOrSrcDescWithNoValueInSrcDesc() {
		Map<String, Object> propertyMap = new HashMap<>();
		Map<String, Object> srcDescMap = new HashMap<>();

		propertyMap.put("unit-test-testGetPropertyFromPropertiesOrSrcDesc",
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc-fromProperty");

		String actualValue = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(propertyMap, srcDescMap,
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc",
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc-default");
		String expectedValue = "unit-test-testGetPropertyFromPropertiesOrSrcDesc-fromProperty";
		Assert.assertEquals(actualValue, expectedValue);
	}

	@Test
	public void testGetPropertyFromPropertiesOrSrcDescWithNullSrcDesc() {
		Map<String, Object> propertyMap = new HashMap<>();

		propertyMap.put("unit-test-testGetPropertyFromPropertiesOrSrcDesc",
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc-fromProperty");

		String actualValue = PropertyHelper.getStringPropertyFromPropertiesOrSrcDesc(propertyMap, null,
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc",
				"unit-test-testGetPropertyFromPropertiesOrSrcDesc-default");
		String expectedValue = "unit-test-testGetPropertyFromPropertiesOrSrcDesc-fromProperty";
		Assert.assertEquals(actualValue, expectedValue);
	}

}
