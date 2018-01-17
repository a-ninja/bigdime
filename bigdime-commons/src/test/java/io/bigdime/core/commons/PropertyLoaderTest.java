package io.bigdime.core.commons;

import java.io.IOException;
import java.io.InputStream;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PropertyLoaderTest {

	@InjectMocks
	PropertyLoader propertyLoader;

	@Mock
	ClassLoader classLoader;

	@BeforeMethod
	public void init() {
		System.out.println("init");
		MockitoAnnotations.initMocks(this);
		System.out.println("propertyLoader=" + propertyLoader);
		System.out.println("classLoader=" + classLoader);
	}

	@Test
	public void testLoadEnvPropertiesWithNullClassLoader() throws IOException {
		ReflectionTestUtils.setField(propertyLoader, "loader", null);
		propertyLoader.loadEnvProperties(null);
	}

	@Test
	public void testLoadEnvPropertiesWithDefaultProperties() throws IOException {
		Mockito.when(classLoader.getResourceAsStream(Mockito.anyString())).thenReturn(Mockito.mock(InputStream.class));
		propertyLoader.loadEnvProperties(null);
	}

	@Test
	public void testLoadEnvProperties() throws IOException {
		ReflectionTestUtils.setField(propertyLoader, "loader", PropertyLoader.class.getClassLoader());
		System.setProperty("env.properties", "application.properties");
		propertyLoader.loadEnvProperties("env.properties");
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testLoadEnvPropertiesWithInvalidPropertiesFile() throws IOException {
		System.setProperty("env.properties", "application.properties1");
		propertyLoader.loadEnvProperties("env.properties");
	}

	/**
	 * If the InputStream throws an IOException, PropertyLoader should fail.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IOException.class)
	public void testLoadEnvPropertiesWithIOException() throws IOException {
		InputStream is = Mockito.mock(InputStream.class);
		Mockito.when(classLoader.getResourceAsStream(Mockito.anyString())).thenReturn(is);
		Mockito.when(is.read(Mockito.any(byte[].class))).thenThrow(new IOException());
		propertyLoader.loadEnvProperties("env.properties");
	}

	/**
	 * If the InputStream throws an IllegalArgumentException, PropertyLoader
	 * should fail.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testLoadEnvPropertiesWithIllegalArgumentException() throws IOException {
		InputStream is = Mockito.mock(InputStream.class);
		Mockito.when(classLoader.getResourceAsStream(Mockito.anyString())).thenReturn(is);
		Mockito.when(is.read(Mockito.any(byte[].class))).thenThrow(new IllegalArgumentException());

		System.setProperty("env.properties", "application.properties");
		propertyLoader.loadEnvProperties("env.properties");
	}

	/**
	 * If the ClassLoader can't load the resource, PropertyLoader should fail.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testLoadEnvPropertiesWithClassLoaderReturnsNull() throws IOException {
		Mockito.when(classLoader.getResourceAsStream(Mockito.anyString())).thenReturn(null);
		propertyLoader.loadEnvProperties("env.properties");
	}

}
