/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;

@Configuration
@ContextConfiguration({ "classpath*:META-INF/application-context-validator-test.xml" })
@Test(singleThreaded = true)
public class ValidatorFactoryTest extends AbstractTestNGSpringContextTests {

	@Autowired
	ValidatorFactory validatorFactory;
	@Autowired
	private List<Validator> v;

	@BeforeMethod
	public void setUp() {

	}

	@Test
	public void testComponent() {
		System.out.println("validatorFactory=" + validatorFactory + ", v=" + v);

	}

	@Test
	public void testGetValidator()
			throws IllegalAccessException, InstantiationException, AdaptorConfigurationException {
		Validator _dummyValidator = validatorFactory.getValidator("_dummy");
		Assert.assertTrue(_dummyValidator.getClass() == DummyValidator.class);
	}

	/**
	 * If there is no validator available for a given type,
	 * AdaptorConfigurationException must be thrown.
	 * 
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testGetValidatorForInvalidType()
			throws IllegalAccessException, InstantiationException, AdaptorConfigurationException {
		validatorFactory.getValidator("unit-type");
	}

	/**
	 * If there are more than one validators available for a given validation
	 * type, AdaptorConfigurationException must be thrown.
	 * 
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws AdaptorConfigurationException
	 */
	@Test(expectedExceptions = AdaptorConfigurationException.class)
	public void testGetValidatorForDuplicateValidators() throws Throwable {
		ValidatorFactory instance = validatorFactory;
		try {
			Map<String, Class<? extends Validator>> validators = new HashMap<>();
			Validator v = Mockito.mock(Validator.class);
			validators.put("_dummy", v.getClass());

			ReflectionTestUtils.setField(instance, "validators", validators);
			ReflectionTestUtils.invokeMethod(instance, "init");
		} catch (Exception e) {
			// reset the values for other tests
			Map<String, Class<? extends Validator>> validators = new HashMap<>();
			ReflectionTestUtils.setField(instance, "validators", validators);
			ReflectionTestUtils.invokeMethod(instance, "init");
			throw e.getCause();
		}
	}

	/**
	 * If the validator class can't be instantiated, ValidatorFactory must throw
	 * an exception.
	 * 
	 * @throws IllegalAccessException
	 * @throws NoSuchBeanDefinitionException
	 * @throws AdaptorConfigurationException
	 */
	@Test
	public void testGetValidatorWithNoSuchBeanDefinitionException() throws AdaptorConfigurationException {
		try {

			@Factory(id = "mock_validator_instantiation_exception", type = MockValidatorIllegalAccess.class)
			class MockValidatorIllegalAccess implements Validator {
				@Override
				public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {
					return null;
				}

				@Override
				public String getName() {
					return null;
				}
			}
			validatorFactory.getValidator("mock_validator_instantiation_exception");
			Assert.fail("must throw an AdaptorConfigurationException");
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(e.getCause() instanceof NoSuchBeanDefinitionException);
		}
	}
}
