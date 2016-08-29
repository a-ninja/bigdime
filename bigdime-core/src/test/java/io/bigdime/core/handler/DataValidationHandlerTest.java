/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;

public class DataValidationHandlerTest {

	// @Test
	public void testBuild() throws AdaptorConfigurationException {
		// DataValidationHandler dataValidationHandler = new
		// DataValidationHandler();
		// dataValidationHandler.build();
	}

	@Test(expectedExceptions = ValidationHandlerException.class, expectedExceptionsMessageRegExp = "validation failed, null ActionEvent found in HandlerContext")
	public void testProcessForNullActionEvent() throws HandlerException {
		DataValidationHandler dataValidationHandler = new DataValidationHandler();
		HandlerContext.get().createSingleItemEventList(null);
		Assert.assertNull(dataValidationHandler.process());
	}

	@Test
	public void testProcess() throws HandlerException, DataValidationException {
		DataValidationHandler dataValidationHandler = new DataValidationHandler();
		List<Validator> validators = new ArrayList<>();
		Validator validator = Mockito.mock(Validator.class);
		validators.add(validator);
		Mockito.when(validator.validate(Mockito.any(ActionEvent.class)))
				.thenReturn(Mockito.mock(ValidationResponse.class));
		@SuppressWarnings("unchecked")
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
		ReflectionTestUtils.setField(dataValidationHandler, "runtimeInfoStore", runtimeInfoStore);
		ReflectionTestUtils.setField(dataValidationHandler, "validators", validators);

		HandlerContext handlerContext = HandlerContext.get();
		List<ActionEvent> actionEvents = new ArrayList<>();
		ActionEvent validateThisEvent = Mockito.mock(ActionEvent.class);
		actionEvents.add(validateThisEvent);
		handlerContext.setEventList(actionEvents);
		dataValidationHandler.process();
		Mockito.verify(validator, Mockito.times(1)).validate(validateThisEvent);
	}

	/**
	 * If the Validator throws a DataValidationException, process method must
	 * wrap it up in a ValidationHandlerException.
	 * 
	 * @throws HandlerException
	 * @throws DataValidationException
	 */

	@Test(expectedExceptions = ValidationHandlerException.class, expectedExceptionsMessageRegExp = "validation failed, validator_name=.*")
	public void testProcessWithValidatorThrowsDataValidationException()
			throws HandlerException, DataValidationException {
		DataValidationHandler dataValidationHandler = new DataValidationHandler();
		List<Validator> validators = new ArrayList<>();
		Validator validator = Mockito.mock(Validator.class);
		validators.add(validator);
		Mockito.when(validator.validate(Mockito.any(ActionEvent.class)))
				.thenThrow(Mockito.mock(DataValidationException.class));
		ReflectionTestUtils.setField(dataValidationHandler, "validators", validators);
		HandlerContext.get().createSingleItemEventList(Mockito.mock(ActionEvent.class));
		dataValidationHandler.process();
		Mockito.verify(validator, Mockito.times(1)).validate(Mockito.any(ActionEvent.class));
	}

	/**
	 * If the Validator throws a RuntimeException, process method must wrap it
	 * up in a HandlerException.
	 * 
	 * @throws HandlerException
	 * @throws DataValidationException
	 */
	@Test(expectedExceptions = HandlerException.class)
	public void testProcessWithValidatorThrowsRuntimeException() throws HandlerException, DataValidationException {
		DataValidationHandler dataValidationHandler = new DataValidationHandler();
		List<Validator> validators = new ArrayList<>();
		Validator validator = Mockito.mock(Validator.class);
		validators.add(validator);
		Mockito.when(validator.validate(Mockito.any(ActionEvent.class)))
				.thenThrow(Mockito.mock(RuntimeException.class));
		ReflectionTestUtils.setField(dataValidationHandler, "validators", validators);
		HandlerContext.get().createSingleItemEventList(Mockito.mock(ActionEvent.class));
		dataValidationHandler.process();
		Mockito.verify(validator, Mockito.times(1)).validate(Mockito.any(ActionEvent.class));
	}

	@Test
	public void testProcessWithValidatorReturnsNull() throws HandlerException, DataValidationException {
		DataValidationHandler dataValidationHandler = new DataValidationHandler();
		List<Validator> validators = new ArrayList<>();
		Validator validator = Mockito.mock(Validator.class);
		validators.add(validator);
		ValidationResponse validationResponse = new ValidationResponse();
		validationResponse.setValidationResult(ValidationResult.NOT_READY);
		Mockito.when(validator.validate(Mockito.any(ActionEvent.class))).thenReturn(validationResponse).thenReturn(validationResponse);
		ReflectionTestUtils.setField(dataValidationHandler, "validators", validators);
		HandlerContext.get().createSingleItemEventList(Mockito.mock(ActionEvent.class));
		dataValidationHandler.process();
		// no asserts, no exceptions.
	}

}
