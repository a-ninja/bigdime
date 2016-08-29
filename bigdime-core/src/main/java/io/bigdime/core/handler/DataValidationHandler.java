/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants.ValidationHandlerConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidatorFactory;

/**
 * @formatter:off
 * Data validation handler that uses one or more validators to perform
 * validation. data-validation-handler needs to be configured with
 * validation_type property to inject one or more validators; e.g.
 * "properties": {
 *    "validation_type" : "raw_checksum | record_count"
 * }
 * @formatter:on
 * 
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class DataValidationHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DataValidationHandler.class));

	@Autowired
	private ValidatorFactory validatorFactory;
	private List<Validator> validators = new ArrayList<>();

	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;

	/**
	 * Read the validation_type property, get corresponding Validators and add
	 * them to the list.
	 */
	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		setHandlerPhase("building validation handler");
		logger.debug(getHandlerPhase(), "property_map=\"{}\"", getPropertyMap());

		final String validationTypeProperty = (String) getPropertyMap()
				.get(ValidationHandlerConfigConstants.VALIDATION_TYPE);
		final String[] validationTypes = validationTypeProperty.split("\\|");
		for (String type : validationTypes) {
			type = type.trim();
			logger.debug(getHandlerPhase(), "validation_type=\"{}\"", type);
			validators.add(validatorFactory.getValidator(type.trim()));
		}
	}

	@Override
	public Status process() throws HandlerException {
		logger.debug("DataValidationHandler processing event", "DataValidationHandler processing event");
		List<ActionEvent> actionEvents = getHandlerContext().getEventList();
		Preconditions.checkNotNull(actionEvents, "eventList in HandlerContext can't be null");
		logger.debug("DataValidationHandler processing event", "actionEvents.size=\"{}\"", actionEvents.size());
		Preconditions.checkArgument(!actionEvents.isEmpty(), "eventList in HandlerContext can't be empty");
		ActionEvent actionEvent = actionEvents.get(0);
		process0(actionEvent);

		processLastHandler();
		return Status.READY;
	}

	private void process0(ActionEvent actionEvent) throws HandlerException {
		logger.debug("DataValidationHandler processing event", "actionEvent==null=\"{}\"", actionEvent == null);
		boolean validationPassed = true;
		ValidationResult validationReady = ValidationResult.NOT_READY;
		if (actionEvent == null) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.VALIDATION_ERROR, ALERT_SEVERITY.BLOCKER,
					"_message=\"validation failed, null ActionEvent found in HandlerContext\"");
			throw new ValidationHandlerException("validation failed, null ActionEvent found in HandlerContext");
		}
		for (final Validator validator : validators) {

			try {
				ValidationResponse validationResponse = validator.validate(actionEvent);
				validationReady = validationResponse.getValidationResult();
				if (validationReady != ValidationResult.NOT_READY) {
					validationPassed = validationPassed
							& (validationResponse.getValidationResult() == ValidationResult.PASSED);

					logger.debug("DataValidationHandler processing event", "received validation results", actionEvent);
				} else {
					logger.debug("DataValidationHandler processing event", "validation was skipped");
					break;
				}
			} catch (DataValidationException e) {
				logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.VALIDATION_ERROR, ALERT_SEVERITY.BLOCKER,
						"_message=\"validation failed\" validator_name=\"{}\"", validator.getName(), e);
				throw new ValidationHandlerException("validation failed, validator_name=" + validator.getName(), e);
			} catch (Exception e) {
				logger.debug("exception during validation", "src_desc=\"{}\"",
						actionEvent.getHeaders().get("src-desc"));
				throw new HandlerException(e.getMessage(), e);
			}
		}
		try {
			if (validationReady != ValidationResult.NOT_READY) {
				logger.debug("DataValidationHandler processing event", "updating runtime info actionEvent={}",
						actionEvent);
				updateRuntimeInfoToStoreAfterValidation(runtimeInfoStore, validationPassed, actionEvent);
			}
		} catch (Exception e) {
			logger.debug("exception during validation", "src_desc=\"{}\"", actionEvent.getHeaders().get("src-desc"));
			throw new HandlerException(e.getMessage(), e);
		}
		logger.debug("DataValidationHandler processing event", "updated runtime info");
	}
}
