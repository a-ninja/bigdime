/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.validation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.reflections.Reflections;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;

/**
 * Factory to get the {@link Validator} object implementing a specific
 * validation-type. Only one implementation for each validation type is
 * supported.
 * 
 * @author Neeraj Jain
 * 
 */
@Component
public final class ValidatorFactory {

	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(ValidatorFactory.class));
	private Set<Class<?>> annotated;
	private Map<String, Class<? extends Validator>> validators = new HashMap<>();

	@Autowired
	private ApplicationContext context;

	private ValidatorFactory() throws AdaptorConfigurationException {

	}

	@PostConstruct
	private void init() throws AdaptorConfigurationException {
		logger.info("initializing ValidatorFactory", "validators=\"{}\"",
				validators);

		final Reflections reflections = new Reflections("io.bigdime");
		annotated = reflections.getTypesAnnotatedWith(Factory.class);
		for (Class<?> controller : annotated) {
			Factory dataValidatorFactory = controller
					.getAnnotation(Factory.class);
			logger.info(
					"initializing validators",
					"validator_type=\"{}\" validator_class=\"{}\" type_exists=\"{}\"",
					dataValidatorFactory.id(), dataValidatorFactory.type(),
					validators.get(dataValidatorFactory.id()));
			if (validators.get(dataValidatorFactory.id()) != null) {
				throw new AdaptorConfigurationException(
						"more than one Validators found for type="
								+ dataValidatorFactory.id());
			}
			validators.put(dataValidatorFactory.id(),
					dataValidatorFactory.type());
		}
	}

	/**
	 * Gets a @link {@link Validator} for given validationType.
	 * 
	 * @param validationType
	 *            identifier for Validator
	 * @return Validator identified by ValidationType
	 * @throws AdaptorConfigurationException
	 *             if the validator class can't instantiated or if there are
	 *             more than one validators available for validationType or
	 *             there are is no validator available for validationType.
	 */
	public Validator getValidator(final String validationType)
			throws AdaptorConfigurationException {
		if (validators.get(validationType) == null) {
			throw new AdaptorConfigurationException(
					"no validators found for type=" + validationType);
		}
		try {
			return context.getBean(validators.get(validationType));
		} catch (BeansException e) {
			throw new AdaptorConfigurationException(e);
		}
	}
}
