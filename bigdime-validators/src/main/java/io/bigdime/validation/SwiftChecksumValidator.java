package io.bigdime.validation;

import org.apache.commons.codec.digest.DigestUtils;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.core.validation.Validator;

@Factory(id = "swift_checksum", type = SwiftChecksumValidator.class)
@Component
@Scope("prototype")

public class SwiftChecksumValidator implements Validator {
	// @Autowired

	private String containerName;
	private String username;
	private String password; // make it char[]
	private String authUrl;
	private String tenantId;
	private String tenantName;
	private AccountConfig config;
	private Account account;
	private Container container;

	protected void init(ActionEvent actionEvent) {

		username = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.USER_NAME);
		password = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.SECRET);
		authUrl = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.AUTH_URL);
		tenantId = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.TENANT_ID);
		tenantName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.TENANT_NAME);
		containerName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.CONTAINER_NAME);
		config = new AccountConfig();
		config.setUsername(username);
		config.setPassword(password);
		config.setAuthUrl(authUrl);
		config.setTenantId(tenantId);
		config.setTenantName(tenantName);
		account = new AccountFactory(config).createAccount();
		container = account.getContainer(containerName);
	}

	private static final Logger logger = LoggerFactory.getLogger(SwiftChecksumValidator.class);

	/**
	 * 
	 */
	@Override
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {

		String objectName = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.OBJECT_NAME);
		String objectEtag = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.OBJECT_ETAG);

		String sourceChecksum = DigestUtils.md5Hex(actionEvent.getBody());
		// StoredObject storedObject = container.getObject(objectName);
		logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
				"processing SwiftChecksumValidator", "objectEtag={} sourceChecksum={}", objectEtag, sourceChecksum);

		actionEvent.getHeaders().put(ActionEventHeaderConstants.SwiftHeaders.SOURCE_CHECKSUM, sourceChecksum);
		ValidationResponse validationResponse = new ValidationResponse();
		if (objectEtag.equalsIgnoreCase(sourceChecksum))
			validationResponse.setValidationResult(ValidationResult.PASSED);
		else {
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"processing SwiftChecksumValidator",
					"_message=\"validation failed, will be retried\" objectEtag={} sourceChecksum={}", objectEtag,
					sourceChecksum);
			StoredObject object = container.getObject(objectName);
			final String newEtag = object.getEtag();
			logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
					"processing SwiftChecksumValidator",
					"_message=\"validation retry\" objectEtag={} sourceChecksum={}", newEtag, sourceChecksum);
			if (objectEtag.equalsIgnoreCase(sourceChecksum)) {
				validationResponse.setValidationResult(ValidationResult.PASSED);
			} else {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
						"processing SwiftChecksumValidator",
						"_message=\"validation failed after retry\" objectEtag={} sourceChecksum={}", newEtag,
						sourceChecksum);
				validationResponse.setValidationResult(ValidationResult.FAILED);
			}
		}

		return validationResponse;
	}

	@Override
	public String getName() {
		return "swift-checksum-validator";
	}

}
