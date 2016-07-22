package io.bigdime.validation;

import org.apache.commons.codec.digest.DigestUtils;
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
	// protected String containerName;
	// protected String username;
	// protected String password; // make it char[]
	// protected String authUrl;
	// protected String tenantId;
	// protected String tenantName;
	// private AccountConfig config;
	// private Account account;
	// private Container container;

	// protected void init(ActionEvent actionEvent) {
	//
	// username =
	// actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.USER_NAME);
	// password =
	// actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.SECRET);
	// authUrl =
	// actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.AUTH_URL);
	// tenantId =
	// actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.TENANT_ID);
	// tenantName =
	// actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.TENANT_NAME);
	// containerName =
	// actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.CONTAINER_NAME);
	// config = new AccountConfig();
	// config.setUsername(username);
	// config.setPassword(password);
	// config.setAuthUrl(authUrl);
	// config.setTenantId(tenantId);
	// config.setTenantName(tenantName);
	// account = new AccountFactory(config).createAccount();
	// container = account.getContainer(containerName);
	// }
	private static final Logger logger = LoggerFactory.getLogger(SwiftChecksumValidator.class);

	/**
	 * 
	 */
	@Override
	public ValidationResponse validate(ActionEvent actionEvent) throws DataValidationException {

		// String objectName =
		// actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.OBJECT_NAME);
		String objectEtag = actionEvent.getHeaders().get(ActionEventHeaderConstants.SwiftHeaders.OBJECT_ETAG);

		String sourceChecksum = DigestUtils.md5Hex(actionEvent.getBody());
		// StoredObject storedObject = container.getObject(objectName);
		logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(),
				"processing SwiftChecksumValidator", "objectEtag={} sourceChecksum={}", objectEtag, sourceChecksum);

		ValidationResponse validationResponse = new ValidationResponse();
		if (objectEtag.equalsIgnoreCase(sourceChecksum))
			validationResponse.setValidationResult(ValidationResult.PASSED);
		else
			validationResponse.setValidationResult(ValidationResult.FAILED);

		return validationResponse;
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

}
