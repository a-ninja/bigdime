package io.bigdime.handler.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.handler.webhdfs.WebHDFSReaderHandler;
import io.bigdime.handler.webhdfs.WebHDFSReaderHandlerConfig.READ_HDFS_PATH_FROM;
import io.bigdime.handler.webhdfs.WebHDFSReaderHandlerConstants;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;

public class WebHDFSReaderHandlerTest {
	@Test
	public void testBuild() throws AdaptorConfigurationException {
		Map<String, Object> properties = getDefaultProperties();
		WebHDFSReaderHandler webHDFSReaderHandler = new WebHDFSReaderHandler();
		webHDFSReaderHandler.setPropertyMap(properties);
		webHDFSReaderHandler.build();

		Assert.assertEquals(webHDFSReaderHandler.getAuthOption(), HDFS_AUTH_OPTION.KERBEROS);
		Assert.assertNull(webHDFSReaderHandler.getEntityName());
		Assert.assertNull(webHDFSReaderHandler.getHdfsPath());
		Assert.assertEquals(webHDFSReaderHandler.getHostNames(), "sandbox.localhost");
		Assert.assertEquals(webHDFSReaderHandler.getReadHdfsPathFrom(), READ_HDFS_PATH_FROM.HEADERS);
		Assert.assertEquals(webHDFSReaderHandler.getPort(), 50080);
	}

	private Map<String, Object> getDefaultProperties() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(WebHDFSReaderHandlerConstants.AUTH_CHOICE, "kerberos");
		properties.put(ActionEventHeaderConstants.ENTITY_NAME, "unit-entity");
		properties.put(WebHDFSReaderHandlerConstants.HOST_NAMES, "sandbox.localhost");
		properties.put(WebHDFSReaderHandlerConstants.READ_HDFS_PATH_FROM, "headers");
		properties.put(WebHDFSReaderHandlerConstants.PORT, "50080");

		return properties;
	}

}
