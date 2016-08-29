/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hdfs;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidDataException;
import io.bigdime.core.commons.StringCase;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.handler.webhdfs.HdfsFilePathBuilder;

public class HdfsFilePathBuilderTest {
	/**
	 * Test build with no tokens in the hdfspath.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuild() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdFs";
		ActionEvent actionEvent = createSampleTestEventWithHeaders();
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		Assert.assertEquals(path, "unithdFs/unitbase/unitrelative");
	}

	@Test
	public void testBuildWithLowerCase() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdFS";
		ActionEvent actionEvent = createSampleTestEventWithHeaders();
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).withCase(StringCase.LOWER)
				.build();
		Assert.assertEquals(path, "unithdFS/unitbase/unitrelative");
	}

	@Test
	public void testBuildWithUpperCase() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdFS";
		ActionEvent actionEvent = createSampleTestEventWithHeaders();
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).withCase(StringCase.UPPER)
				.build();
		Assert.assertEquals(path, "unithdFS/unitbase/unitrelative");
	}

	@Test
	public void testBuild1() throws HandlerException {
		// System.out.println(File.separator);
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "/unithdfs/";
		ActionEvent actionEvent = createSampleTestEventWithHeaders();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.BASE_PATH, "/unitbase/");

		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		Assert.assertEquals(path, "/unithdfs/unitbase/unitrelative");
	}

	/**
	 * If preserveBasePath and preserveRelativePath flags are not set, basePath
	 * and relativePath will NOT be added to hdfsPath.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuildWithNoPreservePathFlag() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdfs";
		ActionEvent actionEvent = createSampleTestEventWithEmptyHeaders();
		addBasePathHeader(actionEvent);
		addRelativePathHeader(actionEvent);
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		Assert.assertEquals(path, "unithdfs");
	}

	/**
	 * If actionEvent has no headers, no basePath and relativePath should be
	 * added to hdfsPath.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuildWithNoHeaders() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdfs";
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setHeaders(null);

		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		Assert.assertEquals(path, "unithdfs");
	}

	/**
	 * If preserveBasePath and preserveRelativePath flags are set but there is
	 * no basePath and relativePath set in header, no basePath and relativePath
	 * should be added to hdfsPath.
	 * 
	 * @throws HandlerException
	 */

	@Test
	public void testBuildWithNoBaseOrRelativePath() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdfs";
		ActionEvent actionEvent = createSampleTestEventWithEmptyHeaders();
		addPreserveBasePathHeader(actionEvent);
		addPreserveRelativePathHeader(actionEvent);

		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		Assert.assertEquals(path, "unithdfs");
	}

	/**
	 * Irrespective of input paths end with slash or not, output path should be
	 * same.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuildWithPathEndingWithSlash() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		ActionEvent actionEvent = createSampleTestEventWithHeaders();
		String hdfsPath = "unithdfs";
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		Assert.assertEquals(path, "unithdfs/unitbase/unitrelative");
	}

	/**
	 * Test with hdfsPath being null.
	 * 
	 * @throws HandlerException
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "hdfsPath can't be null or empty.*")
	public void testBuildWithNullHdfsPath() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		ActionEvent actionEvent = createSampleTestEventWithHeaders();
		hdfsFilePathBuilder.withActionEvent(actionEvent).build();
	}

	/**
	 * Test with hdfsPath being empty.
	 * 
	 * @throws HandlerException
	 */
	@Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "hdfsPath can't be null or empty.*")
	public void testBuildWithEmptyHdfsPath() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		ActionEvent actionEvent = createSampleTestEventWithHeaders();

		hdfsFilePathBuilder.withActionEvent(actionEvent).build();
	}

	/**
	 * Test with no basePath or relativePath from file handler.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuildWithOnlyHdfsPath() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdfs";
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setHeaders(new HashMap<String, String>());
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		Assert.assertEquals(path, "unithdfs");
	}

	/**
	 * If the hdfsPath has tokens, the basePath should be the path before first
	 * token.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testGetBaseHdfsPath() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdfsbase/${partition1}/${partition2}/partition3";
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setHeaders(new HashMap<String, String>());
		hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		String baseHdfsPath = hdfsFilePathBuilder.getBaseHdfsPath();
		Assert.assertEquals(baseHdfsPath, "unithdfsbase/");
	}

	@Test
	public void testGetBaseHdfsPathWithNoPartition() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "unithdfsbase";
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setHeaders(new HashMap<String, String>());
		// hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		String baseHdfsPath = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).getBaseHdfsPath();
		Assert.assertEquals(baseHdfsPath, "unithdfsbase/");
	}

	/**
	 * If the hdfsPath starts with token, the basePath should be empty.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testGetBaseHdfsPathWithhdfsPathStartingWithToken() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		String hdfsPath = "${partition1}/${partition2}/partition3";
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setHeaders(new HashMap<String, String>());
		hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).build();
		String baseHdfsPath = hdfsFilePathBuilder.getBaseHdfsPath();
		Assert.assertEquals(baseHdfsPath, "/");
	}

	/**
	 * If the hdfsPath has partitions, the tokens should be redeemed from the
	 * headers.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuildWithHdfsPathWithPartitions() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		Map<String, String> tokenToHeaderNameMap = new HashMap<>();
		tokenToHeaderNameMap.put("${partition1}", "partition1");
		tokenToHeaderNameMap.put("${partition2}", "partition2");
		String hdfsPath = "unithdfs/${partition1}/${partition2}/partition3";
		ActionEvent actionEvent = new ActionEvent();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("partition1", "partition1-value");
		headers.put("partition2", "partition2-value");
		actionEvent.setHeaders(headers);
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent)
				.withTokenHeaderMap(tokenToHeaderNameMap).build();
		Assert.assertEquals(path, "unithdfs/partition1-value/partition2-value/partition3");

		Map<String, String> hivePartitionNameValueMap = hdfsFilePathBuilder.getPartitionNameValueMap();
		Assert.assertEquals(hivePartitionNameValueMap.get("partition1"), "partition1-value");
		Assert.assertEquals(hivePartitionNameValueMap.get("partition2"), "partition2-value");
	}

	/**
	 * If the hdfsPath has partitions, the tokens should be redeemed from the
	 * headers. The string value should be changed to lower case.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuildWithHdfsPathWithPartitionsWithLowerCase() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		Map<String, String> tokenToHeaderNameMap = new HashMap<>();
		tokenToHeaderNameMap.put("${partition1}", "partition1");
		tokenToHeaderNameMap.put("${partition2}", "partition2");
		String hdfsPath = "UNIThdfs/${partition1}/${partition2}/partition3";
		ActionEvent actionEvent = new ActionEvent();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("partition1", "partiTIon1-value");
		headers.put("partition2", "partitiON2-value");
		actionEvent.setHeaders(headers);
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent)
				.withTokenHeaderMap(tokenToHeaderNameMap).withCase(StringCase.LOWER).build();
		Assert.assertEquals(path, "UNIThdfs/partition1-value/partition2-value/partition3");

		Map<String, String> hivePartitionNameValueMap = hdfsFilePathBuilder.getPartitionNameValueMap();
		Assert.assertEquals(hivePartitionNameValueMap.get("partition1"), "partition1-value");
		Assert.assertEquals(hivePartitionNameValueMap.get("partition2"), "partition2-value");
	}

	/**
	 * If the hdfsPath has partitions, the tokens should be redeemed from the
	 * headers. The string value should be changed to UPPER case.
	 * 
	 * @throws HandlerException
	 */
	@Test
	public void testBuildWithHdfsPathWithPartitionsWithUpperCase() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		Map<String, String> tokenToHeaderNameMap = new HashMap<>();
		tokenToHeaderNameMap.put("${partition1}", "partition1");
		tokenToHeaderNameMap.put("${partition2}", "partition2");
		String hdfsPath = "unithdfs/${partition1}/${partition2}/partition3";
		ActionEvent actionEvent = new ActionEvent();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("partition1", "partiTIon1-value");
		headers.put("partition2", "partitiON2-value");
		actionEvent.setHeaders(headers);
		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent)
				.withTokenHeaderMap(tokenToHeaderNameMap).withCase(StringCase.UPPER).build();
		Assert.assertEquals(path, "unithdfs/PARTITION1-VALUE/PARTITION2-VALUE/partition3");

		Map<String, String> hivePartitionNameValueMap = hdfsFilePathBuilder.getPartitionNameValueMap();
		Assert.assertEquals(hivePartitionNameValueMap.get("partition1"), "PARTITION1-VALUE");
		Assert.assertEquals(hivePartitionNameValueMap.get("partition2"), "PARTITION2-VALUE");
	}

	/**
	 * If the hdfsPath has no partitions, there are no tokens to be redeemed,
	 * final path should be same as hdfsPath.
	 * 
	 * @throws HandlerException
	 */

	@Test
	public void testBuildWithHdfsPathWithNoPartitions() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		Map<String, String> tokenToHeaderNameMap = new HashMap<>();
		tokenToHeaderNameMap.put("${partition1}", "partition1");
		tokenToHeaderNameMap.put("${partition2}", "partition2");
		String hdfsPath = "unithdfs/partition1-value/partition2-value/partition3";
		ActionEvent actionEvent = createSampleTestEventWithHeaders();

		String path = hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent)
				.withTokenHeaderMap(tokenToHeaderNameMap).build();
		Assert.assertEquals(path, "unithdfs/partition1-value/partition2-value/partition3/unitbase/unitrelative");
	}

	@Test(expectedExceptions = InvalidDataException.class)
	public void testBuildWithOnlyHdfsPathWithPartitionsAndMissingHeader() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		Map<String, String> tokenToHeaderNameMap = new HashMap<>();
		tokenToHeaderNameMap.put("${partition1}", "partition1");
		tokenToHeaderNameMap.put("${partition2}", "partition2");
		String hdfsPath = "unithdfs/${partition1}/${partition2}/partition3";
		ActionEvent actionEvent = new ActionEvent();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put("partition1", "partition1-value");
		actionEvent.setHeaders(headers);
		hdfsFilePathBuilder.withHdfsPath(hdfsPath).withActionEvent(actionEvent).withTokenHeaderMap(tokenToHeaderNameMap)
				.build();
	}

	@Test
	public void testBuildWithPartitionValuesHeader() throws HandlerException {
		HdfsFilePathBuilder hdfsFilePathBuilder = new HdfsFilePathBuilder();
		ActionEvent actionEvent = new ActionEvent();
		Map<String, String> headers = new HashMap<String, String>();
		headers.put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, "act1,11032015");
		headers.put(ActionEventHeaderConstants.RELATIVE_PATH, "path1");
		headers.put(ActionEventHeaderConstants.PRESERVE_RELATIVE_PATH, "true");
		actionEvent.setHeaders(headers);
		String path = hdfsFilePathBuilder.withActionEvent(actionEvent).withHdfsPath("/webhdfs/v1/data/path/raw")
				.build();
		String basePath = hdfsFilePathBuilder.getBaseHdfsPath();
		Assert.assertEquals(path, "/webhdfs/v1/data/path/raw/path1/act1/11032015");
		Assert.assertEquals(basePath, "/webhdfs/v1/data/path/raw/path1/");
		// System.out.println("path=" + path + ", basePath=" + basePath);

	}

	private ActionEvent createSampleTestEventWithEmptyHeaders() {
		Map<String, String> headers = new HashMap<String, String>();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setHeaders(headers);
		return actionEvent;
	}

	private ActionEvent createSampleTestEventWithHeaders() {
		Map<String, String> headers = new HashMap<String, String>();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.setHeaders(headers);
		addPreserveBasePathHeader(actionEvent);
		addBasePathHeader(actionEvent);
		addPreserveRelativePathHeader(actionEvent);
		addRelativePathHeader(actionEvent);
		return actionEvent;
	}

	private void addPreserveBasePathHeader(ActionEvent actionEvent) {
		actionEvent.getHeaders().put(ActionEventHeaderConstants.PRESERVE_BASE_PATH, "true");
	}

	private void addBasePathHeader(ActionEvent actionEvent) {
		actionEvent.getHeaders().put(ActionEventHeaderConstants.BASE_PATH, "unitbase/");
	}

	private void addPreserveRelativePathHeader(ActionEvent actionEvent) {
		actionEvent.getHeaders().put(ActionEventHeaderConstants.PRESERVE_RELATIVE_PATH, "true");
	}

	private void addRelativePathHeader(ActionEvent actionEvent) {
		actionEvent.getHeaders().put(ActionEventHeaderConstants.RELATIVE_PATH, "unitrelative");
	}

}
