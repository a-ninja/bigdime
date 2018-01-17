package io.bigdime.handler.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.joda.time.format.DateTimeFormatter;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.libs.hdfs.HDFS_AUTH_OPTION;

public class HiveJdbcReaderHandlerTest {

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		Map<String, Object> properties = getDefaultProperties();
		HiveJdbcReaderHandler hiveJdbcReaderHandler = new HiveJdbcReaderHandler();
		hiveJdbcReaderHandler.setPropertyMap(properties);
		hiveJdbcReaderHandler.build();

		Assert.assertEquals(hiveJdbcReaderHandler.getBaseOutputDirectory(), "/user/johndoe/bigdime/");
		Assert.assertEquals(hiveJdbcReaderHandler.getEntityName(), "unit-entity");
		Assert.assertEquals(hiveJdbcReaderHandler.getHiveQuery(), "unit hive query");
		Assert.assertEquals(hiveJdbcReaderHandler.getGoBackDays(), 1);
		Assert.assertEquals(hiveJdbcReaderHandler.getJdbcUrl(),
				"jdbc:hive2://unit.hostname:10000/default;principal=hadoop/unit.hostname@DOMAIN.COM");
		Assert.assertEquals(hiveJdbcReaderHandler.getDriverClassName(), "org.apache.hive.jdbc.HiveDriver");
		Assert.assertEquals(hiveJdbcReaderHandler.getAuthOption(), HDFS_AUTH_OPTION.KERBEROS);
		Assert.assertEquals(hiveJdbcReaderHandler.getUserName(), "unit-hive-user");

		DateTimeFormatter hdfsOutputPathDtf = hiveJdbcReaderHandler.getHdfsOutputPathDtf();
		Assert.assertNotNull(hdfsOutputPathDtf);
		System.out.println(System.currentTimeMillis());
		long time = 1469753256170l;

		Assert.assertEquals(hdfsOutputPathDtf.print(time), "2016-07-28");
		time = 1438217256170l;
		Assert.assertEquals(hdfsOutputPathDtf.print(time), "2015-07-29");

		Map<String, String> hiveConfigurations = hiveJdbcReaderHandler.getHiveConfigurations();

		Assert.assertNotNull(hiveConfigurations);

	}

	@Test(threadPoolSize = 1, enabled = false)
	public void testProcessWithNullInputDescriptor() throws Throwable {
		HiveJdbcReaderHandler hiveJdbcReaderHandler = createHiveJdbcReaderHandler(getDefaultProperties());
		ReturnStatus returnStatus = setupHandler(hiveJdbcReaderHandler);
		Assert.assertEquals(returnStatus.getStatus(), Status.READY);
	}

	@Test(threadPoolSize = 1, enabled = false)
	public void testProcessWithNonNullInputDescriptor() throws Throwable {
		HiveJdbcReaderHandler hiveJdbcReaderHandler = createHiveJdbcReaderHandler(getDefaultProperties());
		HiveReaderDescriptor inputDescriptor = new HiveReaderDescriptor("unit-entity", "2016-01-01",
				"/user/johndoe/bigdime/2016-01-01/unit-entity", "unit-hive-query");
		ReflectionTestUtils.setField(hiveJdbcReaderHandler, "inputDescriptor", inputDescriptor);
		ReturnStatus returnStatus = setupHandler(hiveJdbcReaderHandler);
		Assert.assertEquals(returnStatus.getStatus(), Status.READY);
	}

	public ReturnStatus setupHandler(final HiveJdbcReaderHandler hiveJdbcReaderHandler) throws Throwable {
		FutureTask<ReturnStatus> futureTask = new FutureTask<>(new Callable<ReturnStatus>() {
			@Override
			public ReturnStatus call() throws Exception {

				Status status = hiveJdbcReaderHandler.process();

				ReturnStatus returnStatus = new ReturnStatus();
				returnStatus.setStatus(status);
				return returnStatus;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			return futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

	private Map<String, Object> getDefaultProperties() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(HiveJdbcReaderHandlerConstants.AUTH_CHOICE, "kerberos");
		properties.put(HiveJdbcReaderHandlerConstants.BASE_OUTPUT_DIRECTORY, "/user/johndoe/bigdime/");
		properties.put(HiveJdbcReaderHandlerConstants.DRIVER_CLASS_NAME, "org.apache.hive.jdbc.HiveDriver");
		properties.put(HiveJdbcReaderHandlerConstants.ENTITY_NAME, "unit-entity");
		properties.put(HiveJdbcReaderHandlerConstants.GO_BACK_DAYS, "1");
		properties.put(HiveJdbcReaderHandlerConstants.HIVE_JDBC_SECRET, "unit-hive-secret");
		properties.put(HiveJdbcReaderHandlerConstants.HIVE_JDBC_USER_NAME, "unit-hive-user");
		properties.put(HiveJdbcReaderHandlerConstants.HIVE_QUERY, "unit-hive-query");
		properties.put(HiveJdbcReaderHandlerConstants.JDBC_URL,
				"jdbc:hive2://unit.hostname:10000/default;principal=hadoop/unit.hostname@DOMAIN.COM");
		properties.put(HiveJdbcReaderHandlerConstants.OUTPUT_DIRECTORY_DATE_FORMAT, "yyyy-MM-dd");

		Map<String, Object> srcDescEntryMap = new HashMap<>();
		srcDescEntryMap.put(HiveJdbcReaderHandlerConstants.ENTITY_NAME, "unit-entity");
		srcDescEntryMap.put(HiveJdbcReaderHandlerConstants.HIVE_QUERY, "unit hive query");

		Map<Object, String> srcDescValueMap = new HashMap<>();
		srcDescValueMap.put(srcDescEntryMap, "input1");

		properties.put(SourceConfigConstants.SRC_DESC, srcDescValueMap.entrySet().iterator().next());

		return properties;
	}

	private HiveJdbcReaderHandler createHiveJdbcReaderHandler(Map<String, Object> properties)
			throws AdaptorConfigurationException, RuntimeInfoStoreException {

		HiveJdbcReaderHandler hiveJdbcReaderHandler = new HiveJdbcReaderHandler();
		hiveJdbcReaderHandler.setPropertyMap(properties);
		hiveJdbcReaderHandler.build();

		@SuppressWarnings("unchecked")
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);

		List<RuntimeInfo> runtimeInfos = new ArrayList<>();

		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfos.add(runtimeInfo);
		Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), "unit-entity",
				RuntimeInfoStore.Status.STARTED)).thenReturn(null);
		Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), "unit-entity",
				RuntimeInfoStore.Status.QUEUED)).thenReturn(runtimeInfos);

		runtimeInfo.setInputDescriptor("hiveConfDate:");

		Map<String, String> runtimeProperty = new HashMap<>();
		runtimeProperty.put("hiveConfDate", "hiveConfDateFromFB");
		runtimeProperty.put("hiveConfDirectory", "hiveConfDirectoryFromDB");
		runtimeProperty.put("hiveConfDate", "hiveQueryFromDB");

		runtimeInfo.setProperties(runtimeProperty);
		ReflectionTestUtils.setField(hiveJdbcReaderHandler, "runtimeInfoStore", runtimeInfoStore);

		// final List<RuntimeInfo> runtimeInfos =
		// runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(),
		// entityName, RuntimeInfoStore.Status.STARTED);

		return hiveJdbcReaderHandler;
	}

	// private List<ActionEvent> getTestEvents(int num) {
	// List<ActionEvent> contextEventList = new ArrayList<>(num);
	// for (int i = 0; i < num; i++) {
	// ActionEvent event = new ActionEvent();
	// event.setBody(
	// ("/webhdfs/v1/root/johndoe/bigdime/20160101/entityName/fileName01.ext\n/webhdfs/v1/root/johndoe/bigdime/newdir/20160101/entityName/fileName0"
	// + num + ".ext").getBytes());
	//
	// event.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME,
	// "/webhdfs/v1/root/johndoe/bigdime/20160101/entityName/fileName01.ext");
	// contextEventList.add(event);
	// }
	// return contextEventList;
	// }
}

class ReturnStatus {
	private Status status;
	private List<ActionEvent> eventList = new ArrayList<>();

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public List<ActionEvent> getEventList() {
		return eventList;
	}

	public void setEventList(List<ActionEvent> eventList) {
		this.eventList = eventList;
	}

}
