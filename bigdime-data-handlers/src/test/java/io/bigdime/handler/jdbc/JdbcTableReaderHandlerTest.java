/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

import javax.sql.DataSource;

import org.json.JSONObject;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@PrepareForTest({JdbcTemplate.class, JdbcTableReaderHandler.class})
public class JdbcTableReaderHandlerTest extends PowerMockTestCase {
	
	JdbcTableReaderHandler jdbcTableHandler;
	
	@Mock
	JdbcTemplate jdbcTemplate;
	@Mock
	JdbcInputDescriptor jdbcInputDescriptor;
	@Mock
	DataSource dataSource;
	@Mock
	JdbcMetadataManagement jdbcMetadataManagement;
	@Mock
	Metasegment metasegment;
	@Mock
	JdbcMetadata jdbcMetadata;
	@Mock
	MetadataStore metadataStore;

	@BeforeClass
	public void init() {
		AdaptorConfig.getInstance().setName("sql-adaptor");
	}
	
	@Test
	public void testBuild() throws Exception {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		Map<String, Object> propertyMap = new HashMap<>();
		String inputString = "{\"input1\" : {\"inputType\":\"database\",\"inputValue\":\"testDB\", \"incrementedBy\":\"incrementalCol\", \"include\":\"\"}}";
		JSONObject jsonObject = new JSONObject(inputString);
		Map<String, String> srcDEscEntryMap = new HashMap<>();
		srcDEscEntryMap.put(jsonObject.getString("input1"), "input1");
		propertyMap.put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC,
				srcDEscEntryMap.entrySet().iterator().next());
		jdbcTableHandler.setPropertyMap(propertyMap);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		when(jdbcInputDescriptor.getInputType()).thenReturn("database");
		when(jdbcInputDescriptor.getInputValue()).thenReturn("testDB");
		when(jdbcInputDescriptor.getIncludeFilter()).thenReturn("");
		jdbcTableHandler.build();
		Assert.assertEquals(jdbcInputDescriptor.getInputType(), "database");
		Assert.assertEquals(jdbcInputDescriptor.getInputValue(), "testDB");
		Assert.assertEquals(jdbcInputDescriptor.getIncludeFilter(), "");
	}
	
	@Test(expectedExceptions = InvalidValueConfigurationException.class)
	public void testBuildWithoutSrcDesc() throws Exception {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		Map<String, Object> propertyMap = new HashMap<>();
		String inputString = "{\"input1\" : {\"inputType\":\"database\",\"inputValue\":\"testDB\", \"incrementedBy\":\"incrementalCol\", \"include\":\"\"}}";
		JSONObject jsonObject = new JSONObject(inputString);
		Map<String, String> srcDEscEntryMap = new HashMap<>();
		srcDEscEntryMap.put(jsonObject.getString("input1"), "input1");
		jdbcTableHandler.setPropertyMap(propertyMap);
		jdbcTableHandler.build();
	}
	
	@Test(expectedExceptions = HandlerException.class)
	public void testProcessWithNullActionEvents() throws HandlerException {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		HandlerContext context = HandlerContext.get();
		context.setEventList(null);
		jdbcTableHandler.process();
	}
	
	@Test(expectedExceptions = HandlerException.class)
	public void testProcessWithNullTableSQLQuery() throws HandlerException, JdbcHandlerException {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		HandlerContext context = HandlerContext.get();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.TARGET_ENTITY_NAME, "testTable");
		context.createSingleItemEventList(actionEvent);
		when(jdbcInputDescriptor.getEntityName()).thenReturn("testTable");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn("");
		jdbcTableHandler.process();
	}
	
	@SuppressWarnings("unchecked")
	@Test(expectedExceptions = HandlerException.class)
	public void unableToGetSourceMetadataColumnList() throws Exception {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		HandlerContext context = HandlerContext.get();
		List<ActionEvent> eventList = new ArrayList<>();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.TARGET_ENTITY_NAME, "testTable");
		eventList.add(actionEvent);
		context.setEventList(eventList);
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.getEntityName()).thenReturn("testTable");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn("tableSqlQuery");
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		List<String> mockList = mock(List.class);
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockList);
		when(mockList.size()).thenReturn(0);
		jdbcTableHandler.process();
	}
	
	@SuppressWarnings("unchecked")
	@Test(expectedExceptions = HandlerException.class)
	public void incrementalColumnNotExist() throws Exception {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.TARGET_ENTITY_NAME, "testTable");
        when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn("tableSqlQuery");
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		List<String> mockList = mock(List.class);
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockList);
		when(mockList.size()).thenReturn(1);
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("LUD");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn(null);
		jdbcTableHandler.process();
	}
	
	@SuppressWarnings("unchecked")
	@Test(expectedExceptions = HandlerException.class)
	public void unableToInsertToMetastore() throws Exception {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		HandlerContext context = HandlerContext.get();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.TARGET_ENTITY_NAME, "testTable");
		context.createSingleItemEventList(actionEvent);
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.getEntityName()).thenReturn("testTable");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn("tableSqlQuery");
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		List<String> mockList = mock(List.class);
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockList);
		when(mockList.size()).thenReturn(1);
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("LUD");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("DATE");
		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
		doThrow(new MetadataAccessException("")).when(metadataStore).put(any(Metasegment.class));
		jdbcTableHandler.process();
	}
	

	@SuppressWarnings("unchecked")
	@Test(expectedExceptions = HandlerException.class)
	public void testProcessThrowException() throws Exception{
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		String query = "select t.* from (select * from testDB.testTable where LUD > ? order by LUD asc) where rownum < 5";
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.getEntityName()).thenReturn("testTable");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn(query);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		List<String> mockList = mock(List.class);
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockList);
		when(mockList.size()).thenReturn(2);
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("LUD");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("DATE");
		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
		doNothing().when(metadataStore).put(any(Metasegment.class));
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = mock(RuntimeInfoStore.class);
		when(runtimeInfoStore.getAll(anyString(), anyString(), any(RuntimeInfoStore.Status.class)))
				.thenThrow(new RuntimeInfoStoreException(""));
		ReflectionTestUtils.setField(jdbcTableHandler, "runTimeInfoStore", runtimeInfoStore);
		when(runtimeInfoStore.put(any(RuntimeInfo.class))).thenReturn(true);
		jdbcTableHandler.process();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void entityNotExistsInRuntimeInfoStore() throws Exception {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.TARGET_ENTITY_NAME, "testTable");
		String query = "select t.* from (select * from testDB.testTable where LUD > ? order by LUD asc) where rownum < 5";
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.getEntityName()).thenReturn("testTable");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn(query);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		List<String> mockList = mock(List.class);
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockList);
		when(mockList.size()).thenReturn(2);
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("LUD");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("DATE");
		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
		doNothing().when(metadataStore).put(any(Metasegment.class));
		Map<String, String> proMap = new HashMap<>();
		String ludColumnValue = "1900-01-01 00:00:00";
		proMap.put("LUD", ludColumnValue);
		RuntimeInfo mockRti = mock(RuntimeInfo.class);
		List<RuntimeInfo> mockRtiList = mock(List.class);
		getQueuedRuntimeInfo(null, mockRtiList);
		when(mockRtiList.get(anyInt())).thenReturn(mockRti);
		when(mockRti.getProperties()).thenReturn(proMap);
		ReflectionTestUtils.setField(jdbcTableHandler, "driverName", JdbcConstants.ORACLE_DRIVER_NAME);
		getColumnValue(ludColumnValue);
		getRecordMapListBasedOnLUD(ludColumnValue);
		Assert.assertEquals(jdbcTableHandler.process(), Status.CALLBACK);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void entityExistsInRuntimeInfoStore() throws Exception {
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		HandlerContext context = HandlerContext.get();
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.TARGET_ENTITY_NAME, "testTable");
		context.createSingleItemEventList(actionEvent);
		doNothing().when(jdbcInputDescriptor).setEntityName("testTable");
		doNothing().when(jdbcInputDescriptor).setTargetEntityName("testTable");
		String query = "select t.* from (select * from testDB.testTable where LUD > ? order by LUD asc) where rownum < 5";
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.getEntityName()).thenReturn("testTable");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn(query);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("LUD");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("DATE");
		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
		doNothing().when(metadataStore).put(any(Metasegment.class));
		Map<String, String> proMap = new HashMap<>();
		String ludColumnValue = "1900-01-01 00:00:00";
		proMap.put("LUD", ludColumnValue);
		RuntimeInfo mockRti = mock(RuntimeInfo.class);
		List<RuntimeInfo> mockRtiList = mock(List.class);
		getQueuedRuntimeInfo(mockRtiList, mockRtiList);
		when(mockRtiList.get(anyInt())).thenReturn(mockRti);
		when(mockRti.getProperties()).thenReturn(proMap);
		ReflectionTestUtils.setField(jdbcTableHandler, "driverName", "otherDriver");
		getColumnValue(ludColumnValue);
		getRecordMapListBasedOnLUD(ludColumnValue);
		when(jdbcInputDescriptor.getSnapshot()).thenReturn("yes");
		Assert.assertEquals(jdbcTableHandler.process(), Status.CALLBACK);
	}
	
	//Will fix this test case later caused by null pointer exception
//	@SuppressWarnings("unchecked")
//	@Test
//	public void testHandleRestartAdaptorCase() throws Exception{
//		jdbcTableHandler = new JdbcTableReaderHandler();
//		jdbcTableHandler.setDataSource(dataSource);
//		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
//		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
//		when(jdbcInputDescriptor.getEntityName()).thenReturn("testTable");
//		when(jdbcInputDescriptor.getTargetEntityName()).thenReturn("testTable");
//		String query = "select t.* from (select * from testDB.nonPartitionTable where LUD > ? order by LUD asc) where rownum < 5";
//		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
//		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn(query);
//		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
//		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
//		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
//		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
//		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("LUD");
//		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("DATE");
//		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
//		doNothing().when(metadataStore).put(any(Metasegment.class));
//		Map<String, String> proMap = new HashMap<>();
//		String stopColumnValue = "2016-01-01 00:00:00.0";
//		proMap.put("LUD", stopColumnValue);
//		RuntimeInfo mockRti = mock(RuntimeInfo.class);
//		RuntimeInfo mockRti = createRuntimeInfo(stopColumnValue);
//		List<RuntimeInfo> mockRtiList = mock(List.class);
//		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = mock(RuntimeInfoStore.class);
//		List<RuntimeInfo> mockRtiList = new ArrayList<>();
//		mockRtiList.add(mockRti);
//		getQueuedRuntimeInfo(mockRtiList, mockRtiList);
//		createRuntimeInfo(stopColumnValue);
//		ReflectionTestUtils.setField(jdbcTableHandler, "runTimeInfoStore", runtimeInfoStore);
//		when(runtimeInfoStore.put(any(RuntimeInfo.class))).thenReturn(true);
//		when(mockRtiList.get(anyInt())).thenReturn(mockRti);
//		when(mockRtiList.get(anyInt())).thenReturn(mockRti);
//		ReflectionTestUtils.setField(jdbcTableHandler, "runtimeInfo", mockRti);
//		when(mockRti.getProperties()).thenReturn(proMap);
//		ReflectionTestUtils.setField(jdbcTableHandler, "driverName", JdbcConstants.ORACLE_DRIVER_NAME);
//		ReflectionTestUtils.setField(jdbcTableHandler, "processDirty", Boolean.TRUE);
//		getColumnValue(stopColumnValue);
//		getRecordMapListBasedOnLUD(stopColumnValue);
//		Assert.assertEquals(jdbcTableHandler.process(), Status.READY);
//	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNonPartitionTableReturnReadyCase() throws Exception{
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		when(jdbcInputDescriptor.getEntityName()).thenReturn("nonPartitionTable");
		when(jdbcInputDescriptor.getTargetEntityName()).thenReturn("nonPartitionTable");
		String query = "select t.* from (select * from testDB.nonPartitionTable where ID > 0 order by ID asc) where rownum < 5";
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn(query);
		ReflectionTestUtils.setField(jdbcTableHandler, "processTableSql", query);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("ID");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("INT");
		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
		doNothing().when(metadataStore).put(any(Metasegment.class));
		List<String> mockList = mock(List.class);
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockList);
		when(mockList.size()).thenReturn(2);
		Map<String, String> proMap = new HashMap<>();
		String idColumnValue = "0";
		proMap.put("ID", idColumnValue);
		RuntimeInfo mockRti = mock(RuntimeInfo.class);
		List<RuntimeInfo> mockRtiList = mock(List.class);
		getQueuedRuntimeInfo(null, mockRtiList);
		when(mockRtiList.get(anyInt())).thenReturn(mockRti);
		when(mockRti.getProperties()).thenReturn(proMap);
		ReflectionTestUtils.setField(jdbcTableHandler, "driverName", JdbcConstants.ORACLE_DRIVER_NAME);
		List<Map<String, Object>> rowsList = mock(List.class);
		when(jdbcTemplate.queryForList(anyString(), anyObject())).thenReturn(rowsList);
		when(rowsList.size()).thenReturn(0);
		jdbcTableHandler.incrementInvocationCount();
		Assert.assertEquals(jdbcTableHandler.process(), Status.READY);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNonPartitionTableReturnCallbackCase() throws Exception{
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		when(jdbcInputDescriptor.getEntityName()).thenReturn("nonPartitionTable");
		when(jdbcInputDescriptor.getTargetEntityName()).thenReturn("nonPartitionTable");
		String query = "select t.* from (select * from testDB.nonPartitionTable where ID > ? order by ID asc) where rownum < 5";
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn(query);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("ID");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("INT");
		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
		doNothing().when(metadataStore).put(any(Metasegment.class));
		Map<String, String> proMap = new HashMap<>();
		String idColumnValue = "0";
		proMap.put("ID", idColumnValue);
		RuntimeInfo mockRti = mock(RuntimeInfo.class);
		List<RuntimeInfo> mockRtiList = mock(List.class);
		getQueuedRuntimeInfo(null, mockRtiList);
		when(mockRtiList.get(anyInt())).thenReturn(mockRti);
		when(mockRti.getProperties()).thenReturn(proMap);
		ReflectionTestUtils.setField(jdbcTableHandler, "driverName", JdbcConstants.ORACLE_DRIVER_NAME);
		getRecordMapListBasedOnID(idColumnValue);
		jdbcTableHandler.incrementInvocationCount();
		Assert.assertEquals(jdbcTableHandler.process(), Status.CALLBACK);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNonPartitionTableSnapshotCase() throws Exception{
		jdbcTableHandler = new JdbcTableReaderHandler();
		jdbcTableHandler.setDataSource(dataSource);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcInputDescriptor", jdbcInputDescriptor);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcTemplate", jdbcTemplate);
		when(jdbcInputDescriptor.getEntityName()).thenReturn("nonPartitionTable");
		when(jdbcInputDescriptor.getTargetEntityName()).thenReturn("nonPartitionTable");
		String query = "select t.* from (select * from testDB.nonPartitionTable where ID > ? order by ID asc) where rownum < 5";
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		when(jdbcInputDescriptor.formatProcessTableQuery(anyString(), anyString(), anyString())).thenReturn(query);
		PowerMockito.whenNew(JdbcTemplate.class).withArguments((DataSource)any()).thenReturn(jdbcTemplate);
		when(jdbcTemplate.query(anyString(), (ResultSetExtractor<Metasegment>) any())).thenReturn(metasegment);
		ReflectionTestUtils.setField(jdbcTableHandler, "jdbcMetadataManagment", jdbcMetadataManagement);
		doNothing().when(jdbcMetadataManagement).setColumnList(any(JdbcInputDescriptor.class), any(Metasegment.class));
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("ID");
		when(jdbcInputDescriptor.getIncrementedColumnType()).thenReturn("INT");
		ReflectionTestUtils.setField(jdbcTableHandler, "metadataStore", metadataStore);
		doNothing().when(metadataStore).put(any(Metasegment.class));
		Map<String, String> proMap = new HashMap<>();
		String idColumnValue = "1";
		proMap.put("ID", idColumnValue);
		RuntimeInfo mockRti = mock(RuntimeInfo.class);
		List<RuntimeInfo> mockRtiList = mock(List.class);
		getQueuedRuntimeInfo(null, mockRtiList);
		when(mockRtiList.get(anyInt())).thenReturn(mockRti);
		when(mockRti.getProperties()).thenReturn(proMap);
		ReflectionTestUtils.setField(jdbcTableHandler, "driverName", JdbcConstants.ORACLE_DRIVER_NAME);
		getRecordMapListBasedOnID(idColumnValue);
		jdbcTableHandler.incrementInvocationCount();
		when(jdbcInputDescriptor.getSnapshot()).thenReturn("yes");
		Assert.assertEquals(jdbcTableHandler.process(), Status.CALLBACK);
	}
	
	private void getQueuedRuntimeInfo(final List<RuntimeInfo> runtimeInfoList, final List<RuntimeInfo> addRuntimeInfoList) throws RuntimeInfoStoreException {
		@SuppressWarnings("unchecked")
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = mock(RuntimeInfoStore.class);
		RuntimeInfo mockRti = mock(RuntimeInfo.class);
		if(runtimeInfoList == null){
			when(runtimeInfoStore.getAll(anyString(), anyString(), any(RuntimeInfoStore.Status.class)))
				.thenReturn(runtimeInfoList).thenReturn(addRuntimeInfoList);
		} else {
			when(runtimeInfoStore.getAll(anyString(), anyString(), any(RuntimeInfoStore.Status.class)))
				.thenReturn(addRuntimeInfoList);
		}
		when(addRuntimeInfoList.get(anyInt())).thenReturn(mockRti);
		ReflectionTestUtils.setField(jdbcTableHandler, "runTimeInfoStore", runtimeInfoStore);
		when(runtimeInfoStore.put(any(RuntimeInfo.class))).thenReturn(true);
	}
	
	private void getRuntimeInfo(String dateValue, RuntimeInfo rti) throws RuntimeInfoStoreException {
		@SuppressWarnings("unchecked")
		RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = mock(RuntimeInfoStore.class);
		when(runtimeInfoStore.get(anyString(), anyString(), anyString())).thenReturn(rti);
		ReflectionTestUtils.setField(jdbcTableHandler, "runTimeInfoStore", runtimeInfoStore);
		when(runtimeInfoStore.put(any(RuntimeInfo.class))).thenReturn(true);
	}
	
	private RuntimeInfo createRuntimeInfo(String columnValue) throws RuntimeInfoStoreException {
		Map<String, String> properties = new HashMap<>();
		properties.put(jdbcInputDescriptor.getIncrementedBy(), columnValue);
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setAdaptorName("sql-adaptor");
		runtimeInfo.setEntityName("testTable");
		runtimeInfo.setStatus(RuntimeInfoStore.Status.QUEUED);
		runtimeInfo.setProperties(properties);
		return runtimeInfo;
	}
	
	private void getColumnValue(String dateValue) throws RuntimeInfoStoreException {
		RuntimeInfo runtimeInfo = createRuntimeInfo(dateValue);
		if(!dateValue.equalsIgnoreCase("1900-01-01 00:00:00")){
			getRuntimeInfo(dateValue, runtimeInfo);
		}
	}
	
	private void getRecordMapListBasedOnLUD(String columnValue) throws RuntimeInfoStoreException {
		String s = "test\ndata1";
		Object col = s;
		String dt = columnValue;
		Object lud = dt;
		List<Map<String, Object>> rowsList = new ArrayList<>();
		Map<String, Object> rowMap = new HashMap<>();
		rowMap.put("col1", col);
		rowMap.put("LUD", lud);
		rowsList.add(rowMap);
		@SuppressWarnings("unchecked")
		List<Map<String, Object>> ignoredRowsList = mock(List.class);
		when(jdbcTemplate.queryForList(anyString(), anyObject())).thenReturn(rowsList).thenReturn(ignoredRowsList);
		when(ignoredRowsList.size()).thenReturn(0);
		List<String> mockColumnList = new ArrayList<>();
		mockColumnList.add("col1");
		mockColumnList.add("LUD");
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockColumnList);
		when(jdbcInputDescriptor.getColumnName()).thenReturn("LUD");
		when(jdbcInputDescriptor.getRowDelimeter()).thenReturn("\u0001");
		when(jdbcInputDescriptor.getRowDelimeter()).thenReturn("\n");
	}
	
	private void getRecordMapListBasedOnID(String idValue) throws RuntimeInfoStoreException {
		getColumnValue(idValue);
		String s = "test\ndata1";
		Object col = s;
		String dt = idValue;
		Object lud = dt;
		String str = "2";
		Object lud1 = str;
		List<Map<String, Object>> rowsList = new ArrayList<>();
		Map<String, Object> rowMap = new HashMap<>();
		rowMap.put("col2", col);
		rowMap.put("ID", lud);
		rowsList.add(rowMap);
		List<Map<String, Object>> ignoredRowsList = new ArrayList<>();
		Map<String, Object> ignoredRowMap = new HashMap<>();
		ignoredRowMap.put("col2", col);
		ignoredRowMap.put("ID", lud1);
		ignoredRowsList.add(ignoredRowMap);
		ignoredRowMap.put("col2", col);
		ignoredRowMap.put("ID", lud1);
		ignoredRowsList.add(ignoredRowMap);
		ignoredRowMap.put("col2", col);
		ignoredRowMap.put("ID", lud);
		ignoredRowsList.add(ignoredRowMap);
		when(jdbcTemplate.queryForList(anyString(), anyObject())).thenReturn(rowsList).thenReturn(ignoredRowsList);
		List<String> mockColumnList = new ArrayList<>();
		mockColumnList.add("col2");
		mockColumnList.add("ID");
		when(jdbcInputDescriptor.getColumnList()).thenReturn(mockColumnList);
		when(jdbcInputDescriptor.getColumnName()).thenReturn("ID");
		when(jdbcInputDescriptor.getRowDelimeter()).thenReturn("\u0001");
		when(jdbcInputDescriptor.getRowDelimeter()).thenReturn("\n");
	}
}
