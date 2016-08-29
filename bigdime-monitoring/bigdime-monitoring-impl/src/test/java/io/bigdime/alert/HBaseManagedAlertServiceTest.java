/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_CODE_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_COLUMN_FAMILY_NAME;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_CAUSE_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_SEVERITY_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_DATE_COLUMN;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.ManagedAlertService.ALERT_STATUS;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.DataRetrievalSpecification;
import io.bigdime.hbase.client.HbaseManager;

/**
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */
@PrepareForTest({ DataInsertionSpecification.class })
public class HBaseManagedAlertServiceTest extends PowerMockTestCase {

	HBaseManagedAlertService hBaseManagedAlertService;

	@BeforeClass
	public void init() {
		hBaseManagedAlertService = new HBaseManagedAlertService();
	}

	@BeforeTest
	public void setup() {
		System.setProperty("env", "test");
	}

	@Test
	public void getAlertsTest() throws HBaseClientException, IOException,
			AlertException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);
		Iterator iterator = Mockito.mock(Iterator.class);
		Mockito.when(resultScanner.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
		Result result = Mockito.mock(Result.class);	
		Mockito.when(iterator.next()).thenReturn(result);
		Mockito.when(
				result.containsColumn(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(true);
		Mockito.when(
				result.getValue(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(
				"ERROR".getBytes());
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
				.getAlerts(alertServiceRequest);
		Assert.assertNotNull(alertServiceResponse.getAlerts());
		Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
		for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
			Assert.assertEquals(managedAlerts.getApplicationName()
					.toUpperCase(), "ERROR");
			Assert.assertEquals(
					managedAlerts.getMessageContext().toUpperCase(), "ERROR");
			Assert.assertEquals(managedAlerts.getMessage().toUpperCase(),
					"ERROR");
		}
	}

	@Test
	public void getAlertsParseExceptionTest() throws HBaseClientException,
			IOException, AlertException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);
		Iterator iterator = Mockito.mock(Iterator.class);
		Mockito.when(resultScanner.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
		Result result = Mockito.mock(Result.class);
		Mockito.when(iterator.next()).thenReturn(result);
		Mockito.when(
				result.containsColumn(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(true);
		Mockito.when(
				result.getValue(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(
				"ERROR".getBytes());
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
				.getAlerts(alertServiceRequest);
		Assert.assertNotNull(alertServiceResponse.getAlerts());
		Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
		for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
			Assert.assertNotNull(managedAlerts.getDateTime());
		}
	}

	@Test
	public void getAlertsUnsupportedEncodingExceptionTest()
			throws HBaseClientException, IOException, AlertException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);
		Iterator iterator = Mockito.mock(Iterator.class);
		Mockito.when(resultScanner.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
		Result result = Mockito.mock(Result.class);
		Mockito.when(iterator.next()).thenReturn(result);
		Mockito.when(
				result.containsColumn(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenThrow(
				UnsupportedEncodingException.class);
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
				.getAlerts(alertServiceRequest);
		Assert.assertNotNull(alertServiceResponse.getAlerts());
		Assert.assertTrue(alertServiceResponse.getAlerts().isEmpty());
		for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
			Assert.assertNull(managedAlerts.getApplicationName());
		}
	}

	@Test(expectedExceptions = AlertException.class)
	public void getAlertsHBaseClientExceptionTest()
			throws HBaseClientException, IOException, AlertException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doThrow(HBaseClientException.class).when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		hBaseManagedAlertService.getAlerts(alertServiceRequest);

	}

	@Test
	public void getManagedAlertTest() throws HBaseClientException, IOException,
			AlertException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);
		Iterator iterator = Mockito.mock(Iterator.class);
		Mockito.when(resultScanner.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
		Result result = Mockito.mock(Result.class);
		Mockito.when(iterator.next()).thenReturn(result);
		Mockito.when(
				result.containsColumn(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(true);
		Mockito.when(
				result.getValue(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(
				"ERROR".getBytes());
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
				.getAlerts(alertServiceRequest);
		Assert.assertNotNull(alertServiceResponse.getAlerts());
		Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
		for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
			Assert.assertEquals(managedAlerts.getApplicationName()
					.toUpperCase(), "ERROR");
			Assert.assertEquals(
					managedAlerts.getMessageContext().toUpperCase(), "ERROR");
			Assert.assertEquals(managedAlerts.getMessage().toUpperCase(),
					"ERROR");
			Assert.assertEquals(managedAlerts.getMessageContext(), "ERROR");
			Assert.assertEquals(managedAlerts.getLogLevel(), "ERROR");
		}
	}

	@Test
	public void getManagedAlertAlertTypeTest() throws AlertException,
			HBaseClientException, IOException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);

		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		List<String> alertCodeList = new ArrayList<String>();
		alertCodeList.add("BIG-0001");
		alertCodeList.add("BIG-0002");
		alertCodeList.add("BIG-0003");
		alertCodeList.add("BIG-0004");
		alertCodeList.add("BIG-0005");
		alertCodeList.add("BIG-9999");
		for (String alertCode : alertCodeList) {
			boolean alertValidator = false;
			Iterator iterator = Mockito.mock(Iterator.class);
			Mockito.when(resultScanner.iterator()).thenReturn(iterator);
			Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
			Result result = Mockito.mock(Result.class);
			Mockito.when(iterator.next()).thenReturn(result);
			Mockito.when(
					result.containsColumn(Mockito.any(byte[].class),
							Mockito.any(byte[].class))).thenReturn(true);
			Mockito.when(
					result.getValue(Mockito.any(byte[].class),
							Mockito.any(byte[].class))).thenReturn(
					"ERROR".getBytes());

			Mockito.when(
					result.getValue(ALERT_COLUMN_FAMILY_NAME,
							ALERT_ALERT_CODE_COLUMN)).thenReturn(
					alertCode.getBytes());

			AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
					.getAlerts(alertServiceRequest);
			Assert.assertNotNull(alertServiceResponse.getAlerts());
			Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
			for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
				for (ALERT_TYPE alertType : ALERT_TYPE.values()) {
					if (managedAlerts.getType().equals(alertType)) {
						alertValidator = true;
						break;
					}
				}
				break;
			}
			Assert.assertTrue(alertValidator);

		}
	}

	@Test
	public void getManagedAlertAlertCauseTest() throws AlertException,
			HBaseClientException, IOException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);

		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		List<String> alertCauseList = new ArrayList<String>();
		alertCauseList.add("adaptor configuration is invalid");
		alertCauseList.add("input message too big");
		alertCauseList.add("unsupported data or character type");
		alertCauseList.add("input data schema changed");
		alertCauseList.add("data could not be read from source");
		alertCauseList.add("internal error");
		alertCauseList.add("shutdown command received");
		for (String alertCause : alertCauseList) {
			boolean alertValidator = false;
			Iterator iterator = Mockito.mock(Iterator.class);
			Mockito.when(resultScanner.iterator()).thenReturn(iterator);
			Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
			Result result = Mockito.mock(Result.class);
			Mockito.when(iterator.next()).thenReturn(result);
			Mockito.when(
					result.containsColumn(Mockito.any(byte[].class),
							Mockito.any(byte[].class))).thenReturn(true);
			Mockito.when(
					result.getValue(Mockito.any(byte[].class),
							Mockito.any(byte[].class))).thenReturn(
					"ERROR".getBytes());

			Mockito.when(
					result.getValue(ALERT_COLUMN_FAMILY_NAME,
							ALERT_ALERT_CAUSE_COLUMN)).thenReturn(
					alertCause.getBytes());

			AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
					.getAlerts(alertServiceRequest);
			Assert.assertNotNull(alertServiceResponse.getAlerts());
			Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
			for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
				for (ALERT_CAUSE alertsCause : ALERT_CAUSE.values()) {
					if (managedAlerts.getCause().equals(alertsCause)) {
						alertValidator = true;
						break;
					}
				}
				break;
			}
			Assert.assertTrue(alertValidator);

		}
	}

	@Test
	public void getManagedAlertAlertSeverityTest() throws AlertException,
			HBaseClientException, IOException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);

		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		List<String> alertSeverityList = new ArrayList<String>();
		alertSeverityList.add("BLOCKER");
		alertSeverityList.add("MAJOR");
		alertSeverityList.add("NORMAL");
		for (String alertCause : alertSeverityList) {
			boolean alertValidator = false;
			Iterator iterator = Mockito.mock(Iterator.class);
			Mockito.when(resultScanner.iterator()).thenReturn(iterator);
			Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
			Result result = Mockito.mock(Result.class);
			Mockito.when(iterator.next()).thenReturn(result);
			Mockito.when(
					result.containsColumn(Mockito.any(byte[].class),
							Mockito.any(byte[].class))).thenReturn(true);
			Mockito.when(
					result.getValue(Mockito.any(byte[].class),
							Mockito.any(byte[].class))).thenReturn(
					"ERROR".getBytes());

			Mockito.when(
					result.getValue(ALERT_COLUMN_FAMILY_NAME,
							ALERT_ALERT_SEVERITY_COLUMN)).thenReturn(
					alertCause.getBytes());

			AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
					.getAlerts(alertServiceRequest);
			Assert.assertNotNull(alertServiceResponse.getAlerts());
			Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
			for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
				for (ALERT_SEVERITY alertsSeverity : ALERT_SEVERITY.values()) {
					if (managedAlerts.getSeverity().equals(alertsSeverity)) {
						alertValidator = true;
						break;
					}
				}
				break;
			}
			Assert.assertTrue(alertValidator);

		}
	}

	//@Test
	public void getManagedAlertAlertDateTimeTest() throws AlertException,
			HBaseClientException, IOException, ParseException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		Iterator iterator = Mockito.mock(Iterator.class);
		Mockito.when(resultScanner.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
		Result result = Mockito.mock(Result.class);
		Mockito.when(iterator.next()).thenReturn(result);
		Mockito.when(
				result.containsColumn(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(true);
		Mockito.when(
				result.getValue(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(
				"ERROR".getBytes());

		Mockito.when(
				result.getValue(ALERT_COLUMN_FAMILY_NAME,
						ALERT_ALERT_DATE_COLUMN)).thenReturn(
				"Sat Jan 01 00:00:01 UTC 2015".getBytes());
		AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
				.getAlerts(alertServiceRequest);
		Assert.assertNotNull(alertServiceResponse.getAlerts());
		Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
		for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
			Assert.assertEquals(new SimpleDateFormat("E MMM d HH:mm:ss 'UTC' yyyy").parse("Sat Jan 01 00:00:01 UTC 2015").getTime(),managedAlerts.getDateTime().getTime() );
		}

	}

	@Test
	public void getManagedAlertAlertDateTimeParseExceptionTest()
			throws AlertException, HBaseClientException, IOException {
		AlertServiceRequest alertServiceRequest = new AlertServiceRequest();
		alertServiceRequest.setAlertId("test");
		alertServiceRequest.setFromDate(new Date());
		alertServiceRequest.setToDate(new Date());
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		ResultScanner resultScanner = Mockito.mock(ResultScanner.class);
		Mockito.when(hbaseManager.getResultScanner()).thenReturn(resultScanner);
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		Iterator iterator = Mockito.mock(Iterator.class);
		Mockito.when(resultScanner.iterator()).thenReturn(iterator);
		Mockito.when(iterator.hasNext()).thenReturn(true).thenReturn(false);
		Result result = Mockito.mock(Result.class);
		Mockito.when(iterator.next()).thenReturn(result);
		Mockito.when(
				result.containsColumn(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(true);
		Mockito.when(
				result.getValue(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(
				"ERROR".getBytes());

		Mockito.when(
				result.getValue(ALERT_COLUMN_FAMILY_NAME,
						ALERT_ALERT_DATE_COLUMN)).thenReturn(
				"Sat Jan 01 00:00:01 UT 2015".getBytes());
		AlertServiceResponse<ManagedAlert> alertServiceResponse = hBaseManagedAlertService
				.getAlerts(alertServiceRequest);
		Assert.assertNotNull(alertServiceResponse.getAlerts());
		Assert.assertFalse(alertServiceResponse.getAlerts().isEmpty());
		for (ManagedAlert managedAlerts : alertServiceResponse.getAlerts()) {
			Assert.assertNotEquals("1 Jan 2015 08:00:01 GMT", managedAlerts
					.getDateTime().toGMTString());
		}

	}

	@Test
	public void updateAlertForNonEmptyRowTest() throws HBaseClientException,
			IOException, AlertException {
		hBaseManagedAlertService = new HBaseManagedAlertService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result = Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenReturn(result);
		Mockito.when(result.getRow()).thenReturn(
				"test".getBytes(StandardCharsets.UTF_8));
		DataInsertionSpecification dataInsertionSpecification = PowerMockito
				.mock(DataInsertionSpecification.class);
		Mockito.doNothing().when(hbaseManager)
				.insertData(dataInsertionSpecification);
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		ManagedAlert alertMessage = new ManagedAlert();
		alertMessage.setApplicationName("test");
		alertMessage.setDateTime(new Date());
		alertMessage.setAlertStatus(ALERT_STATUS.ACKNOWLEDGED);
		Assert.assertTrue(hBaseManagedAlertService.updateAlert(alertMessage,
				ALERT_STATUS.ACKNOWLEDGED, "test"));
	}

	@Test
	public void updateAlertStatusAndCommentForExisitingAlertTest()
			throws HBaseClientException, IOException, AlertException {
		hBaseManagedAlertService = new HBaseManagedAlertService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result = Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenReturn(result);
		Mockito.when(result.getRow()).thenReturn(
				("test" + "." + new Date(2015 - 01 - 01).getTime())
						.getBytes(StandardCharsets.UTF_8));
		Mockito.when(
				result.getValue(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(
				"test".getBytes(StandardCharsets.UTF_8));
		DataInsertionSpecification dataInsertionSpecification = PowerMockito
				.mock(DataInsertionSpecification.class);
		Mockito.doNothing().when(hbaseManager)
				.insertData(dataInsertionSpecification);
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		ManagedAlert alertMessage = new ManagedAlert();
		alertMessage.setApplicationName("test");
		alertMessage.setDateTime(new Date(2015 - 01 - 01));
		alertMessage.setAlertStatus(ALERT_STATUS.ACKNOWLEDGED);
		Assert.assertTrue(hBaseManagedAlertService.updateAlert(alertMessage,
				ALERT_STATUS.ACKNOWLEDGED, "test"));
	}

	@Test
	public void updateAlertStatusAndCommentForExisitingAlertWithUpdatedValuesTest()
			throws HBaseClientException, IOException, AlertException {
		hBaseManagedAlertService = new HBaseManagedAlertService();
		HbaseManager hbaseManager = Mockito.mock(HbaseManager.class);
		Mockito.doNothing().when(hbaseManager)
				.retreiveData((DataRetrievalSpecification) Mockito.any());
		Result result = Mockito.mock(Result.class);
		Mockito.when(hbaseManager.getResult()).thenReturn(result);
		Mockito.when(result.getRow()).thenReturn(
				("test" + "." + new Date(2015 - 01 - 01).getTime())
						.getBytes(StandardCharsets.UTF_8));
		Mockito.when(
				result.getValue(Mockito.any(byte[].class),
						Mockito.any(byte[].class))).thenReturn(
				"ACKNOWLEDGED".getBytes(StandardCharsets.UTF_8));
		DataInsertionSpecification dataInsertionSpecification = PowerMockito
				.mock(DataInsertionSpecification.class);
		Mockito.doNothing().when(hbaseManager)
				.insertData(dataInsertionSpecification);
		ReflectionTestUtils.setField(hBaseManagedAlertService, "hbaseManager",
				hbaseManager);
		ManagedAlert alertMessage = new ManagedAlert();
		alertMessage.setApplicationName("test");
		alertMessage.setDateTime(new Date(2015 - 01 - 01));
		alertMessage.setAlertStatus(ALERT_STATUS.ACKNOWLEDGED);
		Assert.assertFalse(hBaseManagedAlertService.updateAlert(alertMessage,
				ALERT_STATUS.ACKNOWLEDGED, "test"));
	}
}
