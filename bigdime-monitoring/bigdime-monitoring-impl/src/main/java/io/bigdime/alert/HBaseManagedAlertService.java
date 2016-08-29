/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.alert;

import static io.bigdime.common.constants.ApplicationConstants.ALERT_COLUMN_FAMILY_NAME;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_COMMENT_COLUMN;
//import static io.bigdime.common.constants.ApplicationConstants.ALERT_HBASE_TABLE_NAME;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_METADATA_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_STATUS;
import static io.bigdime.common.constants.ApplicationConstants.SOURCE_NAME;
import static io.bigdime.common.constants.ApplicationConstants.NOCOMMENT;
import static io.bigdime.common.constants.ApplicationConstants.EMPTYSTRING;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ADAPTOR_NAME_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_CAUSE_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_CODE_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_DATE_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_EXCEPTION_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_MESSAGE_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_NAME_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_SEVERITY_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_MESSAGE_CONTEXT_COLUMN;
import static io.bigdime.common.constants.ApplicationConstants.ALERT_ALERT_LOG_LEVEL_COLUMN;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.DataRetrievalSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * This implementation works with HBase alert store to interact with
 * {@link ManagedAlert}.
 * 
 * @author Neeraj Jain,Sandeep Reddy,Murthy
 * 
 */
@Component
@Scope("prototype")
public class HBaseManagedAlertService implements ManagedAlertService {
	private static final Logger logger = LoggerFactory
			.getLogger(HBaseManagedAlertService.class);

	@Autowired
	private HbaseManager hbaseManager;
	@Value("${hbase.alert.table}")
	private String alertTable;
	@Value("${monitoring.numberofdays}")
	private String numberOfDays;

	/**
	 * Get the list of alerts for the given request from HBase.
	 * 
	 * @throws AlertException
	 */
	@Override
	public AlertServiceResponse<ManagedAlert> getAlerts(
			AlertServiceRequest alertServiceRequest) throws AlertException {
		AlertServiceResponse<ManagedAlert> alertServiceResponse = new AlertServiceResponse<ManagedAlert>();
		List<ManagedAlert> managedAlertList = new ArrayList<ManagedAlert>();
		String startRow = alertServiceRequest.getAlertId() + "."
				+ String.valueOf(alertServiceRequest.getFromDate().getTime());
		Scan scan = new Scan(Bytes.toBytes(startRow));
		PageFilter limitFilter = new PageFilter(alertServiceRequest.getLimit());
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter logLevelFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_LOG_LEVEL_COLUMN,CompareOp.EQUAL,Bytes.toBytes("error"));
		SingleColumnValueFilter adaptorFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ADAPTOR_NAME_COLUMN,CompareOp.EQUAL,Bytes.toBytes(alertServiceRequest.getAlertId()));
		FilterList searchFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		if( alertServiceRequest.getSearch() !=null && !alertServiceRequest.getSearch().isEmpty()){
			SingleColumnValueFilter searchLogFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_LOG_LEVEL_COLUMN,CompareOp.EQUAL,new SubstringComparator(alertServiceRequest.getSearch()));
			SingleColumnValueFilter searchMessageFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_MESSAGE_COLUMN,CompareOp.EQUAL,new SubstringComparator(alertServiceRequest.getSearch()));
			SingleColumnValueFilter searchMessageContextFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_MESSAGE_CONTEXT_COLUMN,CompareOp.EQUAL,new SubstringComparator(alertServiceRequest.getSearch()));
			SingleColumnValueFilter searchDateFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_DATE_COLUMN ,CompareOp.EQUAL,new SubstringComparator(alertServiceRequest.getSearch()));			
			if("BLOCKER".matches("(?i:.*"+alertServiceRequest.getSearch()+".*)") || "MAJOR".matches("(?i:.*"+alertServiceRequest.getSearch()+".*)") ){
				SingleColumnValueFilter severityFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_SEVERITY_COLUMN,CompareOp.EQUAL,new SubstringComparator(alertServiceRequest.getSearch()));
				searchFilterList.addFilter(severityFilter);
			}else if("NORMAL".matches("(?i:.*"+alertServiceRequest.getSearch()+".*)")){
				SingleColumnValueFilter notABlcokerFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_SEVERITY_COLUMN,CompareOp.NOT_EQUAL,Bytes.toBytes("error"));
				SingleColumnValueFilter notAMajorFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_SEVERITY_COLUMN,CompareOp.NOT_EQUAL,Bytes.toBytes("major"));
				searchFilterList.addFilter(notABlcokerFilter);
				searchFilterList.addFilter(notAMajorFilter);
			}	
			searchFilterList.addFilter(searchLogFilter);
			searchFilterList.addFilter(searchMessageFilter);
			searchFilterList.addFilter(searchMessageContextFilter);
			searchFilterList.addFilter(searchDateFilter);	
			filterList.addFilter(searchFilterList);
			}
		filterList.addFilter(logLevelFilter);
		filterList.addFilter(adaptorFilter);
		filterList.addFilter(limitFilter);
		scan.setFilter(filterList);	
		scan.setCaching(1000);
		scan.setReversed(true);
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(alertTable).withScan(scan).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			ResultScanner scanner = hbaseManager.getResultScanner();
			if (scanner != null) {
				for (Result r : scanner) {					
					try {
						ManagedAlert manageAlert = getManagedAlert(r);
						if (manageAlert != null) {
							managedAlertList.add(manageAlert);
						}
					} catch (UnsupportedEncodingException e) {
						logger.warn(
								SOURCE_NAME,
								"Unable convert row's content from hbase into managedAlert as the result cannot be parsed",
								e.getMessage());
					}
				}
				scanner.close();
			}
		} catch (HBaseClientException | IOException e) {
			throw new AlertException("Unable to get data from HBase due to  "
					+ e.getMessage());
		}
		alertServiceResponse.setAlerts(managedAlertList);
		alertServiceResponse.setNumFound(managedAlertList.size());
		return alertServiceResponse;
	}

	/**
	 * Update the alert's status and comment.
	 * 
	 * @throws IOException
	 * 
	 * @throws JsonMappingException
	 * @throws JsonGenerationException
	 * @throws NoSuchAlgorithmException
	 */
	@Override
	public boolean updateAlert(Alert alertMessage, ALERT_STATUS alertStatus,
			String comment) throws AlertException {
		ObjectMapper om = new ObjectMapper();
		Long currentTime = alertMessage.getDateTime().getTime();
		String key = constructRowKey(alertMessage.getApplicationName(),
				String.valueOf(currentTime));
		Get get = new Get(key.getBytes(StandardCharsets.UTF_8));

		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(alertTable).withGet(get).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);

			String rowKeyValuefromHbase = null;
			byte[] hbaseManagerResultRow = hbaseManager.getResult().getRow();
			if (hbaseManagerResultRow != null) {
				rowKeyValuefromHbase = new String(hbaseManagerResultRow,
						StandardCharsets.UTF_8.toString());
			}
			if (!key.equals(rowKeyValuefromHbase)) {

				Put put = new Put(Bytes.toBytes(key));
				put.add(ALERT_COLUMN_FAMILY_NAME,
						ALERT_METADATA_COLUMN,
						Bytes.toBytes(om
								.writeValueAsString((ManagedAlert) alertMessage)));
				if (alertStatus != null) {
					put.add(ALERT_COLUMN_FAMILY_NAME, ALERT_STATUS,
							Bytes.toBytes(alertStatus.toString()));
				} else {
					put.add(ALERT_COLUMN_FAMILY_NAME, ALERT_STATUS,
							Bytes.toBytes(EMPTYSTRING));
				}
				if (comment != null) {
					put.add(ALERT_COLUMN_FAMILY_NAME, ALERT_COMMENT_COLUMN,
							Bytes.toBytes(comment));
				} else {
					put.add(ALERT_COLUMN_FAMILY_NAME, ALERT_COMMENT_COLUMN,
							Bytes.toBytes(NOCOMMENT));
				}
				DataInsertionSpecification.Builder dataInsertionSpecificationBuilder = new DataInsertionSpecification.Builder();
				DataInsertionSpecification dataInsertionSpecification = dataInsertionSpecificationBuilder
						.withTableName(alertTable).withtPut(put).build();
				hbaseManager.insertData(dataInsertionSpecification);
				return true;
			} else {
				String existingAlertStatus = new String(hbaseManager
						.getResult().getValue(ALERT_COLUMN_FAMILY_NAME,
								ALERT_STATUS),
						StandardCharsets.UTF_8.toString());
				if (alertStatus != null
						&& !existingAlertStatus.equals(alertStatus.toString())) {
					Put put = new Put(Bytes.toBytes(key));
					put.add(ALERT_COLUMN_FAMILY_NAME, ALERT_STATUS,
							Bytes.toBytes(alertStatus.toString()));
					put.add(ALERT_COLUMN_FAMILY_NAME, ALERT_COMMENT_COLUMN,
							Bytes.toBytes("This is updated alert Comment"));
					DataInsertionSpecification.Builder dataInsertionSpecificationBuilder = new DataInsertionSpecification.Builder();
					DataInsertionSpecification dataInsertionSpecification = dataInsertionSpecificationBuilder
							.withTableName(alertTable).withtPut(put).build();
					hbaseManager.insertData(dataInsertionSpecification);
					return true;
				} else {
					logger.info(SOURCE_NAME, "Update/Insert Alert",
							"The alert is aleady present in Hbase and no updates are made");
					return false;
				}

			}
		} catch (HBaseClientException | IOException e) {
			throw new AlertException("Unable to connect to Hbase"
					+ e.getMessage());
		}
	}

	private String constructRowKey(String alertName, String time) {
		return alertName + "." + time;
	}

	private ManagedAlert getManagedAlert(Result result)
			throws UnsupportedEncodingException {
		ManagedAlert managedAlert = new ManagedAlert();
		String applicationName = "";
		String messageContext = "";
		String alertSeverity = "";
		String alertMessage = "";
		String alertDateTime = "";
		String alertLogLevel = "";
		String alertCode = "";
		String alertCause = "";
		String alertComment = "";
		String alertStatus = "";
		if (result != null) {
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_ADAPTOR_NAME_COLUMN)) {
				applicationName = new String(result.getValue(
						ALERT_COLUMN_FAMILY_NAME, ALERT_ADAPTOR_NAME_COLUMN),
						StandardCharsets.UTF_8.toString());
			}
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_MESSAGE_CONTEXT_COLUMN)) {
				messageContext = new String(
						result.getValue(ALERT_COLUMN_FAMILY_NAME,
								ALERT_MESSAGE_CONTEXT_COLUMN),
						StandardCharsets.UTF_8.toString());
			}
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_ALERT_SEVERITY_COLUMN)) {
				alertSeverity = new String(result.getValue(
						ALERT_COLUMN_FAMILY_NAME, ALERT_ALERT_SEVERITY_COLUMN),
						StandardCharsets.UTF_8.toString());
			}
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_ALERT_LOG_LEVEL_COLUMN)) {
				alertLogLevel = new String(
						result.getValue(ALERT_COLUMN_FAMILY_NAME,
								ALERT_ALERT_LOG_LEVEL_COLUMN),
						StandardCharsets.UTF_8.toString());
			}
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_ALERT_MESSAGE_COLUMN)) {
				alertMessage = new String(result.getValue(
						ALERT_COLUMN_FAMILY_NAME, ALERT_ALERT_MESSAGE_COLUMN),
						StandardCharsets.UTF_8.toString());
			}
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_ALERT_DATE_COLUMN)) {
				alertDateTime = new String(result.getValue(
						ALERT_COLUMN_FAMILY_NAME, ALERT_ALERT_DATE_COLUMN),
						StandardCharsets.UTF_8.toString());
			}
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_ALERT_CODE_COLUMN)) {
				alertCode = new String(result.getValue(
						ALERT_COLUMN_FAMILY_NAME, ALERT_ALERT_CODE_COLUMN),
						StandardCharsets.UTF_8.toString());
			}
			if (result.containsColumn(ALERT_COLUMN_FAMILY_NAME,
					ALERT_ALERT_CODE_COLUMN)) {
				alertCause = new String(result.getValue(
						ALERT_COLUMN_FAMILY_NAME, ALERT_ALERT_CAUSE_COLUMN),
						StandardCharsets.UTF_8.toString());
			}

		}

		if (alertLogLevel.equalsIgnoreCase("ERROR")) {
			managedAlert
					.setAlertStatus(ManagedAlertService.ALERT_STATUS.ACKNOWLEDGED);
			managedAlert.setComment(alertComment);
			managedAlert.setMessageContext(messageContext);
			managedAlert.setLogLevel(alertLogLevel.toUpperCase());

			if (!applicationName.isEmpty()) {
				managedAlert.setApplicationName(applicationName);
			}
			 if (!messageContext.isEmpty()) {
			 managedAlert.setMessageContext(messageContext);
			 }
			if (!alertCode.isEmpty()) {
				if (alertCode.equalsIgnoreCase("BIG-0001")) {
					managedAlert.setType(ALERT_TYPE.ADAPTOR_FAILED_TO_START);
				} else if (alertCode.equalsIgnoreCase("BIG-0002")) {
					managedAlert.setType(ALERT_TYPE.INGESTION_FAILED);
				} else if (alertCode.equalsIgnoreCase("BIG-0003")) {
					managedAlert.setType(ALERT_TYPE.INGESTION_DID_NOT_RUN);
				} else if (alertCode.equalsIgnoreCase("BIG-0004")) {
					managedAlert.setType(ALERT_TYPE.INGESTION_STOPPED);
				} else if (alertCode.equalsIgnoreCase("BIG-0005")) {
					managedAlert.setType(ALERT_TYPE.DATA_FORMAT_CHANGED);
				} else if (alertCode.equalsIgnoreCase("BIG-9999")) {
					managedAlert.setType(ALERT_TYPE.OTHER_ERROR);
				}
			}

			if (!alertCause.isEmpty()) {
				if (alertCause
						.equalsIgnoreCase("adaptor configuration is invalid")) {
					managedAlert
							.setCause(ALERT_CAUSE.INVALID_ADAPTOR_CONFIGURATION);
				} else if (alertCause.equalsIgnoreCase("data validation error")) {
					managedAlert.setCause(ALERT_CAUSE.VALIDATION_ERROR);
				} else if (alertCause.equalsIgnoreCase("input message too big")) {
					managedAlert.setCause(ALERT_CAUSE.MESSAGE_TOO_BIG);
				} else if (alertCause
						.equalsIgnoreCase("unsupported data or character type")) {
					managedAlert.setCause(ALERT_CAUSE.UNSUPPORTED_DATA_TYPE);
				} else if (alertCause
						.equalsIgnoreCase("input data schema changed")) {
					managedAlert
							.setCause(ALERT_CAUSE.INPUT_DATA_SCHEMA_CHANGED);
				} else if (alertCause
						.equalsIgnoreCase("data could not be read from source")) {
					managedAlert.setCause(ALERT_CAUSE.INPUT_ERROR);
				} else if (alertCause.equalsIgnoreCase("internal error")) {
					managedAlert
							.setCause(ALERT_CAUSE.APPLICATION_INTERNAL_ERROR);
				} else if (alertCause
						.equalsIgnoreCase("shutdown command received")) {
					managedAlert.setCause(ALERT_CAUSE.SHUTDOWN_COMMAND);
				}
			}
			if (!alertSeverity.isEmpty()) {
				if (alertSeverity.equalsIgnoreCase("BLOCKER")) {
					managedAlert.setSeverity(ALERT_SEVERITY.BLOCKER);
				} else if (alertSeverity.equalsIgnoreCase("MAJOR")) {
					managedAlert.setSeverity(ALERT_SEVERITY.MAJOR);
				} else {
					managedAlert.setSeverity(ALERT_SEVERITY.NORMAL);
				}
			} else {
				managedAlert.setSeverity(ALERT_SEVERITY.NORMAL);
			}

			if (!alertMessage.isEmpty()) {
				managedAlert.setMessage(alertMessage);
			}

			if (!alertDateTime.isEmpty()) {
				try {
//					Date date = dateFormatHolder.get().parse(alertDateTime);
					Date date = new Date(alertDateTime);
					managedAlert.setDateTime(date);
				} catch (Exception e) {
					logger.info(SOURCE_NAME, "Creating Managed Alert",
							"Unable to parse the date " + e.getMessage());

				}
			}
			return managedAlert;
		}
		return null;
	}

	/**
	 * dateFormatHolder.get() used to get an instance of SimpleDateFormat object
	 */
	private static final ThreadLocal<SimpleDateFormat> dateFormatHolder = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("E MMM d HH:mm:ss 'UTC' yyyy");
		}
	};

	public List<Long> getDates(AlertServiceRequest alertServiceRequest)
			throws AlertException {
		List<Long> list = new ArrayList<Long>();
		String startRow = alertServiceRequest.getAlertId()
				+ "."
				+ String.valueOf(alertServiceRequest.getFromDate().getTime());
		Scan scan = new Scan(Bytes.toBytes(startRow));
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		SingleColumnValueFilter logLevelFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ALERT_LOG_LEVEL_COLUMN,CompareOp.EQUAL,Bytes.toBytes("error"));
		SingleColumnValueFilter adaptorFilter=new SingleColumnValueFilter(ALERT_COLUMN_FAMILY_NAME,ALERT_ADAPTOR_NAME_COLUMN,CompareOp.EQUAL,Bytes.toBytes(alertServiceRequest.getAlertId()));
		PageFilter limitFilter = new PageFilter(250);
		filterList.addFilter(logLevelFilter);
		filterList.addFilter(adaptorFilter);
		filterList.addFilter(limitFilter);
		filterList.addFilter(new KeyOnlyFilter());
		scan.setFilter(filterList);
		scan.setReversed(true);
		scan.setCaching(1000);
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(alertTable).withScan(scan).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			ResultScanner scanner = hbaseManager.getResultScanner();
			if (scanner != null) {
				int counter=0;
				for (Result result : scanner) {
				    String alertDateTime = new String(result.getRow());
					String datetime = StringUtils.remove(alertDateTime,
							alertServiceRequest.getAlertId() + ".");
					if(counter%25==0 || counter==0){
					list.add(Long.parseLong(datetime));
					}
					counter++;
				}
				scanner.close();
			}
			return list;
		} catch (HBaseClientException | IOException e) {
			throw new AlertException("Unable to get data from HBase due to  "
					+ e.getMessage());
		}

	}

}
