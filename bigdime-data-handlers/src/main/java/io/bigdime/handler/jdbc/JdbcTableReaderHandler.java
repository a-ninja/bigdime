/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

//import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang3.SerializationUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;


/**
 * This class processes each table record in database based on sql query, 
 * inserting into Metastore, updating Runtime Info
 * 
 *@author Murali Namburi, Pavan Sabinikari, Rita Liu
 *
 */

@Component
@Scope("prototype")
public class JdbcTableReaderHandler extends AbstractSourceHandler {
	
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(JdbcTableReaderHandler.class));
	
	@Autowired
	private JdbcInputDescriptor jdbcInputDescriptor;
	@Autowired
	private DataSource lazyConnectionDataSourceProxy;
	@Autowired
	private MetadataStore metadataStore;
	@Autowired
	RuntimeInfoStore<RuntimeInfo> runTimeInfoStore;
	@Value("${database.driverClassName}")
	private String driverName;
	@Value("${hiveDBName}")
	private String hiveDBName;
	@Value("${split.size}")
	private String splitSize;
	private static String initialRuntimeDateEntry="1900-01-01 00:00:00";
	private JdbcTemplate jdbcTemplate;
	private String handlerPhase = null;
	private String jsonStr = null;
	private String columnValue;
	private String highestIncrementalColumnValue;
	private String processTableSql;
	private Boolean processDirty = false;
	@Autowired
	private JdbcMetadataManagement jdbcMetadataManagment;
	
	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building Jdbc Table Reader Handler";
		super.build();
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());
		jdbcTemplate = new JdbcTemplate(lazyConnectionDataSourceProxy);
		@SuppressWarnings("unchecked")
		Entry<String, String> srcDescInputs = (Entry<String, String>) getPropertyMap()
				.get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		if (srcDescInputs == null) {
			throw new InvalidValueConfigurationException(
					"src-desc can't be null");
		}
		logger.info(handlerPhase,
				"entity:fileNamePattern={} input_field_name={}",
				srcDescInputs.getKey(), srcDescInputs.getValue());
		jsonStr = srcDescInputs.getKey();
		try {
			jdbcInputDescriptor.parseDescriptor(jsonStr);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					"incorrect value specified in src-desc, value must be in json string format, wrong json: " + jsonStr);
		}
	}
	
	/**
	 * Process method is process data from table and return status to handler manager
	 * 
	 * @return Status READY, BACKOFF, CALLBACK
	 * @throws HandlerException
	 */
	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing Jdbc Table Reader Handler";
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());
		incrementInvocationCount();
		try {
			return preProcess();
		} catch (RuntimeInfoStoreException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,
					"\"jdbcAdaptor RuntimeInfoStore exception\"  TableName = {} error={}",
					jdbcInputDescriptor.getEntityName(), e.toString());
			throw new HandlerException("Unable to process records from table "+jdbcInputDescriptor.getEntityName()+"", e);
		} catch (JdbcHandlerException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"jdbcAdaptor jdbcHandler exception\" TableName={} error={}",
					jdbcInputDescriptor.getEntityName(), e.toString());
			throw new HandlerException("Unable to process records from table "+jdbcInputDescriptor.getEntityName()+"", e);
		}
	}
	
	public void setDataSource(DataSource dataSource) {
		this.lazyConnectionDataSourceProxy = dataSource;
	}
		
	/**
	 * This preProcess method is get table from previous handler or from inputs, format sql query.
	 * get source metadata from source database, and put into bigdime metastore
	 * check runtimeInfoStore if table already exists or not, if so, call processRecords(), 
	 * else inserting runtimeInfoStore, call processRecords().
	 * 
	 * @return Status READY, CALLBACK
	 * @throws RuntimeInfoStoreException
	 * @throws JdbcHandlerException
	 */
	@Override
	protected Status preProcess() throws RuntimeInfoStoreException, JdbcHandlerException {
		List<ActionEvent> actionEvents = null;
		//this is for database level, process tables from a database, check table name, if null, get from event list
		if(jdbcInputDescriptor.getInputType().equalsIgnoreCase(JdbcConstants.DB_FLAG)){
			try{
				actionEvents = getHandlerContext().getEventList();
				Preconditions.checkNotNull(actionEvents, "ActionEvents can't be null");
			} catch(Exception e) {
				throw new JdbcHandlerException("ActionEvents cannot be null or empty");
			}
			for(ActionEvent actionEvent : actionEvents){
				String entityName = actionEvent.getHeaders().get(ActionEventHeaderConstants.TARGET_ENTITY_NAME);
				jdbcInputDescriptor.setTargetEntityName(entityName);
				jdbcInputDescriptor.setEntityName(entityName);
			}
		}
		jdbcInputDescriptor.setTargetDBName(hiveDBName);
		processTableSql = jdbcInputDescriptor.formatProcessTableQuery(jdbcInputDescriptor.getDatabaseName(), jdbcInputDescriptor.getEntityName(), driverName);
		//format sql query to get source metadata from source db
		if(!StringUtils.isEmpty(processTableSql)){
			logger.debug("Formatted Jdbc Table Reader Handler Query", "processTableSql={}", processTableSql);
			// Get Source Metadata..
			try {
				Metasegment metasegment = jdbcMetadataManagment.getSourceMetadata(
							jdbcInputDescriptor, jdbcTemplate);
				jdbcMetadataManagment.setColumnList(jdbcInputDescriptor,
							metasegment);

				logger.debug("Jdbc Table Reader Handler source column list",
							"ColumnList={}", jdbcInputDescriptor.getColumnList());
				
				if (jdbcInputDescriptor.getColumnList().size() == JdbcConstants.INTEGER_CONSTANT_ZERO)
					throw new JdbcHandlerException(
						"Unable to retrieve the column list for the table Name = "
									+ jdbcInputDescriptor.getEntityName());
				
				// throw Exception:
				// if the incrementedBy column doesn't exist in the table..
				if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
						&& jdbcInputDescriptor.getIncrementedColumnType() == null) {
					throw new JdbcHandlerException(
							"IncrementedBy Value doesn't exist in the table column list");
				}
				// Put into bigdime Metadata...
				metadataStore.put(metasegment);
			} catch (MetadataAccessException e) {
				throw new JdbcHandlerException(
					"Unable to put metadata to Metastore for the table Name = "
								+ jdbcInputDescriptor.getEntityName());
			}
		} else{
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"processed table query is null\" TableName={} ",
					jdbcInputDescriptor.getEntityName());
			throw new JdbcHandlerException("Table query is null, unable to process records from table "+jdbcInputDescriptor.getEntityName());
		}
		boolean processFlag = false;
		// Check if Runtime details Exists..
		if (getOneQueuedRuntimeInfo(runTimeInfoStore,jdbcInputDescriptor.getEntityName()) == null) {
			// Insert into Runtime Data...
			if (jdbcInputDescriptor.getIncrementedBy() != null && driverName
					.equalsIgnoreCase(JdbcConstants.ORACLE_DRIVER_NAME)) {
				HashMap<String, String> properties = new HashMap<String, String>();
				if (jdbcInputDescriptor.getIncrementedColumnType()
								.equalsIgnoreCase("DATE") || jdbcInputDescriptor
								.getIncrementedColumnType().equalsIgnoreCase(
										"TIMESTAMP")) {
					properties.put(jdbcInputDescriptor.getIncrementedBy(),
							initialRuntimeDateEntry);
				} else {
					properties.put(jdbcInputDescriptor.getIncrementedBy(),
							JdbcConstants.INTEGER_CONSTANT_ZERO + "");
				}
				//update to runtimeInfoStore
				boolean runtimeInsertionFlag = updateRuntimeInfo(
						runTimeInfoStore, jdbcInputDescriptor.getEntityName(),
						jdbcInputDescriptor.getIncrementedColumnType(),
						RuntimeInfoStore.Status.QUEUED, properties);

				logger.info(
						"Jdbc Table Reader Handler inserting Runtime data",
						"tableName={} PropertyKey={} PropertyValue={} status={}",
						jdbcInputDescriptor.getEntityName(),
						jdbcInputDescriptor.getIncrementedBy(), columnValue,
						runtimeInsertionFlag);
			} 
			//call processRecords() to get status READY, CALLBACK
			processFlag = processRecords();
		} else {
			logger.debug(
					"Jdbc Table Reader Handler processing an existing table ",
					"tableName={}", jdbcInputDescriptor.getEntityName());
			//call processRecords() to get status READY, CALLBACK
			processFlag = processRecords();
		}
		if (processFlag) {
			return Status.CALLBACK;
		} else {
			return Status.READY;
		}
	}
	
	/**
	 * This methods fetches the number of records from the source.
	 * 
	 * @return
	 */
	private boolean processRecords() {
		boolean moreRecordsExists = true;
		//get column value or index value that needs to fetch records from source
		String repoColumnValue = getCurrentColumnValue();
		logger.debug("Jdbc Table Reader Handler in process records",
				"Latest Incremented Repository Value= {}", repoColumnValue);
		if (repoColumnValue != null)
			columnValue = repoColumnValue;
		logger.info("Jdbc Table Reader Handler in processing ",
				"actual processTableSql={} latestIncrementalValue={}", processTableSql, columnValue);
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
		jdbcTemplate.setQueryTimeout(120);
		long startTime = System.currentTimeMillis();
		//get record rows from source based on query split size
		if (processTableSql.contains(JdbcConstants.QUERY_PARAMETER)) {
			if (columnValue != null) {
				
					if (driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO
							&& (jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE") || jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("TIMESTAMP"))) {
						
						rows = jdbcTemplate
								.queryForList(
										processTableSql,
										new Object[] { Timestamp.valueOf(columnValue)});
						;
												
					} else {
						rows = jdbcTemplate.queryForList(processTableSql,
								new Object[] { columnValue });
					}
				
			} else
				rows = jdbcTemplate.queryForList(processTableSql,
						new Object[] { JdbcConstants.INTEGER_CONSTANT_ZERO });
		} else {
			rows = jdbcTemplate.queryForList(processTableSql);
		}
		long endTime = System.currentTimeMillis();
		logger.info(
				"Jdbc Table Reader Handler during processing records",
				"Time taken to fetch records from table ={} from a columnValue={} with a fetchSize={} is {} milliseconds",
				jdbcInputDescriptor.getEntityName(), columnValue, splitSize,
				(endTime - startTime));
		logger.debug("Jdbc Table Reader Handler during processing records","columns in the tableName={} are {}",
				jdbcInputDescriptor.getEntityName(),jdbcInputDescriptor.getColumnList());

		long processStartTime = System.currentTimeMillis();
		// Process each row to HDFS...
		if (rows.size() > 0) {
			processEachRecord(rows);
		} else {
			logger.info("Jdbc Table Reader Handler during processing records",
					"No more rows found for query={}", processTableSql);
			moreRecordsExists = false;
		}
		// Assigning the current incremental value..
		if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
				&& highestIncrementalColumnValue != null) {
			HashMap<String, String> properties = new HashMap<String, String>();
			properties.put(jdbcInputDescriptor.getIncrementedBy(), highestIncrementalColumnValue);
			boolean highestValueStatus=false;
			try {
				highestValueStatus = updateRuntimeInfo(
						runTimeInfoStore,
						jdbcInputDescriptor.getEntityName(),
						jdbcInputDescriptor.getIncrementedColumnType(),
						RuntimeInfoStore.Status.QUEUED, properties);
			} catch (RuntimeInfoStoreException e) {
				
				logger.alert(ALERT_TYPE.INGESTION_FAILED,
						ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
						ALERT_SEVERITY.BLOCKER,
						"\"jdbcAdaptor RuntimeInfo Update exception\" TableName={} error={}",
						jdbcInputDescriptor.getEntityName(), e.toString());
			}
			logger.info("Jdbc Table Reader Handler saved highest incremental value ="+highestIncrementalColumnValue+"",
					"incremental column saved status(true/false)? {}", highestValueStatus);
			columnValue = highestIncrementalColumnValue;
		}
		
		long processEndTime = System.currentTimeMillis();
		logger.info("Total time taken for data processing" , "for columnValue = {} finished in {} milliseconds", columnValue, (processEndTime-processStartTime));
		
		return moreRecordsExists;
	}

	/**
	 * This method sets each record in rows for further Data cleansing
	 * 
	 * @param rows
	 */
	private void processEachRecord(List<Map<String, Object>> rows) {
        String maxValue = null;
        List<Map<String, Object>> maxList = new ArrayList<Map<String, Object>>();
		long startTime = System.currentTimeMillis();
		//get highest column value
		highestIncrementalColumnValue = rows.get(rows.size()-1).get(jdbcInputDescriptor.getColumnList()
					.get(jdbcInputDescriptor.getColumnList().indexOf(jdbcInputDescriptor.getColumnName())))+ "";
		for(int i = rows.size()-1; i>=0; i--){
			maxValue = rows.get(i).get(jdbcInputDescriptor
					.getColumnList().get(
							jdbcInputDescriptor.getColumnList().indexOf(
									jdbcInputDescriptor.getColumnName())))
					+ "";
			if(maxValue.equals(highestIncrementalColumnValue)){
				maxList.add(rows.get(i));
			} else{ 
				break;
			}
		}
		List<Map<String, Object>> ignoredRowsList = getIgnoredBatchRecords(processTableSql.replaceAll(">","="), maxList, highestIncrementalColumnValue);
		if(ignoredRowsList != null && ignoredRowsList.size() > 0){
			rows.addAll(ignoredRowsList);
		}
		if(rows.size() > 0){
			logger.info("Processing event list each time" , "rows.size ={} for highestIncrementalColumnValue={}", 
					rows.size(), highestIncrementalColumnValue);
			processIt(rows);
		}
		long endTime = System.currentTimeMillis();
		logger.info("Total time taken for data cleansing", "for highestIncrementalColumnValue={} finished in {} milliseconds", highestIncrementalColumnValue, (endTime-startTime));
		
	}
	
	/**
	 * Processes each record to Channel
	 * 
	 * @param actionEvents
	 * @return
	 * @throws HandlerException
	 */
	private void processIt(List<Map<String, Object>> rowsToClean){
		long startTime = System.currentTimeMillis();
		logger.info("processIt start processing action events", "highestIncrementalColumnValue = {}, actionEvents.size ={}", highestIncrementalColumnValue, rowsToClean.size());
		Pattern p = Pattern.compile(JdbcConstants.FIELD_CHARACTERS_TO_REPLACE);
		while (!rowsToClean.isEmpty()) {
			Map<String, Object> row = rowsToClean.remove(0);
			StringBuilder sbFormattedRowContent = new StringBuilder();
			StringBuilder sbHiveNonPartitionColumns = new StringBuilder();
			String datePartition = null;
			if (row != null) {		
				for (int columnNamesListCount = 0; columnNamesListCount < jdbcInputDescriptor
					.getColumnList().size(); columnNamesListCount++) {
					// Ensure each field doesn't have rowlineDelimeter
					StringBuffer sbfMatcher = new StringBuffer();					
					Matcher m = p.matcher(sbfMatcher.append(row.get(jdbcInputDescriptor.getColumnList().
									get(columnNamesListCount))));
					StringBuffer sbFormattedField = new StringBuffer();
					while (m.find()) {
						m.appendReplacement(sbFormattedField, JdbcConstants.FIELD_CHARACTERS_REPLACE_BY);
					}
					m.appendTail(sbFormattedField);
					sbFormattedRowContent.append(sbFormattedField);
					if (columnNamesListCount != jdbcInputDescriptor.getColumnList().size() - 1)
							sbFormattedRowContent.append(jdbcInputDescriptor.getFieldDelimeter());
	
					if (jdbcInputDescriptor.getIncrementedColumnType().indexOf("DATE") >= JdbcConstants.INTEGER_CONSTANT_ZERO
							    || jdbcInputDescriptor.getIncrementedColumnType().indexOf("TIMESTAMP") >= JdbcConstants.INTEGER_CONSTANT_ZERO) {
	
						if (jdbcInputDescriptor.getColumnList().get(columnNamesListCount)
								.equalsIgnoreCase(jdbcInputDescriptor.getIncrementedBy())) {
							
							datePartition = Timestamp.valueOf(row
									.get(jdbcInputDescriptor.getColumnList().get(columnNamesListCount))
									.toString()).toString().substring(0,10).replaceAll("-","");
						}
					}
				}
				//Each Row is delimited by "\n"
				sbFormattedRowContent.append(jdbcInputDescriptor.getRowDelimeter());
				ActionEvent actionEvent = new ActionEvent();
				actionEvent.setBody(sbFormattedRowContent.toString().getBytes());
	
				
				sbHiveNonPartitionColumns.append(ActionEventHeaderConstants.ENTITY_NAME);
				actionEvent.getHeaders().put(ActionEventHeaderConstants.ENTITY_NAME,
						             jdbcInputDescriptor.getTargetEntityName());
				
				actionEvent.getHeaders().put(ActionEventHeaderConstants.LINES_TERMINATED_BY,
									 jdbcInputDescriptor.getRowDelimeter());
				
				actionEvent.getHeaders().put(ActionEventHeaderConstants.FIELDS_TERMINATED_BY,
									 jdbcInputDescriptor.getFieldDelimeter());
	
				// Partition Dates logic..
				if (jdbcInputDescriptor.getSnapshot() != null
									  && jdbcInputDescriptor.getSnapshot().equalsIgnoreCase("YES")) {
					// get current date and format it to string
					actionEvent.getHeaders().put(ActionEventHeaderConstants.DATE, 
										 dateFormatHolder.get().format(new Date()));
				} else {
					if (datePartition != null) {
							actionEvent.getHeaders().put(ActionEventHeaderConstants.DATE, datePartition);
					} else
							actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_REQUIRED, "false");
					
				}
				
				if (actionEvent.getHeaders().get(ActionEventHeaderConstants.DATE) != null){
						actionEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, 
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.DATE));
				} else{
					actionEvent.getHeaders().put(ActionEventHeaderConstants.INPUT_DESCRIPTOR, 
										 actionEvent.getHeaders().get(ActionEventHeaderConstants.ENTITY_NAME) +" "+ JdbcConstants.WITH_NO_PARTITION);
				}  
				actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_NON_PARTITION_NAMES, 
					    		 sbHiveNonPartitionColumns.toString());
				actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_NAME, jdbcInputDescriptor.getEntityName().toLowerCase());
				if(processDirty){
					actionEvent.getHeaders().put(ActionEventHeaderConstants.CLEANUP_REQUIRED, Boolean.toString(processDirty));	
				}
				processDirty = false;
				/*
				 * Check for outputChannel map. get the eventList of channels.
				 * check the criteria and put the message.
				 */
				if (getOutputChannel() != null) {
					getOutputChannel().put(actionEvent);
				}
			}
			
		}
		long endTime = System.currentTimeMillis();
	
		logger.info("Data Cleansing during processing records","Time taken to process action Events for highestIncrementalColumnValue ={} is ={} milliseconds",
				highestIncrementalColumnValue, (endTime - startTime));
	}
	
	
	/**
	 * dateFormatHolder.get() used to get an instance of SimpleDateFormat object
	 */
	private static final ThreadLocal<SimpleDateFormat> dateFormatHolder = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMdd");
		}
	};
	
	/**
	 * get current column value from runtime info
	 * @param runtimeInfo
	 * @return column value
	 */
	private String getCurrentColumnValue() {
		String currentIncrementalColumnValue = null;
		try{
			 RuntimeInfo runtimeInfo = getOneQueuedRuntimeInfo(runTimeInfoStore,jdbcInputDescriptor.getEntityName());
			if (runtimeInfo != null){
				currentIncrementalColumnValue = runtimeInfo.getProperties().get(
						jdbcInputDescriptor.getIncrementedBy());
			} else{
				logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
						ALERT_SEVERITY.BLOCKER,
						"\"jdbcAdaptor RuntimeInfo is null\" TableName:{}",
						jdbcInputDescriptor.getEntityName());
			}
		} catch (RuntimeInfoStoreException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,
					"\"jdbcAdaptor RuntimeInfoStore exception while getting current value\" TableName:{} error={}",
					jdbcInputDescriptor.getEntityName(),e.toString());
		}
		return handleDirtyRecordsConditions(currentIncrementalColumnValue);
	}
	
	/**
	 * handle the stop/restart adaptor case
	 * 
	 * @param columnValue
	 * @return column value
	 */
	private String handleDirtyRecordsConditions(String columnValue) {
		if(isFirstRun() && !columnValue.equalsIgnoreCase(initialRuntimeDateEntry)){
			try {
				RuntimeInfo runtimeInfo = runTimeInfoStore.get(AdaptorConfig.getInstance().getName(), jdbcInputDescriptor.getEntityName(), columnValue.replaceAll("-", ""));
				if(runtimeInfo == null){
					RuntimeInfo rti = runTimeInfoStore.getLatest(AdaptorConfig.getInstance().getName(), jdbcInputDescriptor.getEntityName());
					if(!rti.getInputDescriptor().equalsIgnoreCase("DATE")){
						String updateColumnValue = rti.getInputDescriptor();
						Date d = new SimpleDateFormat("yyyyMMdd").parse(updateColumnValue);
						updateColumnValue = new SimpleDateFormat("yyyy-MM-dd").format(d);
						columnValue = updateColumnValue;
					} else{
						columnValue = initialRuntimeDateEntry;
						processDirty = true;
						return columnValue;
					}
				} 
				Date date = new SimpleDateFormat("yyyy-MM-dd").parse(columnValue);
				logger.info("processing dirty records", "dirty record column value={}", columnValue);
				processDirty = true;
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(date);
				calendar.add(Calendar.SECOND, -1);
				columnValue = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s").format(calendar.getTime());	
			} 
			catch (ParseException e) {
				logger.warn("ParseException, cannot parse the value= " +columnValue, "error={}", e.toString());
			} catch (RuntimeInfoStoreException e) {
				logger.warn("RuntimeInfoStoreException for = " +columnValue, "error={}", e.toString());
			}
		} else {
			logger.info("processing a clean record", "no dirty record found for table {}", jdbcInputDescriptor.getEntityName());
			processDirty = false;
		}
		return columnValue;
	}
	
	/**
	 * get records based on sql query replaced > to =
	 * @param ignoredRowsSql
	 * @param list
	 * @param conditionValue
	 * @return list of ignored records
	 */
	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getIgnoredBatchRecords(String ignoredRowsSql,List<Map<String,Object>> list, String conditionValue){
		List<Map<String, Object>> ignoredRowsList = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> ignoredRows = new ArrayList<Map<String, Object>>();
		if (ignoredRowsSql.contains(JdbcConstants.QUERY_PARAMETER)) {
			if (conditionValue != null) {
					if (driverName.indexOf(JdbcConstants.ORACLE_DRIVER) > JdbcConstants.INTEGER_CONSTANT_ZERO
							&& (jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("DATE") || jdbcInputDescriptor.getIncrementedColumnType()
									.equalsIgnoreCase("TIMESTAMP"))) {
						
						ignoredRows = jdbcTemplate
								.queryForList(
										ignoredRowsSql,
										new Object[] { Timestamp.valueOf(conditionValue)});
						;
												
					} else
						ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql,
								new Object[] { conditionValue });
			} else
				ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql,
						new Object[] { JdbcConstants.INTEGER_CONSTANT_ZERO });
		} else {
			ignoredRows = jdbcTemplate.queryForList(ignoredRowsSql);
		}
		if (ignoredRows.size() > JdbcConstants.INTEGER_CONSTANT_ZERO){
			List<Map<String, Object>> diffList = (List<Map<String, Object>>) CollectionUtils.disjunction(list, ignoredRows);
			if(diffList.size() > JdbcConstants.INTEGER_CONSTANT_ZERO){
				for(Map<String, Object> ignoredRow: diffList){
					ignoredRowsList.add(ignoredRow);
				}
			}
			logger.info("Jdbc Table Reader Handler during Ignored processing records", "Found {} ignored records", ignoredRowsList.size());
		}else {
			logger.info("Jdbc Table Reader Handler during Ignored processing records",
					"No more rows found for query={}", ignoredRowsSql);
		}
		if(ignoredRowsList.size() > 0) {
			return ignoredRowsList;
		} else{
			return ignoredRowsList;
		}
	}
}
