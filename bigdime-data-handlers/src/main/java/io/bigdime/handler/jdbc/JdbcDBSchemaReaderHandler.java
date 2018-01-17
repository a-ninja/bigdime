/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

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
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;

/**
 * This class process all the table names from a database by using sql query,
 * and then filter the processed table names based on "include" filter value if has,
 * otherwise, processed table names will be the all table names
 * 
 * @author Rita Liu
 *
 */

@Component
@Scope("prototype")
public class JdbcDBSchemaReaderHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(JdbcDBSchemaReaderHandler.class));
	
	@Autowired
	private JdbcInputDescriptor jdbcInputDescriptor;
	@Autowired
	private DataSource lazyConnectionDataSourceProxy;
	@Value("${database.driverClassName}")
	private String driverName;
	private JdbcTemplate jdbcTemplate;
	private String handlerPhase = null;
	private String jsonStr = null;
	private static List<String> allTableNameList;
	private static List<String> processTable;
	private String currentTableToProcess = null;
	
	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building Jdbc DB Schema Reader Handler";
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
	 * This process method contains preProcess and doProcess, get all tables from database and filter out the list need to process
	 * send one to the next handler
	 * 
	 * @throws HandlerException
	 */
	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing JdbcDBSchemaReaderHandler";
		logger.info(handlerPhase,
				"handler_id={} handler_name={} properties={}", getId(),
				getName(), getPropertyMap());
		incrementInvocationCount();
		try {
			preProcess();
			return doProcess();
		} catch (JdbcHandlerException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"jdbcAdaptor jdbcHandler exception\" databaseName={}, error={}",
					jdbcInputDescriptor.getDatabaseName(), e.toString());
			throw new HandlerException("Unable to process sql database "+jdbcInputDescriptor.getDatabaseName(), e);
		}
	}
	
	public void setDataSource(DataSource dataSource) {
		this.lazyConnectionDataSourceProxy = dataSource;		
	}
	
	/**
	 * This method is get all tables from a source database based on sql query
	 * @param sqlQuery
	 * @return tableList
	 */
	private List<String> getTableNameList(String sqlQuery) throws DataAccessException{
		List<String> tableList = new ArrayList<String>();
		tableList = jdbcTemplate.queryForList(sqlQuery, String.class);
		return tableList;
	}
	
	/**
	 * This preProcess() will get all table names from a database and filter out the required tables
	 * 
	 * @return Status, READY, CALLBACK, BACKOFF
	 * @throws JdbcHandlerException
	 * @throws HandlerException
	 */
	@Override
	protected Status preProcess() throws JdbcHandlerException{
		String dbSql = jdbcInputDescriptor.formatQuery(jdbcInputDescriptor.getInputType(), jdbcInputDescriptor.getInputValue(), driverName);
		logger.debug("Formatted Jdbc DB Reader Handler Query", "dbSql={}", dbSql);
		try{
			//if handler is first run or not
			if(isFirstRun()){
				if (!StringUtils.isEmpty(dbSql)) {
					//get all table names from database
					allTableNameList = getTableNameList(dbSql);
					//includeFilter is not specified, process all tables in databases
					if(jdbcInputDescriptor.getIncludeFilter().isEmpty()){
						processTable = allTableNameList;
					} else{
						processTable = getFilteredTableList(allTableNameList);
					}
				} else {
					logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
							ALERT_SEVERITY.BLOCKER,"\"database sql query is null\" database = {}",
							jdbcInputDescriptor.getDatabaseName());
					throw new JdbcHandlerException("Unable to format db query for database " + jdbcInputDescriptor.getDatabaseName());
				}
			}
		} catch(DataAccessException e) {
			logger.alert(ALERT_TYPE.INGESTION_FAILED,ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
					ALERT_SEVERITY.BLOCKER,"\"DataAccessException occurred during get tables from database\" database = {}, error = {}",
					jdbcInputDescriptor.getDatabaseName(), e.toString());
			throw new JdbcHandlerException("Unable to process tables from database " + jdbcInputDescriptor.getDatabaseName());
		}
		
		return Status.READY;
	}
	
	/**
	 * This method is filtering out the process table from the all table list
	 * @param tableList
	 * @return processTableList
	 */
	private List<String> getFilteredTableList(List<String> tableList) {
		String regex = jdbcInputDescriptor.getIncludeFilter();
		Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
		List<String> processTableList = new ArrayList<String>();
		for(String str : tableList){
			if(pattern.matcher(str).lookingAt()){
				processTableList.add(str);
			}
		}
		return processTableList;
	}
	
	/**
	 * This doProcess() is sending one table from table list to next handler
	 * if no table needs to process, return BACKOFF, else return READY
	 * @return Status
	 */
	@Override
	protected Status doProcess() {
		currentTableToProcess = getNextTableToProcess(processTable);
		if(currentTableToProcess == null){
			logger.info("no table need to process", "return BACKOFF");
			return Status.BACKOFF;
		}
		logger.debug(handlerPhase, "Process the currentTable ={} in the list", currentTableToProcess.toLowerCase());
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.TARGET_ENTITY_NAME, currentTableToProcess.toLowerCase());
		getHandlerContext().createSingleItemEventList(actionEvent);
		return Status.READY;
	}
	
	/**
	 * This method is getting a table from table list
	 * @param processTableList
	 * @return currentTable if not null, else return null
	 */
	private String getNextTableToProcess(List<String> processTableList){
		if(processTableList.size()>0){
			String currentTable = processTableList.remove(0);
			return currentTable;
		} else {
			return null;
		}
	}
}
