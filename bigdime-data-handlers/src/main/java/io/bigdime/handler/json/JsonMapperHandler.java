/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.json;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.JsonHelper;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.commons.TimeManager;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractHandler;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.handler.SimpleJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.handler.kafka.KafkaInputDescriptor;

/**
 * A handler that receives the Json document, and read relevant fields , optionally,
 * submits to channel.
 * 
 * user's want to use this handler can optionally set these values(timestamp/partition_name) to the adaptor.json
 * {partition_name : "account"}
 * { timestamp  : "timestamp"}
 * these property values used on the input message,retrieve the corresponding value from the Json
 * and add the values to the action header for downstream to consume.
 * 
 * @author mnamburi
 *
 */
@Component
@Scope("prototype")
public class JsonMapperHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(JsonMapperHandler.class));
	private static final String rowSeparatedBy = "\n";

	private static final String TIME_STAMP = "timestamp";
	private static final String PARTITION_NAME = "partition_name";

	private String timestamp;
	private String partition_name;	
	private ObjectMapper objectMapper;
	
	private String partition_dt = null;
	private String partition_hour = null;
	
	@Autowired
	private JsonHelper jsonHelper;
	
	/**
	 * Each instance of handler reads from exactly one topic:partition.
	 */
	@Autowired
	private RuntimeInfoStore<RuntimeInfo> runtimeInfoStore;
	
	private static final String DF = "yyyyMMdd";
	private static final String HOUR_FORMAT = "HH";
	private static  final String TIMESTAMP = "DT";
	private static  final String HOUR = "HOUR";
	private static final DateTimeZone timeZone = DateTimeZone.forID("UTC");
	private static final DateTimeFormatter hourFormatter = DateTimeFormat.forPattern(HOUR_FORMAT).withZone(timeZone);
	private static final DateTimeFormatter formatter = DateTimeFormat.forPattern(DF).withZone(timeZone);
	
	private TimeManager timeManager = TimeManager.getInstance();
	private KafkaInputDescriptor inputDescriptor;
	private String handlerPhase;
	
	private static  final String SKIP_VALIDATIONS = "skip-validations";
	private boolean skipValidations  = false;
	
	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();
		handlerPhase = "building Json MapperHandler";
		timestamp = PropertyHelper.getStringProperty(getPropertyMap(), TIME_STAMP);
		partition_name = PropertyHelper.getStringProperty(getPropertyMap(), PARTITION_NAME);
		skipValidations = PropertyHelper.getBooleanProperty(getPropertyMap(), SKIP_VALIDATIONS);

		objectMapper = new ObjectMapper();
		
		@SuppressWarnings("unchecked")
		Entry<Object, String> srcDescInputs = (Entry<Object, String>) getPropertyMap().get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
		
		@SuppressWarnings("unchecked")
		Map<String,Object>  inputMetadata = (Map<String,Object>) srcDescInputs.getKey();
		
		inputDescriptor = new KafkaInputDescriptor();
		try {
			inputDescriptor.parseDescriptor(inputMetadata);
		} catch (IllegalArgumentException ex) {
			throw new InvalidValueConfigurationException(
					 "incorrect value specified in src-desc "+ ex.getMessage());
		}	

		try {
			assginPartitionsFromRunTimeInfo(inputDescriptor.getEntityName(), String.valueOf(inputDescriptor.getPartition()));
		} catch (RuntimeInfoStoreException e) {
			throw new AdaptorConfigurationException(e);
		}		
		logger.debug("building handler", handlerPhase+"timestamp={} partition_name={}",timestamp,partition_name);
	}
	
	public KafkaInputDescriptor getInputDescriptor(){
		return inputDescriptor;
	}

	/**
	 * get run time information from the metastore
	 * if the data is not available in the metastore, return the -1 says the is need to be processed from the earliest offset information.
	 * otherwise process the data where is is left off.
	 * @param entityName
	 * @param descriptor
	 * @param offsetPropertyName
	 * @return
	 */
	protected void assginPartitionsFromRunTimeInfo(final String entityName, final String descriptor)
					throws RuntimeInfoStoreException {
			RuntimeInfo runtimeInfo = runtimeInfoStore.get(AdaptorConfig.getInstance().getName(), entityName,descriptor);
			if (runtimeInfo != null && runtimeInfo.getProperties() != null) {
				partition_dt = runtimeInfo.getProperties().get(TIMESTAMP);				
				partition_hour = runtimeInfo.getProperties().get(HOUR);
				logger.debug("proessing handler", "MessageOffsetFromRuntimeInfo from runtime account={} date={} hour={}",
						partition_dt,partition_hour);
			}
	}
	
	/**
	 * Picks up the events from handler context and processes them. If there is
	 * no data in the list set on context, an {@link IllegalArgumentException}
	 * is thrown.
	 * 
	 * @formatter:off
	 * if (anything in journal)
	 * 	process from journal
	 * else 
	 * 	process from context
	 * 
	 * 
	 * @return {@link Status#READY} if there was only one event in the input
	 *         list. Returns {@link Status#CALLBACK} if there were more than one
	 *         events available in the list.
	 * @throws HandlerException
	 * 
	 * @formatter:on
	 */
	@Override
	public Status process() throws HandlerException {
		logger.debug("processing handler", "processing jsonmapper handler");
		if (getSimpleJournal().getEventList() != null && !getSimpleJournal().getEventList().isEmpty()) {
			// process for CALLBACK status.
			/*
			 * @formatter:off
			 * Get the list from journal
			 * remove one from the list
			 * submit to channel if needed
			 * set one in context
			 * if more available in journal list, return CALLBACK
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getSimpleJournal().getEventList();
			logger.debug("process JsonMapperHandler", "_message=\"journal not empty\" list_size={}",
					actionEvents.size());
			return processIt(actionEvents);

		} else {
			// process for ready status.
			/*
			 * @formatter:off
			 * Get the list from context
			 * remove one from the list
			 * submit to channel if needed
			 * set one in context
			 * if more available in context list, return CALLBACK
			 * @formatter:on
			 */
			List<ActionEvent> actionEvents = getHandlerContext().getEventList();
			logger.debug("process JsonMapperHandler",
					"_message=\"journal empty, will process from context\" actionEvents={}", actionEvents);

			Preconditions.checkNotNull(actionEvents);
			Preconditions.checkArgument(!actionEvents.isEmpty(),
					"eventList in HandlerContext must contain at least one ActionEvent");
			return processIt(actionEvents);
		}
	}
	/**
	 * if the partition changed, it will set a flag says the validations to be performed.
	 * very first time when there is no data in the system, it will say VALIDATION_READY = false;
	 * date  = 2016-01-01 hour = 01  totalRecords = 1
	 * date  = 2016-01-01 hour = 02 totalRecords = 10
	 * date  = 2016-01-01 hour = 03 totalRecords = 10
	 * date  = 2016-01-01 hour = 04 totalRecords = 5
	 * date  = 2016-01-02 hour = 01 totalRecords = 10
	 * @param accountName
	 * @param hour
	 * @param date
	 * @return
	 */
	@Deprecated
	public boolean isValidationReady(String date,String hour){
		boolean validationReady = false;
		if(partition_hour == null ||  partition_dt == null ){
			partition_dt = date;
			partition_hour = hour;
		} else if(!partition_hour.equalsIgnoreCase(hour) || 
				!partition_dt.equalsIgnoreCase(date)
				){
			partition_dt = date;
			partition_hour = hour;			
			validationReady = true;
		}
		return validationReady;
	}
	
	
	private Status processIt(List<ActionEvent> actionEvents) throws HandlerException {
		Status statusToReturn = Status.READY;
		String dt = null;
		String hour = null;
		DateTime dateTime = null;		
		try {
			ActionEvent actionEvent = actionEvents.remove(0);
			JsonNode jsonDocument = objectMapper.readTree(actionEvent.getBody());

			try {
				ObjectNode s = jsonHelper.find(jsonDocument, timestamp);
				Object serverTimestamp = jsonHelper.getRequiredProperty(s, timestamp);
				
				if(serverTimestamp instanceof String){
					dateTime = new DateTime(serverTimestamp, timeZone);
				}
				if(serverTimestamp instanceof Long){
					dateTime = new DateTime(((Long) serverTimestamp).longValue(), timeZone);
				}
				dt = formatter.print(dateTime);
				hour = hourFormatter.print(dateTime);
				logger.debug(handlerPhase, "formatted date from message is: {} ", dt);
				actionEvent.getHeaders().put(TIMESTAMP, dt);
				actionEvent.getHeaders().put(HOUR, hour);
			} catch (Exception e) {
				DateTime localTime = timeManager.getLocalDateTime();
				dt = timeManager.format(TimeManager.FORMAT_YYYYMMDD, localTime);
				hour = hourFormatter.print(localTime);
				actionEvent.getHeaders().put(TIMESTAMP, dt);
				actionEvent.getHeaders().put(HOUR, hour);
				logger.warn("process JsonMapperHandler",
						"_message=\"timestamp not found in the Json \" timestamp={} error={}", timestamp,e.getMessage());
			}
			actionEvent.setBody((jsonDocument.toString()+rowSeparatedBy).getBytes(Charset.defaultCharset()));
			if(skipValidations){
				actionEvent.getHeaders().put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.FALSE.toString());
			} else{
				actionEvent.getHeaders().put(ActionEventHeaderConstants.VALIDATION_READY, Boolean.TRUE.toString());
			}
			/*
			 * Check for outputChannel map. get the eventList of channels. check
			 * the criteria and put the message.
			 */
			if (getOutputChannel() != null) {
				getOutputChannel().put(actionEvent);
			}
			getHandlerContext().createSingleItemEventList(actionEvent);
			if (!actionEvents.isEmpty()) {
				getSimpleJournal().setEventList(actionEvents);
				statusToReturn = Status.CALLBACK;
			} else {
				getSimpleJournal().setEventList(null);
				statusToReturn = Status.READY;
			}
		} catch (IOException e) {
			throw new HandlerException("unable to parse json document", e);
		}
		return statusToReturn;

	}

	private HandlerJournal getSimpleJournal() throws HandlerException {
		HandlerJournal simpleJournal = getNonNullJournal(SimpleJournal.class);
		return simpleJournal;
	}

}
