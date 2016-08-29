/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hive;

import static io.bigdime.core.commons.DataConstants.SCHEMA_FILE_NAME;
import static io.bigdime.core.commons.DataConstants.ENTITY_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.adaptor.metadata.utils.MetaDataJsonUtils;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.handler.AbstractHandler;

@Component
@Scope("prototype")
/**
 * This class responsible to load the schema from a file system to bigdime metastore.
 * 
 * @author mnamburi
 *
 */
public class AdaptorMetaDataHandler  extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(AdaptorMetaDataHandler.class));
	
	private static String hiveSchemaFileName = null;
	private static String entityName = null;
	private ObjectMapper objectMapper;	
	private String handlerPhase;
	
	@Autowired private MetadataStore metadataStore;
	@Autowired private MetaDataJsonUtils metaDataJsonUtils;
	
	/**
	 * <p>
	 * The build method being called only once by the framework ( bootstrap), upon startup of the Adaptor 
	 * the builder method will read a schema file from class path and persist the object to metadata.
	 * the method expects ENTITY_NAME,SCHEMA_FILE_NAME. It will throw Runtime Exception in case it didn't find ENTITY_NAME,SCHEMA_FILE_NAME.
	 */
	@Override
	public void build() throws AdaptorConfigurationException{
		super.build();
		handlerPhase = "building AdaptorMetaDataHandler";
		
		Metasegment metaSegment = null;

		hiveSchemaFileName = PropertyHelper.getStringProperty(getPropertyMap(), SCHEMA_FILE_NAME);
		entityName = PropertyHelper.getStringProperty(getPropertyMap(),ENTITY_NAME);
		// try find it from the Json values.
		if(entityName == null){
			@SuppressWarnings("unchecked")
			Entry<Object, String> srcDescInputs = (Entry<Object, String>) getPropertyMap().get(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC);
			@SuppressWarnings("unchecked")
			Map<String,Object>  inputMetadata = (Map<String,Object>) srcDescInputs.getKey();
			hiveSchemaFileName = PropertyHelper.getStringProperty(inputMetadata, SCHEMA_FILE_NAME);
			entityName = PropertyHelper.getStringProperty(inputMetadata,ENTITY_NAME);
		}
		Preconditions.checkNotNull(entityName,"Entity Name Cannot be null");
		Preconditions.checkNotNull(hiveSchemaFileName,"Hive Schema Value Cannot be null");
		try {
			objectMapper = new ObjectMapper();
			metaSegment = metadataStore.getAdaptorMetasegment(AdaptorConfig.getInstance().getName(), "HIVE",entityName);
			if (metaSegment == null) {
				metaSegment = buildMetaSegment();
				metadataStore.put(metaSegment);
			}
		} catch (final Exception ex) {
			throw new AdaptorConfigurationException(ex);
		}
	}
	/**
	 * Returns an Metasegment object that can be used to persist the object to the metastore. 
	 * <p>
	 * This method reads schema file from the local file system and use the MetaDataJsonUtils to convert to Metasegment Object.
	 * if the file is not readable it throws IOException and log an alert to the system.
	 * 
	 * @return
	 * @throws IOException
	 */	
	private Metasegment buildMetaSegment() throws IOException {
		logger.debug(handlerPhase, "reading the schema from config location, schemaFileName={}",hiveSchemaFileName);
		JsonNode jsonNode = null;
		Metasegment metaSegment  = null;
		try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(hiveSchemaFileName)) {
			if (is == null) {
				throw new FileNotFoundException(hiveSchemaFileName);
			}
			jsonNode = objectMapper.readTree(is);
			metaSegment = metaDataJsonUtils.convertJsonToMetaData(AdaptorConfig.getInstance().getName(),entityName,
					jsonNode);
		}catch (IOException e) {
			logger.alert(ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.INPUT_ERROR, ALERT_SEVERITY.BLOCKER,
					"\"unable to read a schema file  \" filename={} error={}",hiveSchemaFileName, e.toString());
			throw e;
		}
		return metaSegment;
	}
	
	/**
	 * always send as a Ready, we need to add an enhancement to framework to not to call this process method.
	 */
	@Override
	public Status process() throws HandlerException {
		return Status.READY;
	}

}
