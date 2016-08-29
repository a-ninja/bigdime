/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;

import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import io.bigdime.core.config.AdaptorConfig;

/**
 * This class is inserting table schema into Big dime Metastore, 
 * also check and update Metastore
 * 
 * @author Pavan Sabinikari, Rita Liu
 * 
 */

@Component
@Scope("prototype")
public class JdbcMetadataManagement {

	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(JdbcMetadataManagement.class));

	/**
	 * Forms Metasegment object by retrieving from source data base.
	 * @param jdbcInputDescriptor
	 * @param jdbcTemplate
	 * @return
	 */
	public Metasegment getSourceMetadata(
			JdbcInputDescriptor jdbcInputDescriptor, JdbcTemplate jdbcTemplate) {
		Metasegment metaSegmnt = null;
		if(!jdbcInputDescriptor.getDatabaseName().isEmpty()){
			metaSegmnt = (Metasegment) jdbcTemplate.query(
				JdbcConstants.SELECT_FROM + jdbcInputDescriptor.getDatabaseName()+"."+jdbcInputDescriptor.getEntityName(),
				new JdbcMetadata(jdbcInputDescriptor));
		} else{
			metaSegmnt = (Metasegment) jdbcTemplate.query(
					JdbcConstants.SELECT_FROM +jdbcInputDescriptor.getEntityName(),
					new JdbcMetadata(jdbcInputDescriptor));
		}
		return metaSegmnt;
	}

	/**
	 * Gets the column List
	 * @param jdbcInputDescriptor
	 * @param metasegment
	 * @return
	 */
	public HashMap<String, String> getColumnList(
			JdbcInputDescriptor jdbcInputDescriptor, Metasegment metasegment) {
		HashMap<String, String> columnNamesAndTypes = new HashMap<String, String>();
		if (metasegment != null) {

			logger.debug("JDBC Handler Reader getting column list",
					"tableName={}", jdbcInputDescriptor.getEntityName());
			Set<Entitee> entitySet = metasegment.getEntitees();
			for (Entitee entity : entitySet) {
				if (entity.getAttributes() != null)
					for (Attribute attribute : entity.getAttributes()) {
						columnNamesAndTypes.put(attribute.getAttributeName(),
								attribute.getAttributeType());
					}
			}
		} else
			throw new IllegalArgumentException(
					"Provided argument:metasegment object in getColumnList() cannot be null");
		return columnNamesAndTypes;
	}

	/**
	 * Sets Columns List
	 * @param jdbcInputDescriptor
	 * @param metasegment
	 */
	public void setColumnList(JdbcInputDescriptor jdbcInputDescriptor,
			Metasegment metasegment) {
		List<String> columnNames = new ArrayList<String>();
		if (metasegment != null) {
			logger.debug("JDBC Handler Reader setting column list",
					"tableName={}", jdbcInputDescriptor.getEntityName());
			if (metasegment.getEntitees() == null) {
				throw new IllegalArgumentException(
						"Metasegment should contain atleast one entity");
			}
			Set<Entitee> entitySet = metasegment.getEntitees();
			for (Entitee entity : entitySet) {
				if (entity.getAttributes() != null) {
					for (Attribute attribute : entity.getAttributes()) {
						if (!columnNames.contains(attribute.getAttributeName())) {
							columnNames.add(attribute.getAttributeName());
						}
						if (jdbcInputDescriptor.getIncrementedBy().length() > JdbcConstants.INTEGER_CONSTANT_ZERO
								&& attribute.getAttributeName()
										.equalsIgnoreCase(
												jdbcInputDescriptor
														.getIncrementedBy())) {

							jdbcInputDescriptor
									.setIncrementedColumnType(attribute
											.getAttributeType());
							jdbcInputDescriptor.setColumnName(attribute
									.getAttributeName());
						}
						if (jdbcInputDescriptor.getColumnName() == null) {
							jdbcInputDescriptor.setColumnName(attribute
									.getAttributeName());
						}
					}
				}
			}

			if (jdbcInputDescriptor.getColumnList().size() > 0){
				jdbcInputDescriptor.getColumnList().clear();
			}
			jdbcInputDescriptor.setColumnList(columnNames);

		} else
			throw new IllegalArgumentException(
					"Provided argument:metasegment object in getColumnList() cannot be null");
	}

	/**
	 * This will insert/update the metadata details.
	 * @param metasegment
	 * @param tableName
	 * @param columnNamesList
	 * @param metadataStore
	 */
	public void checkAndUpdateMetadata(Metasegment metasegment,
			String tableName, List<String> columnNamesList,
			MetadataStore metadataStore,String databaseName) {
		
		try {
			if (metasegment != null) {
				if (metasegment.getEntitees() != null){
					for (Entitee entity : metasegment.getEntitees()) {
							entity.setEntityName(tableName);
					}
				} 
				if(databaseName!=null) {
					metasegment.setDatabaseName(databaseName);
				}
			}
			if(databaseName!=null) {
				metasegment.setDatabaseName(databaseName);
			}
			Metasegment metaseg = metadataStore.getAdaptorMetasegment(
					AdaptorConfig.getInstance().getName(),
					JdbcConstants.METADATA_SCHEMA_TYPE, tableName);
			if (metaseg != null && metaseg.getEntitees() != null) {
				for (Entitee entity : metaseg.getEntitees()){
					if (entity == null
							|| (entity.getAttributes().size() != columnNamesList
									.size())){
						metadataStore.put(metasegment);
					}
				}
				
			} else{
				metadataStore.put(metasegment);
			}
			
		} catch (MetadataAccessException e) {
			logger.warn("Unable to put into MetaStore for entity={}", tableName);
		}

	}

}