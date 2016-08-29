/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.common.SqlTypes2HiveTypes;
import io.bigdime.libs.hive.metadata.TableMetaData;
import io.bigdime.libs.hive.table.HiveTableManger;
import io.bigdime.validation.common.AbstractValidator;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Factory(id = "column_schema", type = ColumnSchemaValidator.class)
@Component
@Scope("prototype")
public class ColumnSchemaValidator implements Validator{

	@Autowired
	private MetadataStore metadataStore;
	
	private String name;
	
	private static final Logger logger = LoggerFactory
			.getLogger(ColumnSchemaValidator.class);
	
	private Properties props = new Properties();
	private HiveTableManger hiveTableManager = null;
	private List<Column> hiveColumnList = null;
	Set<Attribute> metadataColumnsList = null;
	private Metasegment metasegment = null;
	@Override
	public ValidationResponse validate(ActionEvent actionEvent)
			throws DataValidationException {
		
		int hiveColumnCount =0;
		int sourceColumnCount =0;
		ValidationResult countValid = ValidationResult.FAILED;
		ValidationResult orderValid = ValidationResult.FAILED;
		ValidationResult typeValid = ValidationResult.FAILED;
		AbstractValidator commonCheckValidator = new AbstractValidator();
		ValidationResponse validationPassed = new ValidationResponse();
		validationPassed.setValidationResult(ValidationResult.FAILED);
		String hiveMetaStoreURL = actionEvent.getHeaders().get(ActionEventHeaderConstants.HIVE_METASTORE_URI);
		String hiveDBName = actionEvent.getHeaders().get(
				ActionEventHeaderConstants.HIVE_DB_NAME);
		String hiveTableName = actionEvent.getHeaders().get(
				ActionEventHeaderConstants.HIVE_TABLE_NAME);

		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_METASTORE_URI, hiveMetaStoreURL);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_DB_NAME, hiveDBName);
		commonCheckValidator.checkNullStrings(ActionEventHeaderConstants.HIVE_TABLE_NAME,
				hiveTableName);
		props.put(HiveConf.ConfVars.METASTOREURIS, hiveMetaStoreURL);
		hiveTableManager = HiveTableManger.getInstance(props);
		long startTime = System.currentTimeMillis();
		try {
			if (hiveTableManager.isTableCreated(hiveDBName, hiveTableName)) {
				hiveColumnList = getHiveColumnList(hiveTableManager, hiveDBName, hiveTableName);
				hiveColumnCount = hiveColumnList.size();
				metasegment = metadataStore.getAdaptorMetasegment(AdaptorConfig.getInstance().getAdaptorContext()
								.getAdaptorName(),
						ActionEventHeaderConstants.SCHEMA_TYPE_HIVE,
						hiveTableName);
				if (metasegment == null || metasegment.getEntitees() == null
						|| metasegment.getEntitees().size() == 0) {
					logger.alert(
							AdaptorConfig.getInstance().getAdaptorContext()
									.getAdaptorName(),
							ALERT_TYPE.OTHER_ERROR,
							ALERT_CAUSE.VALIDATION_ERROR,
							ALERT_SEVERITY.MAJOR,
							"No such metasegment for table {} found in {} database in metastore",
							hiveTableName, hiveDBName);
					validationPassed
							.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
				} else {
					metadataColumnsList = getSourceColumnList(metasegment, hiveTableName);
					sourceColumnCount = metadataColumnsList.size();
					countValid = columnCountValiation(hiveDBName, hiveTableName, sourceColumnCount, hiveColumnCount);
					orderValid = columnOrderValidation(hiveColumnList, metadataColumnsList, hiveDBName, hiveTableName);
					typeValid = columnTypeValidation(hiveColumnList, metadataColumnsList, hiveDBName, hiveTableName);
				}
				if(countValid == ValidationResult.PASSED && orderValid == ValidationResult.PASSED 
						&& typeValid == ValidationResult.PASSED) {
					validationPassed.setValidationResult(ValidationResult.PASSED);
				} else{
					validationPassed.setValidationResult(ValidationResult.FAILED);
				}
			} else {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
						.getAdaptorName(), "Hive table not exist",
						"Hive table {} is not found in hive databases {}",
						hiveTableName, hiveDBName);
				validationPassed
						.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
			}
		} catch (HCatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
					.getAdaptorName(), "HCatException",
					"Exception occurred while getting column schema from hive, cause: "
							+ e.getCause());
			throw new DataValidationException(
					"Exception during getting column schema from hive");
		} catch (MetadataAccessException ex) {
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"MetadataAccessException",
					"Exception occurred while getting column schema from metastore",
					ex);
			throw new DataValidationException(
					"Exception during getting column schema from metastore");
		}
		long endTime = System.currentTimeMillis();
		logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), 
				"Column schema validation for table = " +hiveTableName, "finished in {} milliseconds", (endTime-startTime));
		return validationPassed;
	}
	
	private List<Column> getHiveColumnList(HiveTableManger hiveTableManager, String dbName, String tableName) throws HCatException {
		TableMetaData table = hiveTableManager.getTableMetaData(dbName, tableName);
		List<Column> hiveColumnList = table.getColumns();
		return hiveColumnList;
	}
	
	private Set<Attribute> getSourceColumnList(Metasegment metasegment, String tableName){
		Entitee entitee = metasegment.getEntity(tableName);
		Set<Attribute> sourceColumnList = entitee.getAttributes();
		return sourceColumnList;
	}
	
	private ValidationResult columnCountValiation(String dbName, String tableName, int sourceCount, int hiveCount) {
		if(hiveCount == sourceCount) {
			logger.info(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Column Count match",
					"Hive table {} has the same number of columns as source in {} database, column count: {}",
					tableName, dbName, hiveCount);
			return ValidationResult.PASSED;
		} else if(hiveCount > sourceCount){
			int diff = Math.abs(hiveCount-sourceCount);
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"column count validator failed",
					"source column count({}) is not match as hive column count({}), diff is {}",
					sourceCount, hiveCount, diff);
			String additionalHiveColumn = getAdditionalHiveColumns(hiveColumnList, metadataColumnsList);
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Additional hive column found. This is really BAD!!",
					"Hive table {} has more column(s) than source in {} database, column(s): {}",
					tableName, dbName, additionalHiveColumn);
			return ValidationResult.FAILED;
		} else {
			int diff = Math.abs(hiveCount-sourceCount);
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"column count mismatch",
					"source column count({}) is not match as hive column count({}), diff is {}",
					sourceCount, hiveCount, diff);
			String additionalSourceColumn = getAdditionalSourceColumns(hiveColumnList, metadataColumnsList);
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Additional source column found",
					"Source table {} has more column than hive in {} database, column(s): {}",
					tableName, dbName, additionalSourceColumn);
			return ValidationResult.FAILED;
		}	
	}
	
	private String getAdditionalHiveColumns(List<Column> hiveColumnList, Set<Attribute> sourceColumnList) {
		List<Column> hiveList = new ArrayList<Column>(hiveColumnList);
		List<Attribute> sourceList = new ArrayList<Attribute>(sourceColumnList);
		Iterator<Column> iterator = hiveList.iterator();
		while (iterator.hasNext()) {
			Column column = iterator.next();
			for (Attribute attribute : sourceList) {
				if (attribute.getAttributeName()
						.compareToIgnoreCase(column.getName()) == 0) {
					iterator.remove();
				}
			}
		}
		// additional columns list
		StringBuilder sb = new StringBuilder();
		if (!hiveList.isEmpty()) {
			for (Column col : hiveList) {
				sb.append(col + " ");
			}
			hiveList.clear();
		}
		return sb.toString();	
	}
	
	private String getAdditionalSourceColumns(List<Column> hiveColumnList, Set<Attribute> sourceColumnList) {
		List<Attribute> sourceList = new ArrayList<Attribute>(
				sourceColumnList);
		Iterator<Attribute> iterator = sourceList
				.iterator();
		while (iterator.hasNext()) {
			Attribute attribute = iterator.next();
			for (Column column : hiveColumnList) {
				if (attribute.getAttributeName()
						.compareToIgnoreCase(column.getName()) == 0) {
					iterator.remove();
				}
			}
		}
		// additional columns list
		StringBuilder strBuilder = new StringBuilder();
		if (!sourceList.isEmpty()) {
			for (Attribute attr : sourceList) {
				strBuilder.append(attr.getAttributeName()
						.toLowerCase()
						+ DataConstants.COLON
						+ attr.getAttributeType().toLowerCase()
						+ " ");
			}
			sourceList.clear();
		}
		return strBuilder.toString();
	}
	
	private ValidationResult columnOrderValidation(List<Column> hiveColumnList, Set<Attribute> sourceColumnList,
					String dbName, String tableName) {
		ValidationResult orderValid = ValidationResult.FAILED;
		Boolean columnOrder = false;
		String wrongOrder = null;
		List<Attribute> sourceList = new ArrayList<Attribute>(sourceColumnList);
		StringBuilder sourceSb = new StringBuilder();
		for (int i = 0; i < sourceColumnList.size(); i++) {
			sourceSb.append(sourceList.get(i).getAttributeName()
					.toLowerCase()
					+ DataConstants.COLON
					+ sourceList.get(i).getAttributeType()
							.toLowerCase() + " ");
		}
		for (int i = 0; i < hiveColumnList.size(); i++) {
			if (hiveColumnList
					.get(i)
					.getName()
					.compareToIgnoreCase(
							sourceList.get(i)
									.getAttributeName()) == 0) {
				columnOrder = true;
			} else {
				columnOrder = false;
				wrongOrder = hiveColumnList.get(i).getName()
						+ DataConstants.COLON
						+ hiveColumnList.get(i).getType();
				break;
			}
		}
		if (columnOrder) {
			logger.info(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Column Order match",
					"Hive table {} in {} database has the same column order as source, columns: {}",
					tableName, dbName,
					hiveColumnList.toString());
			orderValid = ValidationResult.PASSED;
		} else {
			logger.debug(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Wrong order",
					"Hive Column list: {} and Source Column list: {}",
					hiveColumnList.toString(), sourceSb.toString());
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Column Order mismatch",
					"Hive table {} in {} database has different column order as source, first happened at {}",
					tableName, dbName, wrongOrder);
			orderValid = ValidationResult.FAILED;
		}
		return orderValid;
	}
	
	private ValidationResult columnTypeValidation(List<Column> hiveColumnList, Set<Attribute> sourceColumnList, String dbName, String tableName){
		List<Attribute> sourceList = new ArrayList<Attribute>(sourceColumnList);
		Boolean columnTypeMatch = false;
		ValidationResult typeValid = ValidationResult.FAILED;
		StringBuilder sourceSb = new StringBuilder();
		StringBuilder hiveSb = new StringBuilder();
		for (int i = 0; i < sourceColumnList.size(); i++) {
			sourceSb.append(sourceList.get(i).getAttributeName()
					.toLowerCase()
					+ DataConstants.COLON
					+ sourceList.get(i).getAttributeType()
							.toLowerCase() + " ");
		}
		if(hiveColumnList.size() <= sourceColumnList.size()) {
			Iterator<Attribute> iterator = sourceList.iterator();
			while (iterator.hasNext()) {
				Attribute attribute = iterator.next();
				for (Column column : hiveColumnList) {
					if (attribute.getAttributeName()
							.compareToIgnoreCase(column.getName()) == 0
							&& column
									.getType()
									.compareToIgnoreCase(
											SqlTypes2HiveTypes
													.sqlType2HiveType(
															attribute
																	.getAttributeType())
													.getTypeName()) == 0) {
						iterator.remove();
						columnTypeMatch = true;
					}
				}
			}

			// list of column type mismatch and additional columns added (if possible)
			if (!sourceList.isEmpty()) {
				for (Attribute attr : sourceList) {
					hiveSb.append(attr.getAttributeName().toLowerCase()
							+ DataConstants.COLON
							+ attr.getAttributeType().toLowerCase()
							+ " ");
				}
				sourceList.clear();
				columnTypeMatch = false;
			}
		}
			
		if (hiveColumnList.size() > sourceColumnList.size()) {
			List<Column> hiveList = new ArrayList<Column>(
						hiveColumnList);
			Iterator<Column> iterator = hiveList.iterator();
			while (iterator.hasNext()) {
				Column column = iterator.next();
				for (Attribute attribute : sourceList) {
					if (attribute.getAttributeName()
							.compareToIgnoreCase(column.getName()) == 0
							&& column
								.getType()
									.compareToIgnoreCase(
											SqlTypes2HiveTypes
												.sqlType2HiveType(
													attribute.getAttributeType())
														.getTypeName()) == 0) {
						iterator.remove();
						columnTypeMatch = true;
					}
				}
			}

			// additional columns list
			if (!hiveList.isEmpty()) {
				for (Column col : hiveList) {
					hiveSb.append(col.toString() + " ");
				}
				hiveList.clear();
				columnTypeMatch = false;
			}
		}
		
		if(columnTypeMatch) {
			logger.info(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Column Type Match",
					"Hive table {} in {} database has the same column type as source, columns: {}",
					tableName, dbName,
					hiveColumnList.toString());
			typeValid = ValidationResult.PASSED;
		} else {
			logger.debug(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Column list with type",
					"Hive Column list: {} and Source Column list: {}",
					hiveColumnList.toString(), sourceSb.toString());
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"Column Type Mismatch",
					"Hive table {} in {} database has different column type as source, column(s): {}",
					tableName, dbName, hiveSb.toString());
			sourceSb.setLength(0);
			typeValid = ValidationResult.FAILED;
		}
		
		return typeValid;
		
	}
	
	
	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
