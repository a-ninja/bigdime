/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.validation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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

@Factory(id = "column_type", type = ColumnTypeValidator.class)
@Component
@Scope("prototype")
/**
 * Performs validation by comparing hive column type and source column type
 * 
 * @author Rita Liu
 * 
 */
public class ColumnTypeValidator implements Validator {

	@Autowired
	private MetadataStore metadataStore;

	private String name;
	
	private static final Logger logger = LoggerFactory
			.getLogger(ColumnTypeValidator.class);

	/**
	 * This method is to compare hive column data type to source column data
	 * type
	 * 
	 * @param actionEvent
	 * @return PASSED if hive column data type is same as source column data
	 *         type otherwise return COLUMN_TYPE_MISMATCH and list wrong column
	 *         data type, return INCOMPLETE_SETUP if hive table is not found or
	 *         no table metadata in metastore
	 * 
	 * 
	 */

	@Override
	public ValidationResponse validate(ActionEvent actionEvent)
			throws DataValidationException {
		TableMetaData table = null;
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

		// connect to hive
		Properties props = new Properties();
		props.put(HiveConf.ConfVars.METASTOREURIS, hiveMetaStoreURL);

		HiveTableManger hiveTableManager = HiveTableManger.getInstance(props);
		long startTime = System.currentTimeMillis();
		try {
			if (hiveTableManager.isTableCreated(hiveDBName, hiveTableName)) {
				table = hiveTableManager.getTableMetaData(hiveDBName,
						hiveTableName);
				List<Column> hiveColumnList = table.getColumns();
				Set<Attribute> metadataColumns = null;
				Metasegment metasegment = metadataStore.getAdaptorMetasegment(AdaptorConfig
						.getInstance().getAdaptorContext().getAdaptorName(),
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
					Entitee entitee = metasegment.getEntity(hiveTableName);
					metadataColumns = entitee.getAttributes();
					List<Attribute> sourceColumnList = new ArrayList<Attribute>(
							metadataColumns);
					StringBuilder sb = new StringBuilder();
					boolean columnTypeMatch = false;
					List<Attribute> sourceList = new ArrayList<Attribute>(
							sourceColumnList);
					StringBuilder st = new StringBuilder();
					for (int i = 0; i < metadataColumns.size(); i++) {
						st.append(sourceColumnList.get(i).getAttributeName()
								.toLowerCase()
								+ DataConstants.COLON
								+ sourceColumnList.get(i).getAttributeType()
										.toLowerCase() + " ");
					}
					// hive has less (or equal) column than source ---- hive column count < source column count
					if (hiveColumnList.size() <= metadataColumns.size()) {
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
								sb.append(attr.getAttributeName().toLowerCase()
										+ DataConstants.COLON
										+ attr.getAttributeType().toLowerCase()
										+ " ");
							}
							sourceList.clear();
							columnTypeMatch = false;
						}
					}
					// hive has more column than source ---- hive column count > source column count
					if (hiveColumnList.size() > metadataColumns.size()) {
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
																		attribute
																				.getAttributeType())
																.getTypeName()) == 0) {
									iterator.remove();
									columnTypeMatch = true;
								}
							}
						}

						// additional columns list
						if (!hiveList.isEmpty()) {
							for (Column col : hiveList) {
								sb.append(col.toString() + " ");
							}
							hiveList.clear();
							columnTypeMatch = false;
						}
					}
					if (columnTypeMatch) {
						logger.info(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"Column Type Match",
								"Hive table {} in {} database has the same column type as source, columns: {}",
								hiveTableName, hiveDBName,
								hiveColumnList.toString());
						validationPassed
								.setValidationResult(ValidationResult.PASSED);
					} else {
						logger.debug(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"Column list with type",
								"Hive Column list: {} and Source Column list: {}",
								hiveColumnList.toString(), st.toString());
						logger.warn(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"Column Type Mismatch",
								"Hive table {} in {} database has different column type as source, column(s): {}",
								hiveTableName, hiveDBName, sb.toString());
						validationPassed
								.setValidationResult(ValidationResult.COLUMN_TYPE_MISMATCH);
						sb.setLength(0);
					}
				}
			} else {
				logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
						.getAdaptorName(), "Hive table not exist",
						"Hive table {} is not found in hive database {}",
						hiveTableName, hiveDBName);
				validationPassed
						.setValidationResult(ValidationResult.INCOMPLETE_SETUP);
			}
		} catch (HCatException e) {
			logger.warn(AdaptorConfig.getInstance().getAdaptorContext()
					.getAdaptorName(), "HCatException",
					"Exception occurred while getting column type from hive, cause: " +
					e.getCause());
			throw new DataValidationException(
					"Exception during getting column type from hive");
		} catch (MetadataAccessException ex) {
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"MetadataAccessException",
					"Exception occurred while getting column type from metastore",
					ex);
			throw new DataValidationException(
					"Exception during getting column type from metastore");
		}
		long endTime = System.currentTimeMillis();
		logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), 
				"Column type validation for table = " +hiveTableName, " finished in {} milliseconds", (endTime-startTime));
		return validationPassed;

	}

	@Override
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
