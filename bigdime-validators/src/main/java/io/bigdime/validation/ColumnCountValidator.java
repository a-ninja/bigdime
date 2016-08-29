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

import io.bigdime.alert.Logger;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.commons.DataConstants;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.validation.DataValidationException;
import io.bigdime.core.validation.Factory;
import io.bigdime.core.validation.ValidationResponse;
import io.bigdime.core.validation.Validator;
import io.bigdime.core.validation.ValidationResponse.ValidationResult;
import io.bigdime.adaptor.metadata.*;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.libs.hive.common.Column;
import io.bigdime.libs.hive.metadata.TableMetaData;
import io.bigdime.libs.hive.table.HiveTableManger;
import io.bigdime.validation.common.AbstractValidator;

@Factory(id = "column_count", type = ColumnCountValidator.class)
@Component
@Scope("prototype")
/**
 * Performs validation by comparing hive column count and source column count
 * 
 * @author Rita Liu
 * 
 */
public class ColumnCountValidator implements Validator {

	private static final Logger logger = LoggerFactory
			.getLogger(ColumnCountValidator.class);

	@Autowired
	private MetadataStore metadataStore;

	private String name;

	/**
	 * This method is to compare column count of specific hive table to column
	 * count of specific table in metastore(Source)
	 * 
	 * @param actionEvent
	 * @return ValidationResponse Three conditions: 1. hive column count =
	 *         source column count --- PASSED 2. hive column count > source
	 *         column count --- FAILED (BAD situation) 3. hive column count <
	 *         source column count --- COLUMN_COUNT_MISMATCH return
	 *         INCOMPLETE_SETUP if hive table is not found or no such metadata
	 *         found in metastore
	 * 
	 */

	@Override
	public ValidationResponse validate(ActionEvent actionEvent)
			throws DataValidationException {
		TableMetaData table = null;
		int sourceColumnCount = 0;
		ValidationResponse validationPassed = new ValidationResponse();
		AbstractValidator commonCheckValidator = new AbstractValidator();
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
				int hiveColumnCount = hiveColumnList.size();
				Set<Attribute> metadataColumns = null;
				Metasegment metasegment = metadataStore.getAdaptorMetasegment(
						AdaptorConfig.getInstance().getAdaptorContext()
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
					Entitee entitee = metasegment.getEntity(hiveTableName);
					metadataColumns = entitee.getAttributes();
					sourceColumnCount = metadataColumns.size();

					if (hiveColumnCount == sourceColumnCount) {
						logger.info(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"Column Count match",
								"Hive table {} has the same number of columns as source in {} database, column count: {}",
								hiveTableName, hiveDBName, hiveColumnCount);
						validationPassed
								.setValidationResult(ValidationResult.PASSED);
					}
					// hive has more column than source --- hive column count > source column count
					if (hiveColumnCount > sourceColumnCount) {
						List<Column> hiveList = new ArrayList<Column>(
								hiveColumnList);
						List<Attribute> sourceList = new ArrayList<Attribute>(
								metadataColumns);
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
						int diff = Math
								.abs(hiveColumnCount - sourceColumnCount);
						logger.debug(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"column count validate failed",
								"source column count({}) is not match as hive column count({})",
								sourceColumnCount, hiveColumnCount);
						logger.warn(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"Additional hive column found",
								"Hive table {} has {} more column than source in {} database, column(s): {}",
								hiveTableName, diff, hiveDBName, sb.toString(),
								"This is really BAD!!");
						validationPassed
								.setValidationResult(ValidationResult.FAILED);
						sb.setLength(0);
					}
					// hive has less column than source --- hive column count < source column count
					if (hiveColumnCount < sourceColumnCount) {
						List<Attribute> sourceColumnList = new ArrayList<Attribute>(
								metadataColumns);
						Iterator<Attribute> iterator = sourceColumnList
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
						if (!sourceColumnList.isEmpty()) {
							for (Attribute attr : sourceColumnList) {
								strBuilder.append(attr.getAttributeName()
										.toLowerCase()
										+ DataConstants.COLON
										+ attr.getAttributeType().toLowerCase()
										+ " ");
							}
							sourceColumnList.clear();
						}
						int diff = Math
								.abs(hiveColumnCount - sourceColumnCount);
						logger.debug(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"column count mismatch",
								"source column count({}) is not match as hive column count({})",
								sourceColumnCount, hiveColumnCount);
						logger.warn(
								AdaptorConfig.getInstance().getAdaptorContext()
										.getAdaptorName(),
								"Additional source column found",
								"Source table {} has {} more column than hive in {} database, column(s): {}",
								hiveTableName, diff, hiveDBName,
								strBuilder.toString());
						validationPassed
								.setValidationResult(ValidationResult.COLUMN_COUNT_MISMATCH);
						strBuilder.setLength(0);
					}
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
					"Exception occurred while getting column count from hive, cause: "
							+ e.getCause());
			throw new DataValidationException(
					"Exception during getting column count from hive");
		} catch (MetadataAccessException ex) {
			logger.warn(
					AdaptorConfig.getInstance().getAdaptorContext()
							.getAdaptorName(),
					"MetadataAccessException",
					"Exception occurred while getting column count from metastore",
					ex);
			throw new DataValidationException(
					"Exception during getting column count from metastore");
		}
		long endTime = System.currentTimeMillis();
		logger.info(AdaptorConfig.getInstance().getAdaptorContext().getAdaptorName(), 
				"Column count validation for table = " +hiveTableName, "finished in {} milliseconds", (endTime-startTime));
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
