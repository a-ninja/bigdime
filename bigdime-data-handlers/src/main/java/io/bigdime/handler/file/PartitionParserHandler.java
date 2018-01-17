/**
 * Copyright (C) 2015 Stubhub.
 */

package io.bigdime.handler.file;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.AbstractSourceHandler;

/**
 * 
 * 
 * This handler parses the partition names from the input file name.
 * 
 * @formatter:off
 * date-partition-name parameter, if specified, reads the value of field specified by this name.
 * date-partition-input-format and date-partition-output-format must be specified if date-partition-name is specified.
 * 
 * If the date-partition-name parameter is specified, the value of the field is read, parsed based on value specified by date-partition-input-format and then formats based on value specified by date-partition-output-format. 
 * @formatter:on
 * 
 * @author Neeraj Jain
 * 
 * 
 */
@Component
@Scope("prototype")
public class PartitionParserHandler extends AbstractSourceHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(PartitionParserHandler.class));
	private String regex;
	private String headerName;
	private String handlerPhase;
	private char[] truncateChars;

	/**
	 * Should any characters be truncated from the string?
	 */
	private String truncateCharacters;
	/**
	 * say partition-names="account, date, country",
	 * partition-names-output-order could be = "country, account, date"
	 * 
	 */
	private String outputPartitionNamesStr;
	/**
	 * Use list as opposed to set, allow same partition value to be in multiple
	 * places in the path.
	 */
	private List<String> inputPartitionNames = new ArrayList<>();
	private List<String> outputPartitionNames = new ArrayList<>();
	private Map<String, String> partitionaNameValueMap = new HashMap<>();

	private String datePartitionName;
	private DateTimeFormatter datePartitionInputFormatter;
	private DateTimeFormatter datePartitionOutputFormatter;
	private int datePartitionIndex = 0;

	private String getParam(String paramName, String defaultValue) {
		String paramValue = PropertyHelper.getStringProperty(getPropertyMap(), paramName, defaultValue);
		return paramValue;
	}

	private String getRequiredParam(String paramName) {
		String paramValue = PropertyHelper.getStringProperty(getPropertyMap(), paramName);
		Preconditions.checkNotNull(paramValue, paramName + " must be configured in handler properties");
		return paramValue;
	}

	@Override
	public void build() throws AdaptorConfigurationException {
		handlerPhase = "building PartitionParserHandler";
		logger.info(handlerPhase, "handler_name={} handler_id={} \"properties={}\"", getName(), getId(),
				getPropertyMap());
		super.build();
		regex = getRequiredParam(PartitionNamesParserHandlerConstants.REGEX);

		final String partitionNames = getRequiredParam(PartitionNamesParserHandlerConstants.PARTITION_NAMES);
		outputPartitionNamesStr = getParam(PartitionNamesParserHandlerConstants.PARTITION_NAMES_OUTPUT_ORDER,
				partitionNames);

		headerName = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.HEADER_NAME);
		if (StringUtils.isBlank(headerName)) {
			headerName = ActionEventHeaderConstants.SOURCE_FILE_NAME;
		}
		truncateCharacters = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.TRUNCATE_CHARACTERS);
		if (truncateCharacters != null) {
			truncateChars = truncateCharacters.toCharArray();
		} else {
			truncateChars = new char[0];
		}

		datePartitionName = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME);
		String datePartitionInputFormat = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT);
		String datePartitionOutputFormat = PropertyHelper.getStringProperty(getPropertyMap(),
				PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT);
		logger.debug(handlerPhase,
				"headerId={} datePartitionName={} datePartitionInputFormat={} datePartitionOutputFormat={}", getId(),
				datePartitionName, datePartitionInputFormat, datePartitionOutputFormat);

		if (datePartitionName != null) {
			Preconditions.checkNotNull(datePartitionInputFormat,
					PartitionNamesParserHandlerConstants.DATE_PARTITION_INPUT_FORMAT
							+ "  must be configured in handler properties if "
							+ PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME + " is specified");
			Preconditions.checkNotNull(datePartitionInputFormat,
					PartitionNamesParserHandlerConstants.DATE_PARTITION_OUTPUT_FORMAT
							+ "  must be configured in handler properties if "
							+ PartitionNamesParserHandlerConstants.DATE_PARTITION_NAME + " is specified");

			datePartitionName = StringUtils.trim(datePartitionName);
			datePartitionInputFormatter = DateTimeFormat.forPattern(datePartitionInputFormat);

			datePartitionOutputFormatter = DateTimeFormat.forPattern(datePartitionOutputFormat);
		}

		final String[] partitionNameArray = partitionNames.split(",");
		for (int i = 0; i < partitionNameArray.length; i++) {
			String temp = StringUtils.trim(partitionNameArray[i]);
			inputPartitionNames.add(temp);
			if (datePartitionName != null && temp.equals(datePartitionName)) {
				datePartitionIndex = i;
				logger.debug(handlerPhase, "headerId={} datePartitionIndex={}", getId(), datePartitionIndex);
				break;
			}
		}

		final String[] partitionNameOutputArray = outputPartitionNamesStr.split(",");
		for (int i = 0; i < partitionNameOutputArray.length; i++) {
			String temp = StringUtils.trim(partitionNameOutputArray[i]);
			outputPartitionNames.add(temp);
		}

		logger.debug(handlerPhase,
				"headerId={} headerName={} regex={} truncateCharacters={} outputPartitionNamesStr={} outputPartitionNames={}",
				getId(), headerName, regex, truncateCharacters, outputPartitionNamesStr, outputPartitionNames);
	}

	@Override
	public Status process() throws HandlerException {
		handlerPhase = "processing PartitionParserHandler";
		Pattern p = Pattern.compile(regex);

		List<ActionEvent> actionEvents = getHandlerContext().getEventList();
		Preconditions.checkNotNull(actionEvents, "ActionEvents can't be null");
		Preconditions.checkArgument(!actionEvents.isEmpty(), "ActionEvents can't be empty");

		if (datePartitionName != null) {

		}
		for (ActionEvent actionEvent : actionEvents) {
			String rawString = actionEvent.getHeaders().get(headerName);
			logger.debug(handlerPhase, "handler_id={} rawString={}", getId(), rawString);

			Matcher m = p.matcher(rawString);
			String partitionValues = "";
			StringBuilder partitionValuesSb = null;
			while (m.find()) {
				logger.debug(handlerPhase, "found groups, count={}", m.groupCount());
				for (int i = 1; i <= m.groupCount(); i++) {
					String temp = m.group(i);
					for (char ch : truncateChars) {
						temp = temp.replace(String.valueOf(ch), "");
					}
					/*
					 * If the partition is a date partition, then format the
					 * value.
					 */
					if (datePartitionName != null && (i - 1) == datePartitionIndex) {
						logger.debug(handlerPhase, "handler_id={} inputTimestamp={}", getId(), temp);

						DateTime dt = datePartitionInputFormatter.parseDateTime(temp);
						String temp1 = datePartitionOutputFormatter.print(dt).trim();
						logger.debug(handlerPhase, "handler_id={} inputTimestamp={} outputTimestamp={}", getId(), temp,
								temp1);
						temp = temp1;
					}
					logger.debug(handlerPhase, "pName={} pValue={}", inputPartitionNames.get(i - 1), temp);
					partitionaNameValueMap.put(inputPartitionNames.get(i - 1), temp);
				}
			}
			logger.debug(handlerPhase, "handler_id={} partitionNameValueMap={}", getId(), partitionaNameValueMap);
			for (final String pName : outputPartitionNames) {
				if (partitionValuesSb == null) {
					partitionValuesSb = new StringBuilder();
				} else {
					partitionValuesSb.append(",");
				}
				partitionValuesSb.append(partitionaNameValueMap.get(pName));
			}
			partitionValues = partitionValuesSb.toString();
			logger.debug(handlerPhase, "handler_id={} partitionNames={} partitionValues={}", getId(),
					outputPartitionNamesStr, partitionValues);
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_NAMES, outputPartitionNamesStr);
			actionEvent.getHeaders().put(ActionEventHeaderConstants.HIVE_PARTITION_VALUES, partitionValues);
			processChannelSubmission(actionEvent);
		}
		return Status.READY;
	}

	public String getRegex() {
		return regex;
	}

	public String getHeaderName() {
		return headerName;
	}

	public String getPartitionNames() {
		return outputPartitionNamesStr;
	}

	public String getTruncateCharacters() {
		return truncateCharacters;
	}
}
