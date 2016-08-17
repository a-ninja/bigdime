package io.bigdime.handler.hive;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import io.bigdime.core.InputDescriptor;

public class HiveReaderDescriptor implements InputDescriptor<String> {

	private String entityName;
	private String hiveConfDate;
	private String hiveConfDirectory;
	private String hiveQuery;
	private static final String INPUT_DESCRIPTOR_PREFIX = "handlerClass:io.bigdime.handler.hive.HiveJdbcReaderHandler";
	private String jobName;

	public HiveReaderDescriptor(final String _entityName, final String _hiveConfDate, final String _hiveConfDirectory,
			final String _hiveQuery) {
		this.entityName = _entityName;
		this.hiveConfDate = _hiveConfDate;
		this.hiveConfDirectory = _hiveConfDirectory;
		this.hiveQuery = _hiveQuery;
	}

	@Override
	public String getNext(List<String> availableInputs, String lastInput) {
		isValid(availableInputs, lastInput);

		int indexOfLastInput = availableInputs.indexOf(lastInput);
		if (availableInputs.size() > indexOfLastInput + 1) {
			return availableInputs.get(indexOfLastInput + 1);
		}
		return null;
	}

	private boolean isValid(List<String> availableInputs, String lastInput) {
		if (availableInputs == null) {
			throw new IllegalArgumentException();
		}
		if (StringUtils.isBlank(lastInput))
			throw new IllegalArgumentException();
		return true;
	}

	@Override
	public void parseDescriptor(String descriptor) {
		throw new UnsupportedOperationException();
	}

	public String getInputDescriptorString() {
		return INPUT_DESCRIPTOR_PREFIX + ",hiveConfDate:" + hiveConfDate + ",hiveConfDirectory:" + hiveConfDirectory;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entityName == null) ? 0 : entityName.hashCode());
		result = prime * result + ((hiveConfDate == null) ? 0 : hiveConfDate.hashCode());
		result = prime * result + ((hiveConfDirectory == null) ? 0 : hiveConfDirectory.hashCode());
		result = prime * result + ((hiveQuery == null) ? 0 : hiveQuery.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HiveReaderDescriptor other = (HiveReaderDescriptor) obj;
		if (entityName == null) {
			if (other.entityName != null)
				return false;
		} else if (!entityName.equals(other.entityName))
			return false;
		if (hiveConfDate == null) {
			if (other.hiveConfDate != null)
				return false;
		} else if (!hiveConfDate.equals(other.hiveConfDate))
			return false;
		if (hiveConfDirectory == null) {
			if (other.hiveConfDirectory != null)
				return false;
		} else if (!hiveConfDirectory.equals(other.hiveConfDirectory))
			return false;
		if (hiveQuery == null) {
			if (other.hiveQuery != null)
				return false;
		} else if (!hiveQuery.equals(other.hiveQuery))
			return false;
		return true;
	}

	public String getHiveConfDirectory() {
		return hiveConfDirectory;
	}

	public void setHiveConfDirectory(String hiveConfDirectory) {
		this.hiveConfDirectory = hiveConfDirectory;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	@Override
	public String toString() {
		return "HiveReaderDescriptor [entityName=" + entityName + ", hiveConfDate=" + hiveConfDate
				+ ", hiveConfDirectory=" + hiveConfDirectory + ", hiveQuery=" + hiveQuery + ", jobName=" + jobName
				+ "]";
	}
}
