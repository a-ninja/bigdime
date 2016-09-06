package io.bigdime.handler.hive;

import java.util.List;

import org.apache.hadoop.mapred.JobStatus;

public class HiveJobStatus implements io.bigdime.handler.JobStatus {

	private JobStatus overallStatus;
	private JobStatus newestJobStatus;
	private List<JobStatus> stageStatuses;

	public JobStatus getOverallStatus() {
		return overallStatus;
	}

	public void setOverallStatus(final JobStatus _overallStatus) {
		this.overallStatus = _overallStatus;
	}

	public List<JobStatus> getStageStatuses() {
		return stageStatuses;
	}

	public void setStageStatuses(final List<JobStatus> _stageStatuses) {
		this.stageStatuses = _stageStatuses;
	}

	public JobStatus getNewestJobStatus() {
		return newestJobStatus;
	}

	public void setNewestJobStatus(final JobStatus _latestJobStatus) {
		this.newestJobStatus = _latestJobStatus;
	}
}