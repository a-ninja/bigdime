package io.bigdime.libs.hdfs.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class YarnJobHelper {
	private static final Logger logger = LoggerFactory.getLogger(YarnJobHelper.class);

	private JobClient getJobClient(Configuration conf) throws IOException {
		JobClient jc = new JobClient(conf);
		logger.debug("Got connection, jc={}", jc);
		logger.debug("Cluster Status, status_null={}", jc.getClusterStatus() == null);
		return jc;
	}

	public JobStatus[] getStatusForAllJobs(Configuration conf) throws IOException {
		JobClient jc = null;
		try {
			jc = getJobClient(conf);
			JobStatus[] jobStatus = jc.getAllJobs();
			logger.debug("jobStatus, status={} length={}", jobStatus.toString(), jobStatus.length);
			jc.close();
			return jobStatus;
		} finally {
			if (jc != null)
				jc.close();
		}
	}

	public JobStatus[] getStatusForQueue(final String queueName, Configuration conf) throws IOException {
		JobClient jc = null;
		try {
			jc = getJobClient(conf);
			logger.debug("_message=\"status for queue\" queue_name={}", queueName);
			JobStatus[] jobStatus = jc.getJobsFromQueue(queueName);
			logger.debug("jobStatus, status={} length={}", jobStatus.toString(), jobStatus.length);
			return jobStatus;
		} finally {
			if (jc != null)
				jc.close();
		}
	}

	public JobStatus[] getStatusForJob(final String jobName, Configuration conf) throws IOException {
		JobStatus[] jobStatus = getStatusForAllJobs(conf);
		logger.debug("getStatusForJob, status={} length={} jobName={}", jobStatus.toString(), jobStatus.length,
				jobName);
		List<JobStatus> statuses = new ArrayList<>();
		for (JobStatus js : jobStatus) {
			if (js.getJobName().contains(jobName)) {
				statuses.add(js);
			}
		}
		return statuses.toArray(new JobStatus[statuses.size()]);
	}

	public JobStatus getPositiveStatusForJob(String jobName, Configuration conf) throws IOException {
		int runState = -1;
		JobStatus latestJob = null;
		JobStatus[] jobStatuses = getStatusForJob(jobName, conf);
		for (JobStatus js : jobStatuses) {
			logger.debug("getPositiveStatusForJob: jobId={} jobName={} runState={}", js.getJobID(), js.getJobName(),
					js.getRunState());
			if (latestJob == null)
				latestJob = js;
			else if (latestJob.getStartTime() < js.getStartTime()) {
				latestJob = js;
			}
		}
		if (latestJob != null) {
			runState = latestJob.getRunState();
			if (runState == JobStatus.RUNNING || runState == JobStatus.PREP || runState == JobStatus.SUCCEEDED) {
				logger.info(
						"_message=\"found a running or prep or succeeded jobStatus\" jobId={} jobName={} runState={}",
						latestJob.getJobID(), latestJob.getJobName(), latestJob.getRunState());
				return latestJob;
			}
		}
		logger.info("_message=\"getPositiveStatusForJob: no status found\"  jobName={}", jobName);
		return null;
	}

	private static long sleepTimeBetweenStatusCall = TimeUnit.SECONDS.toMillis(60);
	private static long maxWait = TimeUnit.MINUTES.toMillis(60);

	public JobStatus getStatusForNewJob(String jobName, Configuration conf) throws IOException {
		long startTime = System.currentTimeMillis();
		long endTime = startTime;
		JobStatus latestJob = null;
		do {
			endTime = System.currentTimeMillis();
			logger.debug("_message=\"after submitting job, getting status of job\" jobName={}", jobName);
			JobStatus[] jobStatuses = getStatusForJob(jobName, conf);
			if (jobStatuses != null && jobStatuses.length > 0) {
				for (JobStatus js : jobStatuses) {
					if (latestJob == null)
						latestJob = js;
					else if (latestJob.getStartTime() < js.getStartTime()) {
						latestJob = js;
					}
					logger.info("_message=\"after submitting job, got job status\" jobId={} jobName={} runState={}",
							js.getJobID(), js.getJobName(), js.getRunState());
				}
				return latestJob;
			} else {
				try {
					Thread.sleep(sleepTimeBetweenStatusCall);
				} catch (Exception ex) {
					logger.info("Thread interrupted", ex);
				}
			}
		} while ((endTime - startTime) < maxWait);
		logger.info("_message=\"getStatusForNewJob: no status found\"  jobName={}", jobName);
		return null;
	}

	public JobStatus getStatusForCompletedJob(String jobName, Configuration conf) throws IOException {
		long startTime = System.currentTimeMillis();
		long endTime = startTime;
		do {
			endTime = System.currentTimeMillis();
			logger.debug("_message=\"after completing job, getting status of job\" jobName={}", jobName);
			JobStatus[] jobStatuses = getStatusForJob(jobName, conf);
			if (jobStatuses != null && jobStatuses.length > 0) {
				for (JobStatus js : jobStatuses) {
					logger.info("_message=\"after completing job, got job status\" jobId={} jobName={} runState={}",
							js.getJobID(), js.getJobName(), js.getRunState());

					if (js.getRunState() == JobStatus.FAILED || js.getRunState() == JobStatus.KILLED
							|| js.getRunState() == JobStatus.SUCCEEDED)
						return js;
				}
			} else {
				try {
					Thread.sleep(sleepTimeBetweenStatusCall);
				} catch (Exception ex) {
					logger.info("Thread interrupted", ex);
				}
			}
		} while ((endTime - startTime) < maxWait);
		logger.info("_message=\"getStatusForNewJob: no status found\"  jobName={}", jobName);
		return null;
	}

	public static JobStatus getNewestJobStatusFromArray(JobStatus[] jobStatuses) {
		JobStatus newestJob = null;
		if (jobStatuses == null || jobStatuses.length == 0) {
			logger.info("jobStatuses is null, null will be returned as the newest job");
		}
		if (jobStatuses != null) {
			for (JobStatus js : jobStatuses) {
				logger.debug("getNewestJobStatusFromArray: jobId={} jobName={} runState={}", js.getJobID(),
						js.getJobName(), js.getRunState());
				if (newestJob == null)
					newestJob = js;
				else if (newestJob.getStartTime() < js.getStartTime()) {
					newestJob = js;
				}
			}
		}
		return newestJob;
	}
}