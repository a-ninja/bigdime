package io.bigdime.handler.hive;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.handler.JobStatusFetcher;

@Component("hiveJobStatusFether")
@Scope("prototype")
public class HiveJobStatusFetcher implements JobStatusFetcher<String, HiveJobStatus> {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HiveJobStatusFetcher.class));

	@Value("${hive.job.status.sleep.seconds:60}")
	private long sleepTimeBetweenStatusCallSeconds;
	private long sleepTimeBetweenStatusCall;

	@Value("${hive.job.status.max.wait.seconds:3600}")
	private long maxWaitSeconds;

	private long maxWait;

	private JobClient jobClient;

	@Value("${yarn.site.xml.path}")
	private String yarnSiteXml;

	@Value("${hive.jdbc.user.name}")
	private String userName;

	@Value("${hive.jdbc.secret}")
	private String password;

	@PostConstruct
	public void init() throws Exception {
		logger.info("HiveJobStatusFetcher.PostConstruct", "yarnSiteXml={}, this={}", yarnSiteXml, this);
		sleepTimeBetweenStatusCall = TimeUnit.SECONDS.toMillis(sleepTimeBetweenStatusCallSeconds);
		maxWait = TimeUnit.SECONDS.toMillis(maxWaitSeconds);

		Configuration conf = new Configuration();
		if (yarnSiteXml != null) {
			InputStream yarnSiteXmlInputStream = HiveJobStatusFetcher.class.getClassLoader()
					.getResourceAsStream(yarnSiteXml);
			conf.addResource(yarnSiteXmlInputStream);
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(userName, password);
			jobClient = new JobClient(conf);
		}
	}

	public JobStatus[] getStatusForAllJobs() throws IOException {
		JobStatus[] jobStatus = jobClient.getAllJobs();
		logger.debug("getStatusForAllJobs", "status={} length={}", jobStatus.toString(), jobStatus.length);
		jobClient.close();
		return jobStatus;
	}

	public JobStatus[] getAllStatusesForJob(final String jobName) throws IOException {
		JobStatus[] jobStatus = getStatusForAllJobs();
		logger.debug("getStatusForJob", "status={} length={} jobName={}", jobStatus.toString(), jobStatus.length,
				jobName);
		final List<JobStatus> statuses = new ArrayList<>();
		for (JobStatus js : jobStatus) {
			if (js.getJobName().contains(jobName)) {
				statuses.add(js);
			}
		}
		return statuses.toArray(new JobStatus[statuses.size()]);
	}

	/**
	 * Check the status of the job and return the status of all the jobs.
	 * 
	 * @param jobName
	 * @return
	 * @throws IOException
	 */
	public HiveJobStatus getStatusForJob(String jobName) {
		final long startTime = System.currentTimeMillis();
		long endTime = startTime;
		JobStatus overallJobStatus = null;
		JobStatus newestJob = null;
		State state = null;
		final List<JobStatus> jobStatusList = new ArrayList<>();
		HiveJobStatus hiveJobStatus = null;
		do {
			try {
				JobStatus[] jobStatuses = getAllStatusesForJob(jobName);
				for (JobStatus js : jobStatuses) {
					logger.info("getJobStatus", "jobId={} jobName={} runState={}", js.getJobID(), js.getJobName(),
							js.getRunState());
					jobStatusList.add(js);
					State stageState = js.getState();
					if (newestJob == null) {
						newestJob = js;
						state = stageState;
						overallJobStatus = js;
					} else if (newestJob.getStartTime() < js.getStartTime()) {
						newestJob = js;
					}
					/*-
					 * 
					 * if the earlier stage failed or killed, 
					 * 	runState should be left alone as failed or killed
					 * else if earlier stage was successful
					 * 	runState = new state
					 * else if new state is PREP or RUNNING
					 * 	runState = new state
					 * 
					 * if the overall state is killed or failed
					 * 	leave it alone
					 * else if the overall state is PREP
					 * 	leave it alone
					 * else if the overall state is running and the stage state is not SUCCEEDED
					 * 	set the overall state = stage state
					 * else if the overall state is succeeded
					 * 	set the overall state = stage state
					 * 
					 */

					switch (state) {
					case RUNNING:
						if (stageState != State.SUCCEEDED) {
							state = stageState;
							overallJobStatus = js;
						}
						break;
					case SUCCEEDED:
						state = stageState;
						overallJobStatus = js;
						break;
					case FAILED:
						break;
					case PREP:
						if (stageState == State.KILLED || stageState == State.FAILED) {
							state = stageState;
							overallJobStatus = js;
						}
						break;
					case KILLED:
						break;
					}
				}

				if (newestJob != null) {
					logger.info("getJobStatus", "found a not null jobStatus. state={}", state);
					hiveJobStatus = new HiveJobStatus();
					hiveJobStatus.setNewestJobStatus(newestJob);
					hiveJobStatus.setOverallStatus(overallJobStatus);
					hiveJobStatus.setStageStatuses(jobStatusList);
				} else {
					logger.info("getJobStatus", "found a null jobStatus");
				}
			} catch (Exception ex) {
				logger.warn("getJobStatus", "_message=\"getJobStatus: unable to get the job status\" jobName={}",
						jobName, ex);
			}
			if (hiveJobStatus == null) {
				try {
					logger.info("getJobStatus", "sleeping before retry");
					Thread.sleep(sleepTimeBetweenStatusCall);
				} catch (Exception ex) {
					logger.info("getJobStatus", "Thread interrupted", ex);
				}
			} else {
				break;
			}
			endTime = System.currentTimeMillis();
		} while ((endTime - startTime) < maxWait);
		return hiveJobStatus;
	}
}