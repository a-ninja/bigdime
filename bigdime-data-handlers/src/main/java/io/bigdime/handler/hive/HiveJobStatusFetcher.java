package io.bigdime.handler.hive;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.handler.JobStatusException;
import io.bigdime.handler.JobStatusFetcher;
import io.bigdime.libs.hdfs.WebHdfsException;

@Component("hiveJobStatusFether")
@Scope("prototype")
public class HiveJobStatusFetcher implements JobStatusFetcher<HiveJobSpec, HiveJobStatus> {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(HiveJobStatusFetcher.class));

	@Value("${hive.job.status.sleep.seconds:60}")
	private long sleepTimeBetweenStatusCallSeconds;
	private long sleepTimeBetweenStatusCall;

	@Value("${hive.job.status.max.wait.seconds:3600}")
	private long maxWaitSeconds;

	@Value("${mapreduce.framework.name:yarn}")
	private String mapreduceFrameworkName;

	@Value("${hadoop.security.authentication:kerberos}")
	private String hadoopSecurityAuthentication;

	private long maxWait;

	private JobClient jobClient;

	@Autowired
	private JobClientFactory jobClientFactory;

	@Value("${yarn.site.xml.path}")
	private String yarnSiteXml;

	@Value("${hive.jdbc.user.name}")
	private String userName;

	@Value("${hive.jdbc.secret}")
	private String password;

	@Autowired
	private HiveJobOutputFileValidator hiveJobOutputFileValidator;

	private Configuration conf;

	@PostConstruct
	public void init() throws Exception {
		logger.info("HiveJobStatusFetcher.PostConstruct",
				"yarnSiteXml={} mapreduceFrameworkName={} hadoopSecurityAuthentication={} userName={} secret={}, this={}",
				yarnSiteXml, mapreduceFrameworkName, hadoopSecurityAuthentication, userName, password, this);
		sleepTimeBetweenStatusCall = TimeUnit.SECONDS.toMillis(sleepTimeBetweenStatusCallSeconds);
		maxWait = TimeUnit.SECONDS.toMillis(maxWaitSeconds);

		conf = new Configuration();
		conf.set("mapreduce.framework.name", mapreduceFrameworkName);
		conf.set("hadoop.security.authentication", hadoopSecurityAuthentication);
		if (yarnSiteXml != null) {
			InputStream yarnSiteXmlInputStream = new FileInputStream(yarnSiteXml);
			conf.addResource(yarnSiteXmlInputStream);
			UserGroupInformation.setConfiguration(conf);
			UserGroupInformation.loginUserFromKeytab(userName, password);
			jobClient = jobClientFactory.createJobClient(conf);
		}
		logger.info("HiveJobStatusFetcher.PostConstruct done", "yarnSiteXml={}, this={}", yarnSiteXml, this);
	}

	public JobStatus[] getStatusForAllJobs() throws JobStatusException {
		logger.info("getStatusForAllJobs",
				"yarnSiteXml={}, this={} mapreduceFrameworkName={} hadoopSecurityAuthentication={}", yarnSiteXml, this,
				mapreduceFrameworkName, hadoopSecurityAuthentication);
		try {
			jobClient.init(new JobConf(conf));
			Configuration conf = new Configuration();
			conf.set("mapreduce.framework.name", mapreduceFrameworkName);
			conf.set("hadoop.security.authentication", hadoopSecurityAuthentication);
			if (yarnSiteXml != null) {
				InputStream yarnSiteXmlInputStream = new FileInputStream(yarnSiteXml);
				conf.addResource(yarnSiteXmlInputStream);

				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromKeytab(userName, password);
			}
			JobStatus[] jobStatus = jobClient.getAllJobs();
			logger.debug("getStatusForAllJobs", "status={} length={}", jobStatus.toString(), jobStatus.length);

			return jobStatus;
		} catch (IOException ex) {
			logger.warn("getStatusForAllJobs", "_message=\"unable to get the job status\"", ex);
			throw new JobStatusException("unable to get the job status:", ex);
		} finally {
			try {
				jobClient.close();
			} catch (final IOException ex) {
				logger.warn("getStatusForAllJobs", "_message=\"exception while trying to close the jobClient\"", ex);
			}
		}
	}

	public JobStatus[] getAllStatusesForJob(final String jobName) throws JobStatusException {
		final List<JobStatus> statuses = new ArrayList<>();
		try {
			JobStatus[] jobStatus = getStatusForAllJobs();
			logger.debug("getAllStatusesForJob", "status={} length={} jobName={}", jobStatus.toString(),
					jobStatus.length, jobName);
			for (JobStatus js : jobStatus) {
				if (js.getJobName().contains(jobName)) {
					statuses.add(js);
				}
			}
		} catch (JobStatusException ex) {
			logger.warn("getAllStatusesForJob", "_message=\"unable to get the job status\" jobName={} attempt={}",
					jobName, ex);
			throw ex;
		}
		return statuses.toArray(new JobStatus[statuses.size()]);
	}

	public HiveJobStatus getStatusForJobWithRetry(final HiveJobSpec jobSpec) throws JobStatusException {
		String jobName = jobSpec.getJobName();
		final long startTime = System.currentTimeMillis();
		long endTime = startTime;
		HiveJobStatus hiveJobStatus = null;
		int attempt = 0;
		JobStatusException jobEx = null;
		do {
			attempt++;
			try {
				jobEx = null;
				hiveJobStatus = getStatusForJob(jobSpec);
			} catch (JobStatusException ex) {
				logger.warn("getStatusForJobWithRetry",
						"_message=\"unable to get the job status\" jobName={} attempt={}", jobName, attempt, ex);
				jobEx = ex;
			}
			if (hiveJobStatus == null) {
				try {
					logger.info("getStatusForJobWithRetry", "_message=\"sleeping before retry.\"  attempt={}", attempt);
					Thread.sleep(sleepTimeBetweenStatusCall);
				} catch (Exception ex) {
					logger.info("getStatusForJobWithRetry", "_message=\"Thread interrupted.\"  attempt={}", attempt,
							ex);

				}
			} else {
				break;
			}
			endTime = System.currentTimeMillis();
		} while ((endTime - startTime) < maxWait);

		if (jobEx != null)
			throw jobEx;
		return hiveJobStatus;
	}

	public HiveJobStatus getStatusForJob(final HiveJobSpec jobSpec) throws JobStatusException {
		String jobName = jobSpec.getJobName();
		JobStatus overallJobStatus = null;
		JobStatus newestJob = null;
		State state = null;
		final List<JobStatus> jobStatusList = new ArrayList<>();
		HiveJobStatus hiveJobStatus = null;
		JobStatus[] jobStatuses;
		jobStatuses = getAllStatusesForJob(jobName);
		for (JobStatus js : jobStatuses) {
			logger.info("getStatusForJob", "jobId={} jobName={} runState={}", js.getJobID(), js.getJobName(),
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
			logger.info("getStatusForJob", "found a not null jobStatus. state={}", state);
			hiveJobStatus = new HiveJobStatus();
			hiveJobStatus.setNewestJobStatus(newestJob);
			hiveJobStatus.setOverallStatus(overallJobStatus);
			hiveJobStatus.setStageStatuses(jobStatusList);

			if (hiveJobStatus.getOverallStatus().getState() == State.SUCCEEDED) {
				try {
					boolean validated = hiveJobOutputFileValidator.validateOutputFile(jobSpec.getOutputDirectoryPath());
					if (validated) {
						logger.info("getStatusForJob",
								"_message=\"found a SUCCEEDED jobStatus and validated outputDirectory.\" jobName={} outputDirectoryPath={}",
								jobName, jobSpec.getOutputDirectoryPath());
					} else {
						logger.warn("getStatusForJob",
								"_message=\"found a SUCCEEDED jobStatus, but outputDirectory not found\" jobName={} outputDirectoryPath={}",
								jobName, jobSpec.getOutputDirectoryPath());
						hiveJobStatus = null;
					}
				} catch (IOException | WebHdfsException ex) {
					logger.warn("getStatusForJob",
							"_message=\"found a SUCCEEDED jobStatus, but unable to validate outputDirectory in hdfs.\" jobName={}",
							jobName, jobSpec.getOutputDirectoryPath(), ex);
					throw new JobStatusException(
							"unable to validate outputDirectory in hdfs:" + jobSpec.getOutputDirectoryPath() + ":", ex);
				}
			}
		} else {
			logger.info("getStatusForJob", "found a null jobStatus");
		}
		return hiveJobStatus;
	}

}