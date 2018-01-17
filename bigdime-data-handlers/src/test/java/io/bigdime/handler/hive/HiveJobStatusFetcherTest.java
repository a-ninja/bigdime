package io.bigdime.handler.hive;

import io.bigdime.libs.hive.job.*;
import io.bigdime.libs.hdfs.WebHdfsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

@ContextConfiguration(classes = HiveJobStatusFetcherTestConfig.class)
public class HiveJobStatusFetcherTest extends AbstractTestNGSpringContextTests {

	@Autowired
  HiveJobStatusFetcher hiveJobStatusFetcher;

	@Autowired
	JobClient mockJobClient;
	@Autowired
	JobClientFactory jobClientFactory;

	@Autowired
	HiveJobOutputFileValidator hiveJobOutputFileValidator;

	@Value("${yarn.site.xml.path}")
	private String yarnSiteXml;

	@BeforeMethod
	public void init() throws IOException {
		System.out.println("init");

		MockitoAnnotations.initMocks(this);
		ReflectionTestUtils.setField(hiveJobStatusFetcher, "jobClient", mockJobClient);
		ReflectionTestUtils.setField(hiveJobStatusFetcher, "hiveJobOutputFileValidator", hiveJobOutputFileValidator);
		Mockito.when(jobClientFactory.createJobClient(Mockito.any(Configuration.class))).thenReturn(mockJobClient);
	}

	@AfterMethod
	public void tearDown() {
		Mockito.reset(mockJobClient);
		Mockito.reset(jobClientFactory);
	}

	@Test
	public void testPostConstruct() throws Exception {
		Assert.assertNotNull(ReflectionTestUtils.getField(hiveJobStatusFetcher, "jobClient"));
		// Mockito.verify(jobClientFactory,
		// Mockito.times(1)).createJobClient(Mockito.any(Configuration.class));
	}

	@Test
	public void testInit() throws Exception {
		// Mockito.when(jobClientFactory.createJobClient(Mockito.any(Configuration.class))).thenReturn(mockJobClient);
		hiveJobStatusFetcher.init();
		Assert.assertNotNull(ReflectionTestUtils.getField(hiveJobStatusFetcher, "jobClient"));
		Mockito.verify(jobClientFactory, Mockito.times(1)).createJobClient(Mockito.any(Configuration.class));
	}

	@Test
	public void testInitWithYarnSiteXmlAsNull() throws Exception {
		ReflectionTestUtils.setField(hiveJobStatusFetcher, "jobClient", null);
		ReflectionTestUtils.setField(hiveJobStatusFetcher, "yarnSiteXml", null);
		hiveJobStatusFetcher.init();
		Assert.assertNull(ReflectionTestUtils.getField(hiveJobStatusFetcher, "jobClient"));
		Mockito.verify(jobClientFactory, Mockito.times(0)).createJobClient(Mockito.any(Configuration.class));
	}

	@Test
	public void testGetStatusForAllJob() throws JobStatusException, IOException {

		System.out.println(
				"yarnSiteXml=" + yarnSiteXml + ", hiveJobStatusFetcher=" + hiveJobStatusFetcher + ", yarnSiteXml=");
		JobStatus[] jsArray = new JobStatus[3];

		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-1");
		jsArray[2] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-2");

		Mockito.when(mockJobClient.getAllJobs()).thenReturn(jsArray);

		JobStatus[] actualJsArray = hiveJobStatusFetcher.getStatusForAllJobs();
		Assert.assertEquals(actualJsArray.length, jsArray.length);
		for (JobStatus js : actualJsArray) {
			Assert.assertEquals(js.getState(), State.SUCCEEDED);
		}
	}

	@Test
	public void testAllStatusesForJob() throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[4];

		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-1");
		jsArray[2] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-2");
		jsArray[3] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-3", "unit-other-job");

		Mockito.when(mockJobClient.getAllJobs()).thenReturn(jsArray);

		JobStatus[] actualJsArray = hiveJobStatusFetcher.getAllStatusesForJob("unit-job");
		Assert.assertEquals(actualJsArray.length, 3);
		for (JobStatus js : actualJsArray) {
			Assert.assertEquals(js.getState(), State.SUCCEEDED);
		}
	}
	

	@Test
	public void testGetAllStatusesForJobNotValidJobName() throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[1];

		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0", "unit-invalid-name");

		Mockito.when(mockJobClient.getAllJobs()).thenReturn(jsArray);

		JobStatus[] actualJsArray = hiveJobStatusFetcher.getAllStatusesForJob("unit-job");
		Assert.assertEquals(actualJsArray.length, 0);
	}

	@Test
	public void testGetStatusForJobForStatusNotFound() throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[1];

		ReflectionTestUtils.setField(hiveJobStatusFetcher, "sleepTimeBetweenStatusCall", 1);
		ReflectionTestUtils.setField(hiveJobStatusFetcher, "maxWait", 5);
		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0", "unit-invalid-job");
		Mockito.when(mockJobClient.getAllJobs()).thenReturn(jsArray);
		HiveJobSpec hiveJobSpec = new HiveJobSpec("unit-job", null);

		HiveJobStatus actualJsArray = hiveJobStatusFetcher.getStatusForJob(hiveJobSpec);
		Assert.assertNull(actualJsArray);
	}
	
	@Test
	public void testGetStatusForJobWithRetry() throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[1];

		ReflectionTestUtils.setField(hiveJobStatusFetcher, "sleepTimeBetweenStatusCall", 1);
		ReflectionTestUtils.setField(hiveJobStatusFetcher, "maxWait", 5);
		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0", "unit-invalid-job");
		Mockito.when(mockJobClient.getAllJobs()).thenReturn(jsArray);
		HiveJobSpec hiveJobSpec = new HiveJobSpec("unit-job", null);

		HiveJobStatus actualJsArray = hiveJobStatusFetcher.getStatusForJobWithRetry(hiveJobSpec);
		Assert.assertNull(actualJsArray);
	}

	/**
	 * If the getAllJobs from JobClient throws an exception, the getStatusForJob
	 * mathod should return a null HiveJobStatus.
	 * 
	 * @throws IOException
	 */
	@Test(expectedExceptions = JobStatusException.class)
	public void testGetStatusForJobWithException() throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[1];

		ReflectionTestUtils.setField(hiveJobStatusFetcher, "sleepTimeBetweenStatusCall", 1);
		ReflectionTestUtils.setField(hiveJobStatusFetcher, "maxWait", 5);
		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0", "unit-invalid-job");
		Mockito.when(mockJobClient.getAllJobs()).thenThrow(new IOException());
		HiveJobSpec hiveJobSpec = new HiveJobSpec("unit-job", null);
		HiveJobStatus actualJsArray = hiveJobStatusFetcher.getStatusForJob(hiveJobSpec);
		Assert.assertNull(actualJsArray);
		Mockito.verify(mockJobClient, Mockito.atLeast(1)).getAllJobs();
		Mockito.verify(mockJobClient, Mockito.atLeast(1)).init(Mockito.any(JobConf.class));
		Mockito.verify(mockJobClient, Mockito.atLeast(1)).close();
	}

	@Test
	public void testGetJobStatus_FAILED() throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[3];

		long time = System.currentTimeMillis();
		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0", "unit-job", time);
		jsArray[1] = getJobWithStatus(State.FAILED, "unit-job-id-1", "unit-job", time - 1);
		jsArray[2] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-2", "unit-job", time + 1);
		testGetJobStatus0(jsArray, State.FAILED, "unit-job-id-1");
	}

	@Test
	public void testGetJobStatus_PREP() throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[3];

		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(State.PREP, "unit-job-id-1");
		jsArray[2] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-2");
		testGetJobStatus0(jsArray, State.PREP, "unit-job-id-1");
	}

	@Test
	public void testGetJobStatus_SUCCEEDED() throws IOException, JobStatusException, WebHdfsException {
		JobStatus[] jsArray = new JobStatus[3];
		Mockito.when(hiveJobOutputFileValidator.validateOutputFile(Mockito.anyString())).thenReturn(true);

		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-1");
		jsArray[2] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-2");
		testGetJobStatus0(jsArray, State.SUCCEEDED, "unit-job-id-2");
	}

	/**
	 * If the overall state is PREP and new stage's state is KILLED, overall
	 * state should be KILLED.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetJobStatusWhereStageStateIsKILLED() throws IOException, JobStatusException {
		testGetJobStatusWhereStageStateIsFAILEDOrKILLED(State.KILLED);
	}

	/**
	 * If the overall state is PREP and new stage's state is FAILED, overall
	 * state should be FAILED.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testGetJobStatusWhereStageStateIsFAILED() throws IOException, JobStatusException {
		testGetJobStatusWhereStageStateIsFAILEDOrKILLED(State.FAILED);
	}

	@Test
	public void testGetJobStatusWhereOverallStateIsKILLED() throws IOException, JobStatusException {
		testGetJobStatusWhereOverallStateIsFAILEDOrKILLED(State.KILLED);
	}

	@Test
	public void testGetJobStatusWhereOverallStateIsFAILED() throws IOException, JobStatusException {
		testGetJobStatusWhereOverallStateIsFAILEDOrKILLED(State.FAILED);
	}

	/**
	 * If the overall state is KILLED and new stage's state is
	 * PREP/RUNNING/SUCCEEDED/FAILED, overall state should be KILLED.
	 * 
	 * @throws IOException
	 * @throws JobStatusException
	 */
	public void testGetJobStatusWhereOverallStateIsFAILEDOrKILLED(State failedOrKilled)
			throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[2];

		jsArray[0] = getJobWithStatus(failedOrKilled, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(State.PREP, "unit-job-id-1");
		testGetJobStatus0(jsArray, failedOrKilled, "unit-job-id-0");

		jsArray[0] = getJobWithStatus(failedOrKilled, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(State.RUNNING, "unit-job-id-1");
		testGetJobStatus0(jsArray, failedOrKilled, "unit-job-id-0");

		jsArray[0] = getJobWithStatus(failedOrKilled, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-1");
		testGetJobStatus0(jsArray, failedOrKilled, "unit-job-id-0");

		State killedOrFailed = (failedOrKilled == State.KILLED) ? State.FAILED : State.KILLED;
		jsArray[0] = getJobWithStatus(failedOrKilled, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(killedOrFailed, "unit-job-id-1");
		testGetJobStatus0(jsArray, failedOrKilled, "unit-job-id-0");
	}

	public void testGetJobStatusWhereStageStateIsFAILEDOrKILLED(State failedOrKilled)
			throws IOException, JobStatusException {
		JobStatus[] jsArray = new JobStatus[2];

		jsArray[0] = getJobWithStatus(State.PREP, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(failedOrKilled, "unit-job-id-1");
		testGetJobStatus0(jsArray, failedOrKilled, "unit-job-id-1");

		jsArray[0] = getJobWithStatus(State.RUNNING, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(failedOrKilled, "unit-job-id-1");
		testGetJobStatus0(jsArray, failedOrKilled, "unit-job-id-1");

		jsArray[0] = getJobWithStatus(State.SUCCEEDED, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(failedOrKilled, "unit-job-id-1");
		testGetJobStatus0(jsArray, failedOrKilled, "unit-job-id-1");

		State killedOrFailed = (failedOrKilled == State.KILLED) ? State.FAILED : State.KILLED;
		jsArray[0] = getJobWithStatus(killedOrFailed, "unit-job-id-0");
		jsArray[1] = getJobWithStatus(failedOrKilled, "unit-job-id-1");
		testGetJobStatus0(jsArray, killedOrFailed, "unit-job-id-0");
	}

	public void testGetJobStatus0(JobStatus[] jsArray, State expectedState, String expectedJobId)
			throws JobStatusException, IOException {
		Mockito.when(mockJobClient.getAllJobs()).thenReturn(jsArray);
		HiveJobSpec hiveJobSpec = new HiveJobSpec("unit-job", null);
		HiveJobStatus actualJsArray = hiveJobStatusFetcher.getStatusForJob(hiveJobSpec);
		Assert.assertEquals(actualJsArray.getOverallStatus().getState(), expectedState);
		Assert.assertEquals(actualJsArray.getOverallStatus().getJobID().toString(), expectedJobId);
	}

	private JobStatus getJobWithStatus(State state, String jobId, String jobName, long startTime) {
		JobStatus js = Mockito.mock(JobStatus.class);
		Mockito.when(js.getRunState()).thenReturn(state.getValue());
		Mockito.when(js.getState()).thenReturn(state);
		Mockito.when(js.getJobName()).thenReturn(jobName);
		Mockito.when(js.getStartTime()).thenReturn(startTime);
		JobID jobID = Mockito.mock(JobID.class);
		Mockito.when(jobID.toString()).thenReturn(jobId);
		Mockito.when(js.getJobID()).thenReturn(jobID);
		return js;

	}

	private JobStatus getJobWithStatus(State state, String jobId, String jobName) {
		return getJobWithStatus(state, jobId, jobName, System.currentTimeMillis());
	}

	private JobStatus getJobWithStatus(State state, String jobId) {
		return getJobWithStatus(state, jobId, "unit-job");
	}
}
