package io.bigdime.libs.hive.job;

/**
 * This component fetches status of a job that might have been fired by one of
 * the handlers; e.g., one of the handlers may run a map reduce job and it then
 * needs to know the status of the job. The job can be fired internally from
 * adaptor or from outside adaptor.
 * 
 * @author Neeraj Jain
 *
 * @param <I>
 *            type that represents the job identifier; e.g., for hive job, it'll
 *            be a String
 * @param <O>
 *            type representing the JobStatus
 */
public interface JobStatusFetcher<I, O extends HiveJobStatus> {

	/**
	 * Gets the status of the job that's identified by job.
	 * 
	 * @param jobId
	 *            job identifier, can be a jobName, jobId etc.
	 * @return JobStatus representing the status of the job, or null if the job
	 *         was not found
	 * @throws JobStatusException
	 *             if the fetcher was not able to fetch the status of the job
	 */
	public O getStatusForJob(I jobId) throws JobStatusException;

	/**
	 * 
	 * @param jobId
	 * @return
	 * @throws JobStatusException
	 */
	public O getStatusForJobWithRetry(I jobId) throws JobStatusException;

}
