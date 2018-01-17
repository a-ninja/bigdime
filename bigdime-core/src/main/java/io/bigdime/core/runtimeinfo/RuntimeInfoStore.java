/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.runtimeinfo;

import java.util.List;

/**
 * Adaptor stores the status when the job starts or fails or completes
 * successfully. This information is required for next batch to determine which
 * data set needs to be ingested.
 * 
 * @author Neeraj Jain
 *
 */
public interface RuntimeInfoStore<T extends RuntimeInfo> {

	enum Status {
		/**
		 * 
		 */
		QUEUED,

		/**
		 * Ingestion has started for the given source.
		 */
		STARTED,

		/**
		 * Ingestion was started and partially completed, pending for the next
		 * steps such as staging or validation etc. This is helpful when we dont
		 * want to restart the processing from first handler in case of an
		 * error.
		 */
		PENDING,

		/**
		 * Ingestion has failed for the given source.
		 */
		FAILED,

		/**
		 * Ingestion has completed successfully for the given source.
		 */
		VALIDATED,

		/**
		 * Ingestion was failed and the data from the target(hdfs/hbase etc) has
		 * been cleared.
		 */
		ROLLED_BACK,

		/**
		 * Record not valid anymore
		 */
		INVALID
	};

	public T getById(int runtimeInfoId) throws RuntimeInfoStoreException;

	/**
	 * Gets the collection of RuntimeInfo objects for the jobs for given
	 * adaptor, entity.
	 * 
	 * @param adaptorName
	 *            such as click stream data adaptor
	 * @param entityName
	 *            topic:partition for kakfa, table for rdbms etc
	 * @return Collection<T> collection of RuntimeInfo objects
	 * @throws RuntimeInfoStoreException
	 *             if there was any problem in getting RuntimeInfo from the
	 *             store
	 */
	public List<T> getAll(String adaptorName, String entityName) throws RuntimeInfoStoreException;

	/**
	 * Gets the collection of RuntimeInfo objects for the jobs for given
	 * adaptor, entity and status.
	 * 
	 * @param adaptorName
	 *            such as click stream data adaptor
	 * @param entityName
	 *            topic:partition for kakfa, table for rdbms etc
	 * @param status
	 *            status filter
	 * @return Collection<T> collection of RuntimeInfo objects
	 * @throws RuntimeInfoStoreException
	 *             if there was any problem in getting RuntimeInfo from the
	 *             store
	 */
	public List<T> getAll(String adaptorName, String entityName, Status status) throws RuntimeInfoStoreException;

	/**
	 * Get all the RuntimeInfo records for the given adaptorName, entityName and
	 * inputDescriptorPrefix. If the table doesn't have the index on these
	 * columns, this call could be very slow.
	 *
	 * @param adaptorName
	 * @param entityName
	 * @param inputDescriptorPrefix
	 * @return
	 * @throws RuntimeInfoStoreException
	 */
	public List<T> getAll(String adaptorName, String entityName, String inputDescriptorPrefix)
			throws RuntimeInfoStoreException;

	/**
	 * Gets the RuntimeInfo object for the jobs for given adaptor, entity, and
	 * descriptor.
	 * 
	 * @param adaptorName
	 *            such as click stream data adaptor
	 * @param entityName
	 *            topic:partition for kakfa, table for rdbms etc
	 * @param descriptor
	 *            descriptor defining the input source, like
	 *            basepath/hour/minute/file1.txt
	 * @return T RuntimeInfo object
	 * @throws RuntimeInfoStoreException
	 *             if there was any problem in getting RuntimeInfo from the
	 *             store
	 */
	public T get(String adaptorName, String entityName, String descriptor) throws RuntimeInfoStoreException;

	/**
	 * Gets the latest RuntimeInfo object for the jobs for given adaptor,
	 * entity.
	 * 
	 * @param adaptorName
	 *            such as click stream data adaptor
	 * @param entityName
	 *            topic:partition for kakfa, table for rdbms etc
	 * @return T RuntimeInfo object
	 * @throws RuntimeInfoStoreException
	 *             if there was any problem in getting RuntimeInfo from the
	 *             store
	 */
	public T getLatest(String adaptorName, String entityName) throws RuntimeInfoStoreException;

	/**
	 * Stores the runtime information for the given adaptor and entity.
	 * 
	 * @param adaptorRuntimeInfo
	 *            object representing runtime information.
	 * @return true if entry was put, false otherwise
	 * @throws RuntimeInfoStoreException
	 *             if there was any problem in storing RuntimeInfo
	 */
	public boolean put(T adaptorRuntimeInfo) throws RuntimeInfoStoreException;

	public boolean delete(T adaptorRuntimeInfo) throws RuntimeInfoStoreException;
}
