/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang.NotImplementedException;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.InvalidValueConfigurationException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.FileHelper;
import io.bigdime.core.config.AdaptorConfig;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;
import io.bigdime.core.handler.HandlerJournal;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;

public class ZipFileInputStreamHandlerTest {
	private static final AdaptorLogger logger = new AdaptorLogger(
			LoggerFactory.getLogger(ZipFileInputStreamHandlerTest.class));

	/**
	 * If there is status STARTED file in Runtime Info, it need to be processed,
	 * * make sure that process method returns action event with Status.READY
	 * since buffer size is larger than file size.
	 * 
	 * @throws IOException
	 * @throws HandlerException
	 * @throws RuntimeInfoStoreException
	 * @throws AdaptorConfigurationException
	 * 
	 */
	@Test
	public void testProcessWithStartedStatus() throws Throwable {
		String writeData = "unit-test-testProcesstestProcessWithStartedStatus";
		File[] tempFiles = createZipFile(writeData);
		List<RuntimeInfo> runtimeInfoList = new ArrayList<>();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setInputDescriptor(tempFiles[1].getAbsolutePath());
		runtimeInfo.setStatus(RuntimeInfoStore.Status.STARTED);
		runtimeInfoList.add(runtimeInfo);
		ReturnStatus returnStatus = setupzfirhWithStarted(runtimeInfoList, tempFiles[1].getParent());
		Status outputStatuts = returnStatus.getStatus();

		Assert.assertEquals(outputStatuts, Status.READY);
		// HandlerContext handlerContext = HandlerContext.get();
		List<ActionEvent> actionEvents = returnStatus.getEventList();
		Assert.assertNotNull(actionEvents);
		if (tempFiles[0].delete() && tempFiles[1].delete()) {
			logger.info("delete file successfully", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
		} else {
			logger.info("unable to delete file", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
			throw new HandlerException("unable to delete file");
		}
	}

	/**
	 * If there is no new file to be processed, make sure that process method
	 * returns action event with Status.BACKOFF.
	 * 
	 * @throws Throwable
	 * 
	 */
	@Test
	public void testProcessWithNoFileToProcess() throws Throwable {
		String writeData = "unit-test-testProcessWithNoFileToProcess";
		File[] tempFiles = createZipFile(writeData);
		List<RuntimeInfo> runtimeInfoList = new ArrayList<>();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setInputDescriptor(tempFiles[0].getAbsolutePath());
		runtimeInfo.setStatus(RuntimeInfoStore.Status.VALIDATED);
		runtimeInfoList.add(runtimeInfo);
		RuntimeInfo runtimeInfo1 = new RuntimeInfo();
		runtimeInfo1.setInputDescriptor(tempFiles[1].getAbsolutePath());
		runtimeInfo1.setStatus(RuntimeInfoStore.Status.VALIDATED);
		runtimeInfoList.add(runtimeInfo1);
		ReturnStatus returnStatus = setupzfirh(runtimeInfoList, tempFiles[1].getParent());
		Status status = returnStatus.getStatus();
		logger.debug("running testProcessWithNoFileToProcess", "status={}", status);
		Assert.assertTrue((status == Status.BACKOFF));
		if (tempFiles[0].delete() && tempFiles[1].delete()) {
			logger.info("delete file successfully", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
		} else {
			logger.info("unable to delete file", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
			throw new HandlerException("unable to delete file");
		}
	}

	/**
	 * If there is status QUEUED file in Runtime Info, it need to be processed,
	 * make sure that process method returns action event with Status.CALLBACK
	 * since buffer size is smaller than file size.
	 * 
	 * @throws IOException
	 * @throws HandlerException
	 * @throws RuntimeInfoStoreException
	 * @throws AdaptorConfigurationException
	 * 
	 */
	@Test
	public void testProcessWithQueuedStatus() throws Throwable {
		String writeData = "unit-test-testProcessWithQueuedStatus";
		File[] tempFiles = createZipFile(writeData);
		List<RuntimeInfo> runtimeInfoList = new ArrayList<>();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setInputDescriptor(tempFiles[1].getAbsolutePath());
		runtimeInfo.setStatus(RuntimeInfoStore.Status.QUEUED);
		runtimeInfoList.add(runtimeInfo);

		ReturnStatus returnStatus = setupzfirhWithQueued(runtimeInfoList, tempFiles[1].getParent());
		Status outputStatus = returnStatus.getStatus();
		Assert.assertEquals(outputStatus, Status.CALLBACK);
		if (tempFiles[0].delete() && tempFiles[1].delete()) {
			logger.info("delete file successfully", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
		} else {
			logger.info("unable to delete file", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
			throw new HandlerException("unable to delete file");
		}
	}

	/**
	 * If there is status STARTED file in Runtime Info, it need to be processed,
	 * make sure that process method returns action event with Status.CALLBACK
	 * since zip file has more than one file in and buffer size is larger than
	 * file size,
	 * 
	 * @throws IOException
	 * @throws HandlerException
	 * @throws RuntimeInfoStoreException
	 * @throws AdaptorConfigurationException
	 * 
	 */
	@Test
	public void testProcessWithMultipleFileInZip() throws Throwable {
		String writeData = "unit-test-testProcessWithMultipleFileInZip";
		File[] tempFiles = createZipFileWithMultipleFiles(writeData);
		List<RuntimeInfo> runtimeInfoList = new ArrayList<>();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setInputDescriptor(tempFiles[0].getAbsolutePath());
		runtimeInfo.setStatus(RuntimeInfoStore.Status.STARTED);
		runtimeInfoList.add(runtimeInfo);
		ReturnStatus returnStatus = setupzfirhWithStarted(runtimeInfoList, tempFiles[0].getParent());
		Status outputStatuts = returnStatus.getStatus();
		Assert.assertEquals(outputStatuts, Status.CALLBACK);
		List<ActionEvent> actionEvents = returnStatus.getEventList();
		Assert.assertNotNull(actionEvents);
		if (tempFiles[0].delete()) {
			logger.info("delete file successfully", "tempFiles[0]={}", tempFiles[0].getName());
		} else {
			logger.info("unable to delete file", "tempFiles[0]={}", tempFiles[0].getName());
			throw new HandlerException("unable to delete file");
		}
	}

	/**
	 * If there is status STARTED file in Runtime Info, it need to be processed,
	 * make sure that process method returns action event with Status.BACKOFF
	 * since file size is zero.
	 * 
	 * @throws IOException
	 * @throws HandlerException
	 * @throws RuntimeInfoStoreException
	 * @throws AdaptorConfigurationException
	 * 
	 */
	@Test
	public void testProcessWithEmptyFileInZip() throws Throwable {
		String writeData = "";
		File[] tempFiles = createZipFile(writeData);
		List<RuntimeInfo> runtimeInfoList = new ArrayList<>();
		RuntimeInfo runtimeInfo = new RuntimeInfo();
		runtimeInfo.setInputDescriptor(tempFiles[0].getAbsolutePath());
		runtimeInfo.setStatus(RuntimeInfoStore.Status.QUEUED);
		runtimeInfoList.add(runtimeInfo);
		ReturnStatus returnStatus = setupzfirhWithQueued(runtimeInfoList, tempFiles[0].getParent());

		Status outputStatuts = returnStatus.getStatus();
		Assert.assertEquals(outputStatuts, Status.BACKOFF);
		List<ActionEvent> actionEvents = returnStatus.getEventList();
		Assert.assertNotNull(actionEvents);
		if (tempFiles[0].delete() && tempFiles[1].delete()) {
			logger.info("delete file successfully", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
		} else {
			logger.info("unable to delete file", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
					tempFiles[1].getName());
			throw new HandlerException("unable to delete file");
		}
	}

	/**
	 * If there is no byte to read, make sure that process method returns action
	 * event with Status.READY.
	 * 
	 * @throws IOException
	 * @throws HandlerException
	 * 
	 */
	@Test
	public void testProcessWithNoByteToRead() throws IOException, HandlerException {
		ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
		HandlerContext handlerContext = HandlerContext.get();
		HandlerJournal fhj = new ZipFileHandlerJournal();
		fhj.setTotalRead(1);
		fhj.setTotalSize(2);
		handlerContext.setJournal(zfirh.getId(), fhj);

		zfirh.incrementInvocationCount();
		ZipEntry mockZipEntry = Mockito.mock(ZipEntry.class);
		ReflectionTestUtils.setField(zfirh, "zipFileEntry", mockZipEntry);
		BufferedInputStream bufferedInputStream = Mockito.mock(BufferedInputStream.class);
		ReflectionTestUtils.setField(zfirh, "bufferedInputStream", bufferedInputStream);
		Mockito.when(bufferedInputStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
				.thenReturn(-1);
		Assert.assertEquals(zfirh.process(), Status.READY);
	}

	/**
	 * Make sure that IOException is handled and then thrown as
	 * HandlerException. This test is basically to cover all the branches.
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = IOException.class)
	public void testProcessWithIOException() throws Throwable {
		try {
			ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
			HandlerContext handlerContext = HandlerContext.get();
			HandlerJournal fhj = new ZipFileHandlerJournal();
			fhj.setTotalRead(1);
			fhj.setTotalSize(2);
			handlerContext.setJournal(zfirh.getId(), fhj);
			zfirh.incrementInvocationCount();
			ZipEntry mockZipEntry = Mockito.mock(ZipEntry.class);
			ReflectionTestUtils.setField(zfirh, "zipFileEntry", mockZipEntry);
			BufferedInputStream bufferedInputStream = Mockito.mock(BufferedInputStream.class);
			ReflectionTestUtils.setField(zfirh, "bufferedInputStream", bufferedInputStream);
			Mockito.when(bufferedInputStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
					.thenThrow(new IOException(""));
			zfirh.process();
			Assert.fail("should have thrown IOException");
		} catch (HandlerException e) {
			throw e.getCause();
		}
	}

	/**
	 * Make sure that RuntimeInfoStoreException is handled and then thrown as
	 * HandlerException. This test is basically to cover all the branches.
	 * 
	 * @throws Throwable
	 */
	@Test(expectedExceptions = RuntimeInfoStoreException.class)
	public void testProcessWithRuntimeInfoStoreException() throws Throwable {
		try {
			ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
			HandlerContext handlerContext = HandlerContext.get();

			String writeData = "unit-test-testProcess";
			File[] tempFiles = createZipFile(writeData);
			HandlerJournal fhj = new ZipFileHandlerJournal();
			fhj.setTotalRead(1);
			fhj.setTotalSize(1);
			handlerContext.setJournal(zfirh.getId(), fhj);

			@SuppressWarnings("unchecked")
			RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
			Mockito.when(runtimeInfoStore.getAll(Mockito.anyString(), Mockito.anyString(),
					Mockito.any(RuntimeInfoStore.Status.class))).thenThrow(new RuntimeInfoStoreException(""));
			ReflectionTestUtils.setField(zfirh, "entityName", "entityName");
			ReflectionTestUtils.setField(zfirh, "runtimeInfoStore", runtimeInfoStore);
			ReflectionTestUtils.setField(zfirh, "fileHelper", FileHelper.getInstance());
			ReflectionTestUtils.setField(zfirh, "fileLocation", tempFiles[0].getParent() + "/");
			ReflectionTestUtils.setField(zfirh, "fileNamePattern", "test.*");

			zfirh.process();
			Assert.fail("should have thrown RuntimeInfoStoreException");
			if (tempFiles[0].delete() && tempFiles[1].delete()) {
				logger.info("delete file successfully", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
						tempFiles[1].getName());
			} else {
				logger.info("unable to delete file", "tempFiles[0]={}, tempFiles[1]={}", tempFiles[0].getName(),
						tempFiles[1].getName());
				throw new HandlerException("unable to delete file");
			}

		} catch (HandlerException e) {
			throw e.getCause();
		}
	}

	@Test
	public void testBuild() throws InterruptedException {
		try {
			Map<String, Object> propertyMap = new HashMap<>();

			File temp = File.createTempFile("temp-file-name", ".zip");
			temp.deleteOnExit();
			propertyMap.put("fileLocation", temp.getParent());

			Map<String, String> srcDEscEntryMap = new HashMap<>();
			srcDEscEntryMap.put("build_entity:temp-*", "input1");
			propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());

			final ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
			zfirh.setPropertyMap(propertyMap);
			zfirh.build();
			zfirh.build();// 2nd build call to cover file=null branch
			if (temp.delete()) {
				logger.info("delete file successfully", "temp={}" + temp.getName());
			} else {
				logger.info("unable to delete file", "temp={}" + temp.getName());
				throw new Exception();
			}
			Assert.assertEquals(zfirh.getEntityName(), "build_entity");
			Assert.assertEquals(zfirh.getFileNamePattern(), "temp-*");
			Assert.assertEquals(zfirh.getBasePath(), "/");

		} catch (AdaptorConfigurationException e) {
			Assert.fail(e.toString());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	@Test(expectedExceptions = InvalidValueConfigurationException.class, expectedExceptionsMessageRegExp = "src-desc can't be null.*")
	public void testBuildWithNoSrcDesc() throws AdaptorConfigurationException {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put("fileLocation", "some-invalid-path");

		Map<String, String> srcDEscEntryMap = new HashMap<>();
		srcDEscEntryMap.put("some-invalid-path", "input1");

		final ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
		zfirh.setPropertyMap(propertyMap);
		zfirh.build();
		Assert.fail("build should have failed with InvalidValueConfigurationException");
	}

	@Test(expectedExceptions = NotImplementedException.class)
	public void testShutdown() {
		final ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
		zfirh.shutdown();

	}

	private File[] createZipFile(String writeData) throws IOException {
		File[] tempFiles = new File[2];
		for (int i = 0; i < 2; i++) {
			File f = new File("src/test/resources/test-" + i + ".zip");
			ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
			tempFiles[i] = f;
			ZipEntry e = new ZipEntry("test-" + i + ".txt");
			out.putNextEntry(e);
			byte[] data = writeData.getBytes(Charset.forName("UTF-8"));
			out.write(data, 0, data.length);
			out.closeEntry();
			out.close();
		}
		return tempFiles;
	}

	private File[] createZipFileWithMultipleFiles(String writeData) throws IOException {
		File[] tempFiles = new File[1];
		File f = new File("src/test/resources/test.zip");
		ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
		tempFiles[0] = f;
		for (int i = 0; i < 2; i++) {
			ZipEntry e = new ZipEntry("test-" + i + ".txt");
			out.putNextEntry(e);
			byte[] data = writeData.getBytes(Charset.forName("UTF-8"));
			out.write(data, 0, data.length);
		}
		out.closeEntry();
		out.close();
		return tempFiles;
	}

	private ReturnStatus setupzfirh(final List<RuntimeInfo> runtimeInfoList, final String fileLocation)
			throws Throwable {
		FutureTask<ReturnStatus> futureTask = new FutureTask<>(new Callable<ReturnStatus>() {
			@Override
			public ReturnStatus call() throws Exception {

				ReturnStatus returnStatus = new ReturnStatus();

				ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
				HandlerContext context = HandlerContext.get();
				HandlerJournal fhj = new ZipFileHandlerJournal();
				fhj.setTotalRead(0);
				context.setJournal(zfirh.getId(), fhj);
				Map<String, Object> propertyMap = new HashMap<>();
				int bufferSize = 10;

				propertyMap.put(FileInputStreamReaderHandlerConstants.FILE_NAME_PATTERN, "test.*");
				propertyMap.put(FileInputStreamReaderHandlerConstants.BUFFER_SIZE, bufferSize);

				Map<String, String> srcDEscEntryMap = new HashMap<>();
				srcDEscEntryMap.put("entity:test.*", "input1");
				propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());
				propertyMap.put(FileInputStreamReaderHandlerConstants.BASE_PATH, fileLocation);

				zfirh.setPropertyMap(propertyMap);
				zfirh.build();
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), "entity"))
						.thenReturn(runtimeInfoList);
				ReflectionTestUtils.setField(zfirh, "entityName", "entity");
				ReflectionTestUtils.setField(zfirh, "dirtyRecords", runtimeInfoList);
				ReflectionTestUtils.setField(zfirh, "runtimeInfoStore", runtimeInfoStore);
				ReflectionTestUtils.setField(zfirh, "fileInputDescriptor", new FileInputDescriptor());
				ReflectionTestUtils.setField(zfirh, "fileHelper", FileHelper.getInstance());
				Mockito.when(runtimeInfoStore.put(Mockito.any(RuntimeInfo.class))).thenReturn(true);
				Status status = zfirh.process();
				returnStatus.setStatus(status);
				return returnStatus;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			return futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}

	}

	private ReturnStatus setupzfirhWithStarted(final List<RuntimeInfo> runtimeInfoList, final String fileLocation)
			throws Throwable {

		FutureTask<ReturnStatus> futureTask = new FutureTask<>(new Callable<ReturnStatus>() {
			@Override
			public ReturnStatus call() throws Exception {

				ReturnStatus returnStatus = new ReturnStatus();

				ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
				HandlerContext context = HandlerContext.get();
				HandlerJournal fhj = new ZipFileHandlerJournal();
				fhj.setTotalRead(0);
				context.setJournal(zfirh.getId(), fhj);
				Map<String, Object> propertyMap = new HashMap<>();
				int bufferSize = 50;

				propertyMap.put(FileInputStreamReaderHandlerConstants.FILE_NAME_PATTERN, "test.*");
				propertyMap.put(FileInputStreamReaderHandlerConstants.BUFFER_SIZE, bufferSize);

				Map<String, String> srcDEscEntryMap = new HashMap<>();
				srcDEscEntryMap.put("entity:test.*", "input1");
				propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());
				propertyMap.put(FileInputStreamReaderHandlerConstants.BASE_PATH, fileLocation);

				zfirh.setPropertyMap(propertyMap);
				zfirh.build();
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), "entity",
						RuntimeInfoStore.Status.STARTED)).thenReturn(runtimeInfoList);
				ReflectionTestUtils.setField(zfirh, "dirtyRecords", runtimeInfoList);
				ReflectionTestUtils.setField(zfirh, "runtimeInfoStore", runtimeInfoStore);
				ReflectionTestUtils.setField(zfirh, "fileInputDescriptor", new FileInputDescriptor());
				ReflectionTestUtils.setField(zfirh, "fileHelper", FileHelper.getInstance());
				Mockito.when(runtimeInfoStore.put(Mockito.any(RuntimeInfo.class))).thenReturn(true);
				Status status = zfirh.process();
				returnStatus.setStatus(status);
				return returnStatus;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			return futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}

	}

	private ReturnStatus setupzfirhWithQueued(final List<RuntimeInfo> runtimeInfoList, final String fileLocation)
			throws Throwable {
		FutureTask<ReturnStatus> futureTask = new FutureTask<>(new Callable<ReturnStatus>() {
			@Override
			public ReturnStatus call() throws Exception {

				ReturnStatus returnStatus = new ReturnStatus();

				ZipFileInputStreamHandler zfirh = new ZipFileInputStreamHandler();
				HandlerContext context = HandlerContext.get();
				HandlerJournal fhj = new ZipFileHandlerJournal();
				fhj.setTotalRead(0);
				context.setJournal(zfirh.getId(), fhj);
				Map<String, Object> propertyMap = new HashMap<>();
				int bufferSize = 10;

				propertyMap.put(FileInputStreamReaderHandlerConstants.FILE_NAME_PATTERN, "test.*");
				propertyMap.put(FileInputStreamReaderHandlerConstants.BUFFER_SIZE, bufferSize);

				Map<String, String> srcDEscEntryMap = new HashMap<>();
				srcDEscEntryMap.put("entity:test.*", "input1");
				propertyMap.put(SourceConfigConstants.SRC_DESC, srcDEscEntryMap.entrySet().iterator().next());
				propertyMap.put(FileInputStreamReaderHandlerConstants.BASE_PATH, fileLocation);
				propertyMap.put(ActionEventHeaderConstants.LAST_HANDLER_IN_CHAIN, "false");

				zfirh.setPropertyMap(propertyMap);
				zfirh.build();
				@SuppressWarnings("unchecked")
				RuntimeInfoStore<RuntimeInfo> runtimeInfoStore = Mockito.mock(RuntimeInfoStore.class);
				Mockito.when(runtimeInfoStore.getAll(AdaptorConfig.getInstance().getName(), "entity",
						RuntimeInfoStore.Status.QUEUED)).thenReturn(runtimeInfoList);
				ReflectionTestUtils.setField(zfirh, "dirtyRecords", null);
				ReflectionTestUtils.setField(zfirh, "runtimeInfoStore", runtimeInfoStore);
				ReflectionTestUtils.setField(zfirh, "fileInputDescriptor", new FileInputDescriptor());
				ReflectionTestUtils.setField(zfirh, "fileHelper", FileHelper.getInstance());
				Mockito.when(runtimeInfoStore.put(Mockito.any(RuntimeInfo.class))).thenReturn(true);
				Status status = zfirh.process();
				returnStatus.setStatus(status);
				return returnStatus;
			}
		});

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		executorService.execute(futureTask);
		try {
			return futureTask.get();
		} catch (ExecutionException ex) {
			throw ex.getCause();
		}
	}

}

class ReturnStatus {
	private Status status;
	private List<ActionEvent> eventList = new ArrayList<>();

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public List<ActionEvent> getEventList() {
		return eventList;
	}

	public void setEventList(List<ActionEvent> eventList) {
		this.eventList = eventList;
	}

}
