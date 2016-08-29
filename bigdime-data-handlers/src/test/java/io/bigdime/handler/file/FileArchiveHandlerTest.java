/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.commons.io.FileUtils;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import io.bigdime.core.constants.ActionEventHeaderConstants;
import io.bigdime.core.handler.HandlerContext;

public class FileArchiveHandlerTest {

	@Test
	public void testBuild() throws AdaptorConfigurationException {
		Map<String, Object> propertyMap = new HashMap<>();

		Map<String, String> srcDescEntryMap = new HashMap<>();
		srcDescEntryMap.put("build_entity:temp-*", "input1");
		propertyMap.put(SourceConfigConstants.SRC_DESC, srcDescEntryMap.entrySet().iterator().next());

		FileArchiveHandler fileArchiveHandler = new FileArchiveHandler();
		fileArchiveHandler.setPropertyMap(propertyMap);
		fileArchiveHandler.build();
	}

	@Test
	public void testProcessWithReadCompleteAsNull() throws Throwable {
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, null);
		Status status = invoke(actionEvent);
		Assert.assertEquals(status, Status.READY);
	}

	@Test
	public void testProcessWithReadCompleteAsFalse() throws Throwable {
		ActionEvent actionEvent = new ActionEvent();
		actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "false");
		Status status = invoke(actionEvent);
		Assert.assertEquals(status, Status.READY);
	}

	/**
	 * If the source file exists, it should be moved to the dest path. The
	 * destination path should be created and the file must exist in the
	 * destination path, as a file.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testProcessWithReadCompleteAsTrue() throws Throwable {
		ActionEvent actionEvent = new ActionEvent();
		String tempDir = System.getProperty("java.io.tmpdir");
		File sourceFilePath = new File(tempDir, "tempsrc");
		sourceFilePath.mkdirs();
		File destPath = new File(tempDir, "tempdest");
		File sourceFile = File.createTempFile("temp-file-name-" + 0, ".tmp", sourceFilePath);

		actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, sourceFile.getAbsolutePath());
		// actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_LOCATION,
		// sourceFilePath.getAbsolutePath());
		FileArchiveHandler fileArchiveHandler = new FileArchiveHandler();
		ReflectionTestUtils.setField(fileArchiveHandler, "archivePath", destPath.getAbsolutePath());
		Assert.assertTrue(sourceFile.exists());
		Status status = invoke(actionEvent, fileArchiveHandler);
		Assert.assertEquals(status, Status.READY);
		Assert.assertFalse(new File(sourceFile.getAbsolutePath()).exists());
		Assert.assertTrue(new File(destPath.getAbsolutePath() + sourceFile.getAbsolutePath()).exists());
		Assert.assertFalse(new File(destPath.getAbsolutePath() + sourceFile.getAbsolutePath()).isDirectory());
		FileUtils.deleteDirectory(sourceFilePath);
		FileUtils.deleteDirectory(destPath);
	}

	/**
	 * If the source file doesn't exist, archive operation should fail with
	 * IOException. Status should be returned as READY. Make sure that the
	 * destination path exists and exists as a directory.
	 * 
	 * @throws Throwable
	 */
	@Test
	public void testProcessWithSourceFileDoesntExist() throws Throwable {
		ActionEvent actionEvent = new ActionEvent();
		String tempDir = System.getProperty("java.io.tmpdir");
		File destPath = new File(tempDir, "tempdest");
		File sourceFile = new File(tempDir, "/tempsrc/tempfile.txt");

		actionEvent.getHeaders().put(ActionEventHeaderConstants.READ_COMPLETE, "true");
		actionEvent.getHeaders().put(ActionEventHeaderConstants.SOURCE_FILE_PATH, sourceFile.getAbsolutePath());
		FileArchiveHandler fileArchiveHandler = new FileArchiveHandler();
		ReflectionTestUtils.setField(fileArchiveHandler, "archivePath", destPath.getAbsolutePath());
		Assert.assertFalse(sourceFile.exists());
		Status status = invoke(actionEvent, fileArchiveHandler);
		Assert.assertEquals(status, Status.READY);
		Assert.assertFalse(new File(sourceFile.getAbsolutePath()).exists());

		Assert.assertTrue(new File(destPath.getAbsolutePath() + sourceFile.getAbsolutePath()).exists());
		Assert.assertTrue(new File(destPath.getAbsolutePath() + sourceFile.getAbsolutePath()).isDirectory());
		FileUtils.deleteDirectory(destPath);
	}

	public Status invoke(final ActionEvent actionEvent) throws Throwable {
		return invoke(actionEvent, new FileArchiveHandler());
	}

	public Status invoke(final ActionEvent actionEvent, final FileArchiveHandler fileArchiveHandler) throws Throwable {
		FutureTask<Status> futureTask = new FutureTask<>(new Callable<Status>() {
			@Override
			public Status call() throws Exception {
				// FileArchiveHandler fileArchiveHandler = new
				// FileArchiveHandler();
				HandlerContext context = HandlerContext.get();
				context.createSingleItemEventList(actionEvent);
				return fileArchiveHandler.process();
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
