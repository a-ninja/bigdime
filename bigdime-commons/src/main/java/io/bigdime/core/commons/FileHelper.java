/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Component
public final class FileHelper {
	private static FileHelper instance = new FileHelper();

	private FileHelper() {
	}

	public static FileHelper getInstance() {
		return instance;
	}

	/**
	 * Gets the files from directory specified by fileLocation and sub
	 * directories under it. Returns the filenames sorted in natural sorting
	 * order.
	 * 
	 * @param fileLocation
	 * @param fileNamePattern
	 * @return
	 */
	public List<String> getAvailableFiles(String fileLocation, String fileNamePattern) {
		File baseFolder = new File(fileLocation);
		List<String> availableFilesInFolder = new ArrayList<>();
		IOFileFilter fileFilter = new RegexFileFilter(fileNamePattern);
		Collection<File> files = null;
		files = FileUtils.listFiles(baseFolder, fileFilter, DirectoryFileFilter.DIRECTORY);
		for (final File f : files) {
			if (f.isFile()) {
				availableFilesInFolder.add(f.getAbsolutePath());
			}
		}
		Collections.sort(availableFilesInFolder);
		return availableFilesInFolder;
	}

}
