package io.bigdime.alert.impl.swift;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.javaswift.joss.headers.object.ObjectManifest;
import org.javaswift.joss.instructions.UploadInstructions;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Sample filename
 * 
 * bigdime-data-adaptor.2016-01-01.log
 * 
 * Sample segmentNames:
 * 
 * <br>
 * bigdime-data-adaptor.2016-01-01-stamp1
 * 
 * bigdime-data-adaptor.2016-01-01-stamp2
 * 
 * bigdime-data-adaptor.2016-01-01-stamp3
 * 
 * prefixWithoutDash = bigdime-edw-adaptor.2016-01-01
 * 
 * prefixWithDash = prefixWithoutDash + "-"
 * 
 * segmentName = segmentPrefix+ timestamp
 * 
 * objectName = objectPrefix + ".log"
 * 
 * @param source
 * @return
 */
public class SwiftLogTask implements Callable<Object> {
	private final Container container;
	private final String sourceName;
	private final byte[] message;
	final DateTimeFormatter fileNameDtf = DateTimeFormat.forPattern("yyyy-MM-dd");

	public SwiftLogTask(Container _container, String _sourceName, byte[] _message) {
		container = _container;
		sourceName = _sourceName;
		message = _message;
	}

	@Override
	public Object call() throws Exception {
		long timestamp = System.currentTimeMillis();
		writeSegment(timestamp);
		writeManifest(timestamp);
		return null;
	}

	/**
	 * 
	 * @param segmentPrefix
	 * @param timestamp
	 */
	private void writeSegment(long timestamp) {
		String segmentName = getSegmentName(timestamp);
		StoredObject object = container.getObject(segmentName);
		// object.setDeleteAfter(TimeUnit.DAYS.toSeconds(14));
		object.uploadObject(message);
	}

	private void writeManifest(long timestamp) {
		String largeObjectName = getObjectName(timestamp);
		StoredObject object = container.getObject(largeObjectName);
		// object.setDeleteAfter(TimeUnit.DAYS.toSeconds(14));
		UploadInstructions uploadInstructions = new UploadInstructions(new byte[] {});
		uploadInstructions
				.setObjectManifest(new ObjectManifest(container.getName() + "/" + getPrefixWithDash(timestamp)));
		object.uploadObject(uploadInstructions);
	}

	private String getPrefixWithoutDash(long timestamp) {
		String prefix = sourceName + "." + fileNameDtf.print(timestamp);
		return prefix;
	}

	private String getPrefixWithDash(long timestamp) {
		return getPrefixWithoutDash(timestamp) + "-";
	}

	private String getSegmentName(long timestamp) {
		return getPrefixWithDash(timestamp) + timestamp;
	}

	private String getObjectName(long timestamp) {
		return "alerts." + sourceName + ".log" + fileNameDtf.print(timestamp);
		// return getPrefixWithoutDash(timestamp) + ".log";
	}

}
