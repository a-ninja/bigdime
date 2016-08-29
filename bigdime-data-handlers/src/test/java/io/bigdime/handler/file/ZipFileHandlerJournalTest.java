/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.file;

import java.util.Random;

import io.bigdime.common.testutils.GetterSetterTestHelper;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ZipFileHandlerJournalTest {
	@Test
	public void testGetterSetter() {
		ZipFileHandlerJournal journal = new ZipFileHandlerJournal();
		GetterSetterTestHelper.doTest(journal, "zipFileName", "test.zip");
	}
	
	@Test
	public void testReset() {
		Random rand = new Random();
		ZipFileHandlerJournal journal = new ZipFileHandlerJournal();
		journal.setTotalRead(rand.nextLong());
		journal.setEventList(null);
		journal.setTotalSize(rand.nextInt());
		journal.setTotalEntries(rand.nextInt());
		journal.setReadEntries(rand.nextInt());
		journal.setEntryName(null);
		journal.setSrcFileName(null);
		journal.setZipFileName(null);
		journal.reset();
		Assert.assertEquals(journal.getTotalRead(), 0);
		Assert.assertNull(journal.getEventList());
		Assert.assertEquals(journal.getTotalSize(), 0);
		Assert.assertEquals(journal.getTotalEntries(), 0);
		Assert.assertEquals(journal.getReadEntries(), 0);
		Assert.assertEquals(journal.getEntryName(), null);
		Assert.assertEquals(journal.getSrcFileName(), null);
		Assert.assertEquals(journal.getZipFileName(), null);	
	}
	
}