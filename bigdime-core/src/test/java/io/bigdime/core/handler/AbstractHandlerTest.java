/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.core.ActionEvent;
import io.bigdime.core.DataChannel;
import io.bigdime.core.Handler;
import io.bigdime.core.HandlerException;

public class AbstractHandlerTest {

	@Test
	public void testSetState() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		memoryChannelInputHandler.setState(Handler.State.INIT);
		Assert.assertSame(memoryChannelInputHandler.getState(), Handler.State.INIT);

		memoryChannelInputHandler.setState(Handler.State.INIT);
		Assert.assertSame(memoryChannelInputHandler.getState(), Handler.State.INIT);
	}

	/**
	 * Assert that equals method returns true when compared against the same
	 * object.
	 */
	@Test
	public void testEqualsWithSameObject() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		memoryChannelInputHandler.setName("unit-memory-channel-handler-name");
		Assert.assertTrue(memoryChannelInputHandler.equals(memoryChannelInputHandler));
	}

	/**
	 * Assert that equals method returns true when both objects have the same
	 * name.
	 */
	@Test
	public void testEqualsAndHashCodeWithSameName() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler, "id", "unit-memory-channel-handler-name");

		MemoryChannelInputHandler memoryChannelInputHandler1 = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler1, "id", "unit-memory-channel-handler-name");
		Assert.assertTrue(memoryChannelInputHandler.equals(memoryChannelInputHandler1));
		Assert.assertTrue(memoryChannelInputHandler.hashCode() == memoryChannelInputHandler1.hashCode());
	}

	/**
	 * Assert that equals method returns false when both objects have a
	 * different name.
	 */
	@Test
	public void testEqualsWithDifferentName() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler, "id", "unit-memory-channel-handler-name-1");

		MemoryChannelInputHandler memoryChannelInputHandler1 = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler1, "id", "unit-memory-channel-handler-name");
		Assert.assertFalse(memoryChannelInputHandler.equals(memoryChannelInputHandler1));
	}

	/**
	 * Assert that equals method returns false when compared against null.
	 */
	@Test
	public void testEqualsWithNull() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler, "id", "unit-memory-channel-handler-name-1");
		Assert.assertFalse(memoryChannelInputHandler.equals(null));
	}

	/**
	 * Assert that equals method returns false when compared against an object
	 * of different type.
	 */
	@Test
	public void testEqualsWithDifferentObjectType() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		Assert.assertFalse(memoryChannelInputHandler.equals(new Object()));
	}

	/**
	 * Assert that equals method returns false when the calling object's name is
	 * null while other object's name is not null.
	 */
	@Test
	public void testEqualsWithNameAsNullInThis() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler, "id", null);

		MemoryChannelInputHandler memoryChannelInputHandler1 = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler1, "id", "unit-memory-channel-handler-name-1");
		Assert.assertFalse(memoryChannelInputHandler.equals(memoryChannelInputHandler1));
	}

	/**
	 * Assert that equals method returns true when both object's name is null.
	 */
	@Test
	public void testEqualsHashCodeWithNameAsNullInBothObjects() {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler, "id", null);

		MemoryChannelInputHandler memoryChannelInputHandler1 = new MemoryChannelInputHandler();
		ReflectionTestUtils.setField(memoryChannelInputHandler1, "id", null);
		Assert.assertTrue(memoryChannelInputHandler.equals(memoryChannelInputHandler1));
		Assert.assertTrue(memoryChannelInputHandler.hashCode() == memoryChannelInputHandler1.hashCode());
	}

	// Wrong test, no need for MemoryChannelInputHandler to process channel
	// submission
	// @Test
	// public void testProcessChannelSubmission() {
	// MemoryChannelInputHandler memoryChannelInputHandler = new
	// MemoryChannelInputHandler();
	// DataChannel dataChannel = Mockito.mock(DataChannel.class);
	// memoryChannelInputHandler.setOutputChannel(dataChannel);
	// memoryChannelInputHandler.processChannelSubmission(new ActionEvent());
	// Mockito.verify(dataChannel,
	// Mockito.times(1)).put(Mockito.any(ActionEvent.class));
	// }

	@Test
	public void testGetNonNullJournalGetNewJournal() throws HandlerException {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();

		SimpleJournal journal = memoryChannelInputHandler.getNonNullJournal(SimpleJournal.class);
		Assert.assertNotNull(journal);

	}

	@Test
	public void testGetNonNullJournalGetExistingJournal() throws HandlerException {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		SimpleJournal journal = new SimpleJournal();
		memoryChannelInputHandler.setJournal(journal);
		SimpleJournal journal1 = memoryChannelInputHandler.getNonNullJournal(SimpleJournal.class);
		Assert.assertSame(journal1, journal);

	}

	@Test
	public void testGetJournalGetExistingJournal() throws HandlerException {
		MemoryChannelInputHandler memoryChannelInputHandler = new MemoryChannelInputHandler();
		SimpleJournal journal = new SimpleJournal();
		memoryChannelInputHandler.setJournal(journal);
		SimpleJournal journal1 = memoryChannelInputHandler.getJournal(SimpleJournal.class);
		Assert.assertSame(journal1, journal);

	}

}
