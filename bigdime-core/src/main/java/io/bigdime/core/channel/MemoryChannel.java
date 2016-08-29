/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.channel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.commons.PropertyHelper;
import io.bigdime.core.config.AdaptorConfigConstants.ChannelConfigConstants;

/**
 * Memory based DataChannel stores data in a collection backed by an ArrayList.
 * This implementation supports replicating channel feature by maintaining a
 * consumed-event-index for each consumer. Once an event at an index has been
 * consumed by all the consumers, the event is freed from the list and space is
 * made available.
 * 
 *
 * @author Neeraj Jain TODO define data structure MultiplexingQueue
 *         MultipleConsumerQueue
 * 
 */

@Component
@Scope("prototype")
public class MemoryChannel extends AbstractChannel {
	private String channelId;
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(MemoryChannel.class));
	private Set<String> consumerNames = new HashSet<>();
	/*
	 * DataChannel maintains the data in an array list, need random access to
	 * the elements.
	 */
	private List<Event> eventList = Collections.synchronizedList(new ArrayList<Event>());
	// private List<Event> eventList = new ArrayList<Event>();
	/*
	 * Maintains a mapping between the consumer and the last index it fetched.
	 * The take request will return the data from the next index.
	 */
	private Map<String, Integer> consumerToTakenIndexMap = new HashMap<>();
	/*
	 * Maintains a mapping between the index in the eventList and how many times
	 * it has been consumed. Once the element has been consumed by all the
	 * consumers, the take method should remove it from the list.
	 */
	private Map<Integer, Integer> indexToTakenCountMap = new HashMap<>();

	/*
	 * Number of elements that have been removed from the eventList. Need this
	 * to compute the next index.
	 */
	private int removedCount;

	// private String counterNameTake;
	// private String counterNamePut;

	private boolean printStats;
	private long printStatsDurationInSeconds;
	private long putCount;
	private long takeCount;
	private long channelSizeInBytes = 0l;
	private long maxSizeInBytes = 0l;
	private boolean channelStopped = false;
	private long channelCapacity;

	@Override
	public void build() {
		channelId = UUID.randomUUID().toString();
		printStats = PropertyHelper.getBooleanProperty(getProperties(), ChannelConfigConstants.PRINT_STATS);
		channelCapacity = PropertyHelper.getLongProperty(getProperties(), ChannelConfigConstants.CHANNEL_CAPACITY,
				MemoryChannelConstants.DEFAULT_CHANNEL_CAPACITY);

		printStatsDurationInSeconds = PropertyHelper.getLongProperty(getProperties(),
				ChannelConfigConstants.PRINT_STATS_DURATION_IN_SECONDS,
				MemoryChannelConstants.DEFAULT_PRINT_STATS_DURATION_IN_SECONDS);
		logger.debug("building memory channel-notsync",
				"channel_name=\"{}\" channel_id=\"{}\" printStats=\"{}\" printStatsDurationInSeconds=\"{}\" channelCapacity=\"{}\"",
				getName(), channelId, printStats, printStatsDurationInSeconds, channelCapacity);
		printStatsDurationInSeconds = printStatsDurationInSeconds * 1000;
	}

	/**
	 * Add a new item to the storage, after ensuring that it can hold more.
	 * Ensure that the storage has enough capacity to hold more items.
	 */
	@Override
	public void put(Event arg0) {
		/*
		 * Put the data in the eventList and notifyAll.
		 *
		 */

		logger.debug("putting event on memory channel", "channel_name=\"{}\" channelSizeInBytes=\"{}\"", getName(),
				channelSizeInBytes);
		synchronized (this) {
			while ((channelSizeInBytes + arg0.getBody().length) > channelCapacity) {
				try {
					logger.debug("waitingto put event on memory channel", "channel_name=\"{}\" channelCapacity=\"{}\"",
							getName(), channelCapacity);
					wait(1000);
				} catch (InterruptedException e) {
					// nothing to do here
				}
			}
			channelSizeInBytes = channelSizeInBytes + arg0.getBody().length;
			if (channelSizeInBytes >= maxSizeInBytes) {
				maxSizeInBytes = channelSizeInBytes;
			}
			eventList.add(arg0);
			notifyAll();
		}
		logger.debug("put event on memory channel", "channel_name=\"{}\" channelSizeInBytes=\"{}\"", getName(),
				channelSizeInBytes);
		putCount++;
	}

	/**
	 * This method, if used with default bigdime implementation, will throw an
	 * UnsupportedOperationException since bigdime expects the name of the
	 * consumer in the request. Consider using
	 * {@link MemoryChannel#take(String)} method instead.
	 * 
	 * @throws ChannelException
	 *             if the there is no data available on Channel
	 */
	@Override
	public synchronized Event take() {
		if ((consumerNames == null) || consumerNames.isEmpty()) {
			return take("default");
		} else {
			throw new UnsupportedOperationException(
					"this channel has registered consumers, invoking take method without parameters is not supported.");
		}
	}

	/**
	 * This method, if used with default bigdime implementation, will throw an
	 * UnsupportedOperationException since bigdime expects the name of the
	 * consumer in the request. Consider using {@link MemoryChannel#take(String,
	 * int))} method instead.
	 * 
	 */
	@Override
	public synchronized List<Event> take(int size) {
		if ((consumerNames == null) || consumerNames.isEmpty()) {
			return take("default", size);
		} else {
			throw new UnsupportedOperationException(
					"this channel has registered consumers, invoking take method without parameters is not supported.");
		}
	}

	@Override
	public synchronized boolean registerConsumer(String consumerName) {
		logger.info("registering consumers", "_message=\"before registering\" channel_name=\"{}\" consumers=\"{}\"",
				getName(), consumerNames);
		boolean registered = consumerNames.add(consumerName);
		logger.info("registering consumers", "_message=\"after registering\" channel_name=\"{}\" consumers=\"{}\"",
				getName(), consumerNames);
		return registered;
	}
	/*
	 * @formatter:off
	 * 1. Who is consuming? The channel needs to maintain a set of consumerNames.
	 * 2. For each consumer, maintain the takenIndex. consumer->takenIndex map.
	 * 3. For each index, maintain the fetch count. index->takenCount map.
	 * 4. For any take request, get the takenIndex for the consumer.
	 * if takenIndex is null, event with index=0 should be returned.
	 * if the takenIndex is=0, event with index=1 should be returned.
	 * if the takenIndex is=n, event with index=n+1 should be returned.
	 * 5. For each take request, update the indexToTakenCountMap map.
	 * if the takenCount for any index is equals to channelConsumerCount, remove the value from the eventList.
	 * when the value is removed from the eventList, set/increment removedCount.
	 * c1 and c2 on a eventList with 0 1 2 3 4 5
	 * consumerToTakenIndexMap = {} indexToTakenCountMap = {}
	 *
	 * c1.take()
	 * takenIndex=null=>0;
	 * removedCount = 0;
	 * newTakenIndex=0; get i0
	 * consumerToTakenIndexMap = {c1,0}
	 * indexToTakenCountMap = {0,1}
	 * removedCount = 0;
	 * data = i0 i1 i2 i3 i4 i5
	 *
	 * c1.take()
	 * takenIndex=0=>1;
	 * removedCount = 0;
	 * newTakenIndex=1; get i1
	 * consumerToTakenIndexMap = {c1,1}
	 * indexToTakenCountMap = {{0,1}, {1,1}}
	 * data = i0 i1 i2 i3 i4 i5
	 *
	 * c2.take()
	 * takenIndex=null=>0;
	 * removedCount = 0;
	 * newTakenIndex=0; get i0
	 * consumerToTakenIndexMap = {{c1,1}, {c2,0}}
	 * indexToTakenCountMap = {{0,2}, {1,1}} => {{1,1}}
	 * removedCount = 1;
	 * data = i1 i2 i3 i4 i5
	 *
	 * c1.take()
	 * takenIndex=1=>2;
	 * removedCount = 1;
	 * newTakenIndex=1; get i2
	 * consumerToTakenIndexMap = {{c1,2}, {c2,0}}
	 * indexToTakenCountMap = {{1,1}, {2,1}}
	 * removedCount = 1;
	 * data = i1 i2 i3 i4 i5
	 *
	 * c2.take()
	 * takenIndex=0=>1;
	 * removedCount = 1;
	 * newTakenIndex=0; get i1
	 * consumerToTakenIndexMap = {{c1,2}, {c2,1}}
	 * indexToTakenCountMap = {{1,2}, {2,1}} => {{2,1}}
	 * removedCount = 2;
	 * data = i2 i3 i4 i5
	 *
	 * c1.take()
	 * takenIndex=2=>3;
	 * removedCount = 2;
	 * newTakenIndex=1; get i3
	 * consumerToTakenIndexMap = {{c1,3}, {c2,0}}
	 * indexToTakenCountMap = {{2,1}, {3,1}}
	 * removedCount = 2;
	 * data = i2 i3 i4 i5
	 * @formatter:on
	 */

	@Override
	public synchronized Event take(String consumerName) {
		List<Event> events = take(consumerName, 1);
		return events.get(0);
	}

	@Override
	public synchronized List<Event> take(final String consumerName, final int size) {
		// start: update the takenIndex for this consumer
		Integer takenIndex = consumerToTakenIndexMap.get(consumerName);
		if (takenIndex == null) {
			takenIndex = 0;
		} else {
			takenIndex++;
		}
		int newTakenStartIndex = takenIndex - removedCount;
		int availableEventsCount = eventList.size() - newTakenStartIndex;
		if (availableEventsCount == 0) {
			throw new ChannelException("No data found on channel");
		}
		int fetchSize = size;
		if (availableEventsCount < size) {
			fetchSize = availableEventsCount;
		}
		int newTakenEndIndex = newTakenStartIndex + fetchSize;
		List<Event> takenEvent = new ArrayList<>(eventList.subList(newTakenStartIndex, newTakenEndIndex));
		consumerToTakenIndexMap.put(consumerName, (takenIndex + fetchSize) - 1);

		// start:update the taken count for this index
		int toBeRemovedCount = 0;
		for (int i = 0; i < fetchSize; i++) {
			Integer takenCount = indexToTakenCountMap.get(takenIndex + i);
			if (takenCount == null) {
				takenCount = 0;
			}
			takenCount++;
			if (takenCount == consumerNames.size()) { // newTakenIndex must
														// always be zero
				Event removedEvent = eventList.get(i);
				toBeRemovedCount++;
				channelSizeInBytes = channelSizeInBytes - removedEvent.getBody().length;
				indexToTakenCountMap.remove(takenIndex + i);
				removedCount++;
			} else {
				indexToTakenCountMap.put(takenIndex + i, takenCount);
			}
		}
		eventList.subList(0, toBeRemovedCount).clear();

		takeCount++;
		notifyAll();
		return takenEvent;
	}

	public long getPutCount() {
		return putCount;
	}

	public long getTakeCount() {
		return takeCount;
	}

	public void printStats() {
		logger.info("print MemoryChannelStats",
				"channel_id=\"{}\" channel_name=\"{}\" channelCapacity=\"{}\" total_puts=\"{}\" total_takes=\"{}\" current_list_size=\"{}\" size_in_bytes=\"{}\" max_size_in_bytes=\"{}\" current_consumer_count=\"{}\" consumer_names=\"{}\" this=\"{}\"",
				getChannelId(), MemoryChannel.this.getName(), channelCapacity, putCount, takeCount, eventList.size(),
				channelSizeInBytes, maxSizeInBytes, consumerNames.size(), consumerNames, this.getClass());
	}

	@Override
	public void start() {
		logger.info("starting MemoryChannelStats", "starting MemoryChannel, channel_name=\"{}\" channel_id=\"{}\"",
				getName(), getChannelId());
		if (printStats) {
			logger.info("starting MemoryChannelStats", "starting MemoryChannel");
			// printStatsDurationInSeconds = printStatsDurationInSeconds * 1000;
			startStatsThread();
		}
		logger.info("starting MemoryChannelStats", "started MemoryChannel, channel_name=\"{}\" channel_id=\"{}\"",
				getName(), getChannelId());
	}

	@Override
	public void stop() {
		channelStopped = true;
	}

	/**
	 * UUID for channel, set during build method. Only used for internal
	 * purposes.
	 * 
	 * @return
	 */
	public String getChannelId() {
		return channelId;
	}

	private void startStatsThread() {
		new Thread() {
			@Override
			public void run() {
				while (!channelStopped) {
					try {
						logger.info("heathcheck thread for MemoryChannel", "printing stats, printStatsDuration=\"{}\"",
								printStatsDurationInSeconds);
						printStats();
						sleep(printStatsDurationInSeconds);
					} catch (Exception e) {
						logger.warn("heathcheck thread for MemoryChannel",
								"health check thread received an exception, will duck it. printStatsDurationInSeconds=\"{}\"",
								printStatsDurationInSeconds, e);
					}
				}
			}
		}.start();
		logger.info("started heathcheck thread for MemoryChannel",
				"channel_id=\"{}\" thread_name=\"{}\" channel_name=\"{}\" ", getChannelId(), getName(),
				MemoryChannel.this.getName());
	}
}
