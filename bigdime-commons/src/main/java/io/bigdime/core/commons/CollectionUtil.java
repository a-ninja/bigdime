package io.bigdime.core.commons;

import java.util.Collection;

public class CollectionUtil {
	private static CollectionUtil instance = new CollectionUtil();

	private CollectionUtil() {
	}

	public static CollectionUtil getInstance() {
		return instance;
	}

	/**
	 * Simple util function to return the size of the collection.
	 * 
	 * @param collection
	 *            collection object whose size to be returned. Null collection
	 *            is accepted.F
	 * @return size of the collection if the collection is not null, return -1
	 *         otherwise
	 */
	public static int getSize(final Collection<? extends Object> collection) {
		// PECS
		if (collection == null)
			return -1;
		return collection.size();
	}

	public static boolean isEmpty(final Collection<? extends Object> collection) {
		return (collection == null || collection.isEmpty());
	}

	public static boolean isNotEmpty(final Collection<? extends Object> collection) {
		return !CollectionUtil.isEmpty(collection);
	}
}
