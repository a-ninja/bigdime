/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core;

import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.flume.NamedComponent;

import io.bigdime.core.ActionEvent.Status;

/**
 * TODO add comments
 *
 * @author Neeraj Jain
 *
 */

public interface Handler extends NamedComponent {

	public void build() throws AdaptorConfigurationException;

	/**
	 * Returns the name of this handler. The name is read from the configuration
	 * while building the handler. If no name is provided, a null is returned.
	 *
	 * @return this handler's name. null if no name is provided.
	 */
	@Override
	public String getName();

	/**
	 * Returns the identifier of this handler. The identifier is unique and
	 * remains unchanged during its lifetime.
	 *
	 * @return this handler's id, a not null value
	 */
	@NotNull
	public String getId();

	public Status process() throws HandlerException;

	public State getState();

	public void shutdown();

	/**
	 * The propertyMap is an unmodifiable Map.
	 * 
	 * @param propertyMap
	 */
	public void setPropertyMap(Map<String, Object> propertyMap);

	public void handleException();

	/**
	 * Provide the index, 0 based, of the handler in the chain.
	 * 
	 * @return
	 */
	public int getIndex();

	/**
	 * Set the index of the handler in the chain, 0 based.
	 * 
	 * @param index
	 */
	public void setIndex(int index);

	/**
	 * Concrete Class object that represents the handler. Can be used for
	 * printing purposes.
	 * 
	 * @param clazz
	 */
	public void setHandlerClass(Class<? extends Handler> clazz);

	/**
	 * Get the complete class name.
	 * 
	 * @return
	 */
	public String getHandlerClass();

	public enum State {
		/**
		 * CREATED(0) reserved for later use.
		 */

		/**
		 * Handler is in INIT state if it's been instantiated but hasn't
		 * processed anything yet.
		 */
		INIT(1),

		/**
		 * IDLE(2), reserved for later use.
		 */

		/**
		 * A handler in running state if it has processed or is processing
		 * events and not been shutdown yet.
		 */
		RUNNING(3),

		/**
		 * State of handler after it's shutdown.
		 */
		TERMINATED(10);

		private final Integer value;

		private State(Integer value) {
			this.value = value;
		}

		public Integer getValue() {
			return value;
		}
	}
}
