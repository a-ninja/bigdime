/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.handler;

import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.ActionEvent;
import io.bigdime.core.ActionEvent.Status;
import io.bigdime.core.AdaptorConfigurationException;
import io.bigdime.core.HandlerException;
import io.bigdime.core.commons.AdaptorLogger;
import io.bigdime.core.config.AdaptorConfigConstants;
import io.bigdime.core.config.AdaptorConfigConstants.SourceConfigConstants;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author Neeraj Jain
 *
 */
@Component
@Scope("prototype")
public class DummyConcreteHandler extends AbstractHandler {
	private static final AdaptorLogger logger = new AdaptorLogger(LoggerFactory.getLogger(DummyConcreteHandler.class));
	private String inputSrcDescName;
	private String outputChannelName;

	public DummyConcreteHandler() {
	}

	int count = 0;

	@Override
	public Status process() throws HandlerException {

		if (getHandlerContext().getEventList() == null || getHandlerContext().getEventList().isEmpty())
			return Status.BACKOFF;
//			return null;
		ActionEvent actionEvent = getHandlerContext().getEventList().get(0);
		actionEvent.getHeaders().put(AdaptorConfigConstants.SourceConfigConstants.SRC_DESC, inputSrcDescName);
		actionEvent.getHeaders().put("channel", outputChannelName);
		logger.debug("processing handler",
				"_message=\"processing dummy handler\" handler_name={} this={} outputChannel={}", getName(), this,
				getOutputChannel());
		ActionEvent newActionEvent = new ActionEvent();
		newActionEvent.setBody("body".getBytes(Charset.defaultCharset()));
		newActionEvent.getHeaders().put("TIMESTAMP", "dummy");
		// if (getOutputChannel() != null) {
		// getOutputChannel().put(newActionEvent);
		// }
		count++;
		if (count >= 6) {
			return Status.BACKOFF;
		}

		// if (inputChannelMap !=null) {
		//
		// }
		return Status.READY;
		// throw new NotImplementedException();
		// this.state = State.RUNNING;
		// return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void build() throws AdaptorConfigurationException {
		super.build();

		Entry<String, String> srcDesc = (Entry<String, String>) getPropertyMap().get(SourceConfigConstants.SRC_DESC);
		if (srcDesc != null) {
			inputSrcDescName = srcDesc.getKey();
			outputChannelName = srcDesc.getValue();
		}

		// logger
		logger.debug("building handler", "handler_name={} properties={} inputSrcDescName={}", getName(),
				getPropertyMap(), inputSrcDescName);
		Set<String> keys = getPropertyMap().keySet();
		for (String key : keys) {
			logger.debug("building handler", "handler_name=\"{}\" key=\"{}\" value.class=\"{}\" value=\"{}\"",
					getName(), key, getPropertyMap().get(key).getClass(), getPropertyMap().get(key));
			// logger.debug("building handler", "handler_name=\"{}\" key=\"{}\"
			// value.class=\"{}\"", getName(), key,
			// getPropertyMap().get(key));
		}
	}

	protected void submitToChannel(ActionEvent actionEvent) {

	}

	@Override
	public String toString() {
		return "DummyConcreteHandler [inputSrcDescName=" + inputSrcDescName + ", outputChannelName=" + outputChannelName
				+ ", count=" + count + ", properties=" + getPropertyMap() + "]";
	}

	public String getInputSrcDescName() {
		return inputSrcDescName;
	}
}
