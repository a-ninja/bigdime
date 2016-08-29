/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.config;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class SourceConfig {
	private String name;
	private String description;
	private LinkedHashSet<HandlerConfig> handlerConfigs;
	private Map<String, Object> srcDesc;
	private String sourceType;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Set<HandlerConfig> getHandlerConfigs() {
		Collections.unmodifiableSet(handlerConfigs);
		return handlerConfigs;
	}

	public void setHandlerConfigs(LinkedHashSet<HandlerConfig> handlers) {
		this.handlerConfigs = handlers;
	}

	public Map<String, Object> getSrcDesc() {
		return srcDesc;
	}

	public void setSrcDesc(Map<String, Object> srcDesc) {
		this.srcDesc = srcDesc;
	}

	public String getSourceType() {
		return sourceType;
	}

	public void setSourceType(String sourceType) {
		this.sourceType = sourceType;
	}

	@Override
	public String toString() {
		return "SourceConfig [name=" + name + ", description=" + description + ", handlerConfigs=" + handlerConfigs
				+ ", srcDesc=" + srcDesc + ", sourceType=" + sourceType + "]";
	}

}
