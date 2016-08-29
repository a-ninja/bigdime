/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.webhdfs;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import io.bigdime.core.commons.StringCase;
import io.bigdime.core.commons.StringHelper;

public class HdfsFileNameBuilder {
	private String prefix;
	private String channelDesc;
	private String sourceFileName;
	private String extension;
	private StringCase stringCase = StringCase.DEFAULT;

	public HdfsFileNameBuilder withPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	public HdfsFileNameBuilder withChannelDesc(String channelDesc) {
		this.channelDesc = channelDesc;
		return this;
	}

	public HdfsFileNameBuilder withSourceFileName(String sourceFileName) {
		this.sourceFileName = sourceFileName;
		return this;
	}

	public HdfsFileNameBuilder withExtension(String extension) {
		this.extension = extension;
		return this;
	}

	public String build() {
		Preconditions.checkNotNull(prefix, "prefix can't be null");
		Preconditions.checkNotNull(channelDesc, "channelDesc can't be null");
		Preconditions.checkNotNull(extension, "extension can't be null");
		if (StringUtils.isBlank(sourceFileName)) {
			return StringHelper.formatField(prefix + channelDesc + extension, stringCase);
		}
		return StringHelper.formatField(prefix + sourceFileName + extension, stringCase);
	}
}