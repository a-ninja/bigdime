/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;

import io.bigdime.handler.webhdfs.HdfsFileNameBuilder;

public class HdfsFileNameBuilderTest {

	@Test
	public void testBuild() {
		HdfsFileNameBuilder hdfsFileNameBuilder = new HdfsFileNameBuilder();
		String channelDesc = "unitchannel";
		String extension = ".unitext";
		String prefix = "unitprefix";
		String sourceFileName = "unitsource";

		String path = hdfsFileNameBuilder.withChannelDesc(channelDesc).withExtension(extension).withPrefix(prefix)
				.withSourceFileName(sourceFileName).build();
		Assert.assertEquals(path, "unitprefixunitsource.unitext");
	}

	@Test
	public void testBuildWithoutSourceFileName() {
		HdfsFileNameBuilder hdfsFileNameBuilder = new HdfsFileNameBuilder();
		String channelDesc = "unitchannel";
		String extension = ".unitext";
		String prefix = "unitprefix";

		String path = hdfsFileNameBuilder.withChannelDesc(channelDesc).withExtension(extension).withPrefix(prefix)
				.build();
		Assert.assertEquals(path, "unitprefixunitchannel.unitext");
	}

	@Test
	public void testBuildWithCamelCase() {
		HdfsFileNameBuilder hdfsFileNameBuilder = new HdfsFileNameBuilder();
		String channelDesc = "unitChannel";
		String extension = ".uniText";
		String prefix = "unitPrefix";

		String path = hdfsFileNameBuilder.withChannelDesc(channelDesc).withExtension(extension).withPrefix(prefix)
				.build();
		Assert.assertEquals(path, "unitPrefixunitChannel.uniText");
	}

	@Test
	public void testBuildWithAllUpperCase() {
		HdfsFileNameBuilder hdfsFileNameBuilder = new HdfsFileNameBuilder();
		String channelDesc = "UNITCHANNEL";
		String extension = ".UNITTEXT";
		String prefix = "UNITPREFIX";

		String path = hdfsFileNameBuilder.withChannelDesc(channelDesc).withExtension(extension).withPrefix(prefix)
				.build();
		Assert.assertEquals(path, "UNITPREFIXUNITCHANNEL.UNITTEXT");
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void testBuildWithNullChannel() {
		HdfsFileNameBuilder hdfsFileNameBuilder = new HdfsFileNameBuilder();
		String extension = ".unitext";
		String prefix = "unitprefix";
		String sourceFileName = "unitsource";
		hdfsFileNameBuilder.withExtension(extension).withPrefix(prefix).withSourceFileName(sourceFileName).build();
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void testBuildWithNullPrefix() {
		HdfsFileNameBuilder hdfsFileNameBuilder = new HdfsFileNameBuilder();
		String channelDesc = "unitchannel";
		String extension = ".unitext";
		String sourceFileName = "unitsource";

		String path = hdfsFileNameBuilder.withChannelDesc(channelDesc).withExtension(extension)
				.withSourceFileName(sourceFileName).build();
		Assert.assertEquals(path, "unitprefixunitsource.unitext");
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void testBuildWithNullExtension() {
		HdfsFileNameBuilder hdfsFileNameBuilder = new HdfsFileNameBuilder();
		String channelDesc = "unitchannel";
		String prefix = "unitprefix";
		String sourceFileName = "unitsource";

		String path = hdfsFileNameBuilder.withChannelDesc(channelDesc).withPrefix(prefix)
				.withSourceFileName(sourceFileName).build();
		Assert.assertEquals(path, "unitprefixunitsource.unitext");
	}
}
