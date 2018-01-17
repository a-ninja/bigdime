/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * 
 * @author Neeraj Jain
 *
 */
public class StringHelperTest {
	public void getInstance() {
		StringHelper.getInstance();
	}

	@Test
	public void testRedeemTokenForString() {
		Properties properties = new Properties();
		properties.put("unit-testRedeemTokenForObject-1", "unit-testRedeemTokenForObject-value-1");
		Object propertyName = "${unit-testRedeemTokenForObject-1}";
		String newValue = StringHelper.getInstance().redeemToken(propertyName, properties);
		Assert.assertEquals(newValue, "unit-testRedeemTokenForObject-value-1");
	}

	@Test
	public void testRedeemTokenForStringWithNonExistentToken() {
		Properties properties = new Properties();
		Object propertyName = "${unit-testRedeemTokenForObject-1}";
		String newValue = StringHelper.getInstance().redeemToken(propertyName, properties);
		Assert.assertEquals(newValue, propertyName);
	}

	@Test
	public void testRedeemTokenForObject() {
		Properties properties = new Properties();
		Object propertyName = new Object();
		String newValue = StringHelper.getInstance().redeemToken(propertyName, properties);
		Assert.assertEquals(newValue, propertyName.toString());
	}

	@Test
	public void testPartitionByNewLineWithNewLineInMiddle() {
		String str = "testpartitionByNewLineWithNewLineInMiddle-new line starts here\n and more data on second line";
		Segment parsedLines = StringHelper.partitionByNewLine(str.getBytes());
		Assert.assertEquals(new String(parsedLines.getLines()),
				"testpartitionByNewLineWithNewLineInMiddle-new line starts here\n");
		Assert.assertEquals(new String(parsedLines.getLeftoverData()), " and more data on second line");
	}

	@Test
	public void testPartitionByNewLineWithNewLineInTheEnd() {
		String str = "testpartitionByNewLineWithNewLineInTheEnd-new line starts here\n";
		Segment parsedLines = StringHelper.partitionByNewLine(str.getBytes());
		Assert.assertEquals(new String(parsedLines.getLines()),
				"testpartitionByNewLineWithNewLineInTheEnd-new line starts here\n");
		Assert.assertEquals(new String(parsedLines.getLeftoverData()), "");
	}

	@Test
	public void testPartitionByNewLineWithNoNewLine() {
		String str = "testpartitionByNewLineWithNoNewLine-no line here";
		Segment parsedLines = StringHelper.partitionByNewLine(str.getBytes());
		Assert.assertNull(parsedLines);
	}

	@Test
	public void testPartitionByNewLineWithNewLineInTheBeginning() {
		String str = "\ntestpartitionByNewLineWithNewLineInTheBeginning-new line in beginning here";
		Segment parsedLines = StringHelper.partitionByNewLine(str.getBytes());
		Assert.assertEquals(new String(parsedLines.getLines()), "\n");
		Assert.assertEquals(new String(parsedLines.getLeftoverData()),
				"testpartitionByNewLineWithNewLineInTheBeginning-new line in beginning here");
	}

	@Test
	public void testPartitionByNewLineWithMultipleNewLines() {
		String str = "\ntestpartitionByNewLineWithMultipleNewLines\n-new line in beginning here\n";
		Segment parsedLines = StringHelper.partitionByNewLine(str.getBytes());
		Assert.assertEquals(new String(parsedLines.getLines()),
				"\ntestpartitionByNewLineWithMultipleNewLines\n-new line in beginning here\n");
		Assert.assertEquals(new String(parsedLines.getLeftoverData()), "");
	}

	/**
	 * Test with firstPart has no new line and second part has a new line.
	 */
	@Test
	public void testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNewLineInSecondPart() {
		String part1 = "testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNewLineInSecondPart-part1-no new line in part1";
		String part2 = "testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNewLineInSecondPart-part2-\nnew line in part2";
		Segment parsedLines = StringHelper.appendAndPartitionByNewLine(part1.getBytes(), part2.getBytes());
		Assert.assertEquals(new String(parsedLines.getLines()),
				"testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNewLineInSecondPart-part1-no new line in part1testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNewLineInSecondPart-part2-\n");
		Assert.assertEquals(new String(parsedLines.getLeftoverData()), "new line in part2");
	}

	/**
	 * Test with firstPart and secondPart having no new line.
	 */
	@Test
	public void testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNoNewLineInSecondPart() {
		String part1 = "testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNoNewLineInSecondPart-part1-no new line in part1";
		String part2 = "testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNoNewLineInSecondPart-part2-no new line in part2";
		Segment parsedLines = StringHelper.appendAndPartitionByNewLine(part1.getBytes(), part2.getBytes());
		Assert.assertEquals(new String(parsedLines.getLines(), Charset.defaultCharset()),
				"testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNoNewLineInSecondPart-part1-no new line in part1");
		Assert.assertEquals(new String(parsedLines.getLeftoverData(), Charset.defaultCharset()),
				"testAppendAndPartitionByNewLineWithNoNewLineInFirstPartAndNoNewLineInSecondPart-part2-no new line in part2");
	}

	/**
	 * Test with firstPart and secondPart having new line.
	 */
	@Test
	public void testAppendAndPartitionByNewLineWithNewLineInFirstPartAndNewLineInSecondPart() {
		String part1 = "testAppendAndPartitionByNewLineWithNewLineInFirstPartAndNewLineInSecondPart-part1-\n new line in part1";
		String part2 = "testAppendAndPartitionByNewLineWithNewLineInFirstPartAndNewLineInSecondPart-part2-\n new line in part2";
		Segment parsedLines = StringHelper.appendAndPartitionByNewLine(part1.getBytes(), part2.getBytes());
		Assert.assertEquals(new String(parsedLines.getLines()),
				"testAppendAndPartitionByNewLineWithNewLineInFirstPartAndNewLineInSecondPart-part1-\n new line in part1testAppendAndPartitionByNewLineWithNewLineInFirstPartAndNewLineInSecondPart-part2-\n");
		Assert.assertEquals(new String(parsedLines.getLeftoverData()), " new line in part2");
	}

	@Test(expectedExceptions = IllegalArgumentException.class)
	public void testGetRelativePathWithNullAbsolutePath() {
		StringHelper.getRelativePath(null, null);
	}

	@Test
	public void testGetRelativePathWithNullBasePath() {
		String relativePath = StringHelper.getRelativePath("/unit/test/testGetRelativePathWithNullBasePath", null);
		Assert.assertEquals(relativePath, "/unit/test/testGetRelativePathWithNullBasePath");
	}

	@Test
	public void testGetRelativePath() {
		String relativePath = StringHelper.getRelativePath("/unit/test/2/testGetRelativePathWithNullBasePath",
				"/unit/test");
		Assert.assertEquals(relativePath, "/2/testGetRelativePathWithNullBasePath");
	}

	@Test
	public void testGetStringAfterLastToken() {
		String path = StringHelper.getStringAfterLastToken("/unit/test/2/testGetStringAfterLastToken", "/");
		Assert.assertEquals(path, "testGetStringAfterLastToken");

	}

	@Test
	public void testGetStringBeforeLastToken() {
		String path = StringHelper.getStringBeforeLastToken("/unit/test/2/testGetStringAfterLastToken", "/");
		Assert.assertEquals(path, "/unit/test/2");

	}

	/**
	 * 
	 */
	@Test
	public void testReplaceToken() {

		String inputString = "/path1/path2/path3/path4/date=2016-07-01/path6/file_123.txt";

		Pattern inputPattern = Pattern.compile(".+\\/date=(\\w+-\\w+-\\w+)\\/(\\w+)\\/([\\w\\.\\-_]+)$");
		String outPattern = "$3";
		String replacedString = StringHelper.replaceTokens(inputString, outPattern, inputPattern);
		Assert.assertEquals(replacedString, "file_123.txt");

		outPattern = "$1/$3";
		replacedString = StringHelper.replaceTokens(inputString, outPattern, inputPattern);
		Assert.assertEquals(replacedString, "2016-07-01/file_123.txt");
		outPattern = "$1__$2/$3";
		replacedString = StringHelper.replaceTokens(inputString, outPattern, inputPattern);
		Assert.assertEquals(replacedString, "2016-07-01__path6/file_123.txt");
	}

	@Test
	public void testReplaceTokenWithHeaders() {

		String inputString = "/path1/path2/path3/path4/date=2016-07-01/path6/file_123.txt";

		Map<String, String> map = new HashMap<>();
		map.put("entityName", "unit-entity-name");
		Pattern inputPattern = Pattern.compile(".+\\/date=(\\w+-\\w+-\\w+)\\/(\\w+)\\/([\\w\\.\\-_]+)$");
		String outPattern = "$3";
		String replacedString = StringHelper.replaceTokens(inputString, outPattern, inputPattern);
		Assert.assertEquals(replacedString, "file_123.txt");

		outPattern = "$1/$3";
		replacedString = StringHelper.replaceTokens(inputString, outPattern, inputPattern);
		Assert.assertEquals(replacedString, "2016-07-01/file_123.txt");
		
		outPattern = "$1__$2_$entityName/$3";
		replacedString = StringHelper.replaceTokens(inputString, outPattern, inputPattern, map);
		Assert.assertEquals(replacedString, "2016-07-01__path6_unit-entity-name/file_123.txt");
		
		inputString = "/path1/path2/path3/path4/date=2016-07-01/path6/file_123.txt";
		outPattern = "$1__$2_$entityName";
		replacedString = StringHelper.replaceTokens(inputString, outPattern, inputPattern, map);
		Assert.assertEquals(replacedString, "2016-07-01__path6_unit-entity-name");
		
	}
}
