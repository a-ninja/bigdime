package io.bigdime.alert.impl.swift;

import io.bigdime.alert.Logger;
import io.bigdime.alert.Logger.ALERT_CAUSE;
import io.bigdime.alert.Logger.ALERT_SEVERITY;
import io.bigdime.alert.Logger.ALERT_TYPE;
import io.bigdime.alert.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

@ContextConfiguration(locations = {"classpath:META-INF/application-context-monitoring.xml"})

public class SwiftLoggerTest extends AbstractTestNGSpringContextTests {

    @Test
    public void testGetLogger() {
        Logger logger = LoggerFactory.getLogger("test");
        Assert.assertNotNull(logger);
        System.out.println(logger.getClass().toString());
        Assert.assertEquals(logger.getClass().toString(),
                "class io.bigdime.alert.impl.AlertLoggerFactoryImpl$MultipleLogger");

        Logger loggerDup = LoggerFactory.getLogger("test");
        Assert.assertNotNull(loggerDup);

        Assert.assertSame(logger, loggerDup);
    }

    @Test
    public void testGetLogger2() {
        Logger logger = LoggerFactory.getLogger("test-unit");
        Assert.assertNotNull(logger);
        Assert.assertEquals(logger.getClass().toString(),
                "class io.bigdime.alert.impl.AlertLoggerFactoryImpl$MultipleLogger");

        Logger loggerDup = LoggerFactory.getLogger("test-unit1");
        Assert.assertNotNull(loggerDup);

        Assert.assertNotEquals(logger, loggerDup);
    }

    @Test
    public void testAlert1() {
        try {
            Logger logger = LoggerFactory.getLogger("MultipleLoggerFactoryTest.class");
            logger.debug("adap", "short", "long");
            logger.alert("unit-test-source", ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                    ALERT_SEVERITY.BLOCKER, "message in detail");

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAlert2() {
        try {

            Logger logger = LoggerFactory.getLogger(SwiftLoggerTest.class);

            logger.alert("unit-test-source", ALERT_TYPE.ADAPTOR_FAILED_TO_START, ALERT_CAUSE.APPLICATION_INTERNAL_ERROR,
                    ALERT_SEVERITY.BLOCKER, "swift message", new Exception("unit-exception"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testWarn() {
        try {
//			ByteBuffer byteBuffer = ByteBuffer.allocate(12);
//			byteBuffer.put("hello".getBytes(), 0, 5);
//			System.out.println("position="+byteBuffer.position());
//			System.out.println("limit="+byteBuffer.limit());
//			System.out.println("remaining="+byteBuffer.remaining());
////			byteBuffer.flip();
////			System.out.println("position="+byteBuffer.position());
////			System.out.println("limit="+byteBuffer.limit());
//			byte[] b = new byte[byteBuffer.position()];
//			byteBuffer.get(b);
//			System.out.println(new String(b));
//
//			byteBuffer.put(" world".getBytes());//, byteBuffer.position(), 6);
//			ByteArrayOutputStream baos = null;
//			baos.size();

            Logger logger = LoggerFactory.getLogger(SwiftLoggerTest.class);

            for (int i = 0; i < 1000; i++) {
                logger.warn("adaptor-name",
                        "_message=\"source is still running. can't start the source\" source_name=\"{}\"",
                        "source.getName()");
            }
//			logger.warn("starting adaptor",
//					"_message=\"source is still running. can't start the source\" source_name=\"{}\"",
//					"source.getName()");
//			logger.warn("starting adaptor",
//					"_message=\"source is still running. can't start the source\" source_name=\"{}\"",
//					"source.getName()");
//			logger.warn("starting adaptor",
//					"_message=\"source is still running. can't start the source\" source_name=\"{}\"",
//					"source.getName()");
//			logger.warn("starting adaptor",
//					"_message=\"source is still running. can't start the source\" source_name=\"{}\"",
//					"source.getName()");
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

}
