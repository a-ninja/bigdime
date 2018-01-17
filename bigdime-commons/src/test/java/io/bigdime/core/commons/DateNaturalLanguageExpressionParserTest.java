package io.bigdime.core.commons;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

public class DateNaturalLanguageExpressionParserTest {

    @Test
    public void testGetDurationAndTimeUnitFromExpression() {

        Object[] durationAndTimeUnit = null;

        Random rand = new Random();
        int duration = 0;

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " sec");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.SECONDS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " secs");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.SECONDS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " second");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.SECONDS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " seconds");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.SECONDS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " min");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.MINUTES);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " mins");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.MINUTES);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " minute");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.MINUTES);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " minutes");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.MINUTES);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " hour");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.HOURS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " hours");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.HOURS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " day");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.DAYS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));

        duration = rand.nextInt(3600);
        durationAndTimeUnit = DateNaturalLanguageExpressionParser
                .getDurationAndTimeUnitFromExpression(duration + " days");
        Assert.assertTrue((TimeUnit) durationAndTimeUnit[1] == TimeUnit.DAYS);
        Assert.assertEquals((Long) durationAndTimeUnit[0], Long.valueOf(duration));
    }

    @Test
    public void testToMillis() {
        long actualMillis = DateNaturalLanguageExpressionParser.toMillis("1 day");
        long expected = 1 * 24 * 60 * 60 * 1000;
        Assert.assertEquals(actualMillis, expected);

        actualMillis = DateNaturalLanguageExpressionParser.toMillis("2 days");
        expected = 2 * 24 * 60 * 60 * 1000;
        Assert.assertEquals(actualMillis, expected);

        actualMillis = DateNaturalLanguageExpressionParser.toMillis("1 hour");
        expected = 1 * 60 * 60 * 1000;
        Assert.assertEquals(actualMillis, expected);

        actualMillis = DateNaturalLanguageExpressionParser.toMillis("2 min");
        expected = 2 * 60 * 1000;
        Assert.assertEquals(actualMillis, expected);

        actualMillis = DateNaturalLanguageExpressionParser.toMillis("15 secs");
        expected = 15 * 1000;
        Assert.assertEquals(actualMillis, expected);

        actualMillis = DateNaturalLanguageExpressionParser.toMillis("96 days");
        long millis = 24 * 60 * 60 * 1000;
        expected = 96 * millis;
        Assert.assertEquals(actualMillis, expected);

    }
}
