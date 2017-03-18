/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.libs.hdfs;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RoundRobinStrategyTest {

  @Test
  public void testGetNextServiceHost() {
//		RoundRobinStrategy1$.MODULE$.apply("a,b,c,d");
    RoundRobinStrategy$ roundRobinStrategy = RoundRobinStrategy$.MODULE$;
//		String h = inst.getNextServiceHost();
//		System.out.println("h="+h);
//		RoundRobinStrategy roundRobinStrategy = RoundRobinStrategy.getInstance();
    roundRobinStrategy.setHosts("a,b,c,d");
    String nextHost = roundRobinStrategy.getNextServiceHost();
    Assert.assertEquals(nextHost, "a");

    nextHost = roundRobinStrategy.getNextServiceHost();
    Assert.assertEquals(nextHost, "b");

    nextHost = roundRobinStrategy.getNextServiceHost();
    Assert.assertEquals(nextHost, "c");

    nextHost = roundRobinStrategy.getNextServiceHost();
    Assert.assertEquals(nextHost, "d");

    nextHost = roundRobinStrategy.getNextServiceHost();
    Assert.assertEquals(nextHost, "a");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetNextServiceHostWithNullHost() {
//		RoundRobinStrategy roundRobinStrategy = RoundRobinStrategy.getInstance();
    RoundRobinStrategy$ roundRobinStrategy = RoundRobinStrategy$.MODULE$;
    roundRobinStrategy.setHosts(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetNextServiceHostWithEmptyHost() {
//		RoundRobinStrategy roundRobinStrategy = RoundRobinStrategy.getInstance();
    RoundRobinStrategy$ roundRobinStrategy = RoundRobinStrategy$.MODULE$;
    roundRobinStrategy.setHosts("");
  }
}
