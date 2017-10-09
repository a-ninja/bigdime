/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import io.bigdime.alert.AlertMessage;
import io.bigdime.alert.Logger;
import io.bigdime.core.AdaptorContextImpl;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;

public class AdaptorLoggerTest {

  @Test
  public void testWarn() {
    Logger logger = Mockito.mock(Logger.class);
    AdaptorLogger adaptorLogger = new AdaptorLogger(logger);
    AdaptorContextImpl.getInstance().setAdaptorName("");

    Mockito.doNothing().when(logger).warn(any(String.class), any(String.class), any(String.class),
            any(Throwable.class));
    adaptorLogger.warn("unit-shortMessage", "unit-longMessage", new Throwable());
    Mockito.verify(logger, Mockito.times(1)).warn(any(String.class), any(String.class), any(String.class), any(Object.class));
  }

  @Test
  public void testAlert() {
    Logger logger = Mockito.mock(Logger.class);
    AdaptorLogger adaptorLogger = new AdaptorLogger(logger);
    AlertMessage alertMessage = new AlertMessage();
    adaptorLogger.alert(alertMessage);
    Mockito.verify(logger, Mockito.times(1)).alert(alertMessage);
  }

}
