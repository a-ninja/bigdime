/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.common.testutils;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;

/**
 * Helper class to test the getters and setters of a java bean.
 *
 * @author Neeraj Jain
 */
public final class GetterSetterTestHelper {
  private static final GetterSetterTestHelper instance = new GetterSetterTestHelper();

  private GetterSetterTestHelper() {

  }

  /**
   * Helper method to test the getter and setter of a String type field.
   *
   * @param object        Object that contains the String field
   * @param attributeName name of field
   * @param setValue      value to be used to test the getters and setters
   */
  public static void doTest(Object object, String attributeName, String setValue) {
    ReflectionTestUtils.invokeSetterMethod(object, attributeName, setValue);
    Object gotValue = ReflectionTestUtils.invokeGetterMethod(object, attributeName);
    Assert.assertEquals(setValue, gotValue);
  }

  /**
   * Helper method to test the getter and setter of a int type field.
   *
   * @param object        Object that contains the int field
   * @param attributeName name of field
   * @param setValue      value to be used to test the getters and setters
   */
  public static void doTest(Object object, String attributeName, int setValue) {
    ReflectionTestUtils.invokeSetterMethod(object, attributeName, setValue);
    Object gotValue = ReflectionTestUtils.invokeGetterMethod(object, attributeName);
    Assert.assertEquals(setValue, gotValue);
  }

  /**
   * Helper method to test the getter and setter of a long type field.
   *
   * @param object        Object that contains the long field
   * @param attributeName name of field
   * @param setValue      value to be used to test the getters and setters
   */
  public static void doTest(Object object, String attributeName, long setValue) {
    ReflectionTestUtils.invokeSetterMethod(object, attributeName, setValue);
    Object gotValue = ReflectionTestUtils.invokeGetterMethod(object, attributeName);
    Assert.assertEquals(setValue, gotValue);
  }

  /**
   * Helper method to test the getter and setter of a long type field.
   *
   * @param object        Object that contains the long field
   * @param attributeName name of field
   * @param setValue      value to be used to test the getters and setters
   */
  public static void doTest(Object object, String attributeName, Object setValue) {
    ReflectionTestUtils.invokeSetterMethod(object, attributeName, setValue);
    Object gotValue = ReflectionTestUtils.invokeGetterMethod(object, attributeName);
    Assert.assertEquals(setValue, gotValue);
  }

  public static void doGetTest(Object object, String attributeName, Object expectedValue) {
    Object gotValue = ReflectionTestUtils.invokeGetterMethod(object, attributeName);
    Assert.assertEquals(gotValue, expectedValue);
  }
}
