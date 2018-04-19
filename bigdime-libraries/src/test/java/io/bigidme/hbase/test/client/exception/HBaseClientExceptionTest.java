/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigidme.hbase.test.client.exception;


import io.bigdime.hbase.client.exception.HBaseClientException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static io.bigdime.constants.TestConstants.TEST;
/**
 * 
 * @author Sandeep Reddy,Murthy
 * 
 */
public class HBaseClientExceptionTest {

	HBaseClientException hBaseClientException;
   @Test
   public void HBaseClientExceptionStringConstructerTest(){
	   hBaseClientException=new HBaseClientException(TEST);
	   Assert.assertEquals(hBaseClientException.getMessage(), TEST);	   
   }
   
   @Test
   public void HBaseClientExceptionThrowableConstructerTest(){
	   hBaseClientException=new HBaseClientException(TEST,new Exception(TEST));
	   Assert.assertEquals(hBaseClientException.getMessage(), TEST);
	   Assert.assertEquals(hBaseClientException.getCause().getMessage(), TEST);
   }
}
