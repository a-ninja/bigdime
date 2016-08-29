package io.bigdime.impl.biz.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.Source;

import org.testng.Assert;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.bigdime.impl.splunkalert.test.constants.TestResourceConstants.TEST_STRING;

public class SourceTest {
	private static final Logger logger = LoggerFactory.getLogger(SourceTest.class);
	@BeforeTest
	public void setup() {
		logger.info("source type","Test Phase","Setting the environment");
		System.setProperty("env", "test");
	}	
		
	@Test
	public void sourceGettersAndSettersTest(){
		Source source=new Source();
		source.setName(TEST_STRING);
		source.setDescription(TEST_STRING);
		source.setSourcetype(TEST_STRING);
		List<Datahandler> datahandlerList=new ArrayList<Datahandler>(); 
		Datahandler datahandler=new Datahandler();
        datahandler.setDescription(TEST_STRING);
        datahandler.setName(TEST_STRING);
        datahandlerList.add(datahandler);
        source.setDatahandlers(datahandlerList);
        Map<String,String> srcDesc=new HashMap<String,String>();
        srcDesc.put(TEST_STRING, TEST_STRING);
        source.setSrcdesc(srcDesc);
        Assert.assertEquals(TEST_STRING, source.getDescription());
        Assert.assertEquals(TEST_STRING, source.getName());
        Assert.assertEquals(TEST_STRING, source.getSourcetype());
        
        for(Datahandler datahandlerObj:source.getDatahandlers()){
        	Assert.assertEquals(TEST_STRING , datahandlerObj.getDescription());
        	Assert.assertEquals(TEST_STRING , datahandlerObj.getName());
        }
        for (Map.Entry<String, String> sourceDescriptionObj : source.getSrcdesc().entrySet()) {
    		Assert.assertEquals(TEST_STRING,sourceDescriptionObj.getValue() );
    	}
	}
}
