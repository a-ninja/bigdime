package io.bigdime.impl.biz.service;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.hbase.client.DataInsertionSpecification;
import io.bigdime.hbase.client.DataRetrievalSpecification;
import io.bigdime.hbase.client.HbaseManager;
import io.bigdime.hbase.client.exception.HBaseClientException;
import io.bigdime.impl.biz.dao.Adaptor;
import io.bigdime.impl.biz.dao.AdaptorConstants;
import io.bigdime.impl.biz.dao.Datahandler;
import io.bigdime.impl.biz.dao.JsonData;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import  java.util.Arrays;

import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_COLUMN_FAMILY_NAME;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_CHANNEL;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_ENVIRONMENT;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_HANDLERCLASS;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_HANDLERNAME;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_MANDATORYFIELDS;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_NAME;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_NONDEFAULTS;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_SINKCLASS;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_SINKNAME;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_NONESSENTIALFIELDS;
import static io.bigdime.impl.biz.constants.ApplicationConstants.METADATA_JSON_NAME;


@Component
public class HbaseJsonDataService {
	private static final Logger logger = LoggerFactory
			.getLogger(HbaseJsonDataService.class);
	
	@Autowired
	private HbaseManager hbaseManager;
	@Value("${hbase.adaptor.table}")
	private String adaptorTable;
    @Value("${hbase.adaptormetadata.table}")
    private String adaptor_metadata;
    private static String COMMA=",";
    private static String ENVDEV="dev";
    private static String ENVQA="qa";
    private static String ENVPROD="prod";
       
	/**
	 * get configTemplate of a given adaptor
	 */
	
	public JsonData getJSON(String configTemplate){
		JsonData jsonData=null;
		Get get = new Get(Bytes.toBytes(configTemplate));
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(adaptorTable).withGet(get).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			Result result = hbaseManager.getResult();
			ObjectMapper objMapper = new ObjectMapper();
			objMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
			if (result != null) {				
					try {
						ObjectNode adaptor = (ObjectNode) objMapper.readTree(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, Bytes.toBytes("jn"))));
						jsonData=new JsonData();
						jsonData=objMapper.readValue(adaptor.toString(),JsonData.class);
					} catch (UnsupportedEncodingException e) {
						logger.warn(
								"BIGDIME-MONITORING-SERVICE",
								"The result cannot be parsed",
								e.getMessage());
					}
				}

		} catch (HBaseClientException | IOException e) {
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to fetch data from HBASE",
					e.getMessage());
		} catch(Exception e){
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to connect to HBASE",
					e.getMessage());
		}
		return jsonData;
	}
	
	/**
	 * Save user written configTemplate of a given adaptor
	 */
	public boolean updateJson(JsonData jsonData,String key) throws JsonGenerationException,
			JsonMappingException, IOException {
		Get get = new Get(Bytes.toBytes(key));
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(adaptorTable).withGet(get).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			Put put = new Put(Bytes.toBytes(key));
			Result result = hbaseManager.getResult();
			ObjectMapper objMapper = new ObjectMapper();
			objMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
			String rowKeyValuefromHbase = null;
			if (result != null) {				
				rowKeyValuefromHbase=new String(result.getRow());
				}
			if (!key.equals(rowKeyValuefromHbase)) {
				DataInsertionSpecification.Builder dataInsertionSpecificationBuiler=new DataInsertionSpecification.Builder();
				put.add(METADATA_COLUMN_FAMILY_NAME,Bytes.toBytes("jn"),Bytes.toBytes(objMapper
						.writeValueAsString((JsonData)jsonData)));
				dataInsertionSpecificationBuiler.withTableName(adaptorTable).withtPut(put).build();
			}else{
				logger.info("BIGDIME-MONITORING-SERVICE","INSERT/UPDATE","The JSON is aleady present in Hbase and no updates are made");  
	        	 return false;
			}
		} catch (HBaseClientException | IOException e) {
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to fetch data from HBASE",
					e.getMessage());
		} catch(Exception e){
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to connect to HBASE",
					e.getMessage());
		}
		return false;
	}
	
	/**
	 * get handlerTemplate of a given adaptor
	 */
	
	public Datahandler getHandler(String handlerTemplate){
		Datahandler datahandler=null;
		Get get = new Get(Bytes.toBytes(handlerTemplate));
		
		try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder
					.withTableName(adaptorTable).withGet(get).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			Result result = hbaseManager.getResult();
			ObjectMapper objMapper = new ObjectMapper();
			objMapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
			if (result != null) {				
					try {
						ObjectNode handlerJson = (ObjectNode) objMapper.readTree(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_JSON_NAME)));
						datahandler=new Datahandler();
						datahandler=objMapper.readValue(handlerJson.toString(),Datahandler.class);
					} catch (UnsupportedEncodingException e) {
						logger.warn(
								"BIGDIME-MONITORING-SERVICE",
								"The result cannot be parsed",
								e.getMessage());
					}
				}

		} catch (HBaseClientException | IOException e) {
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to fetch data from HBASE",
					e.getMessage());
		} catch(Exception e){
			logger.warn(
					"BIGDIME-MONITORING-SERVICE",
					"Unable to connect to HBASE",
					e.getMessage());
		}
		return datahandler;
	}
	
	public List<AdaptorConstants> getAdaptorConstants(){
		List<AdaptorConstants> adaptorConstantList=new ArrayList<AdaptorConstants>();
		Scan scan = new Scan();
		Map<String,AdaptorConstants> map=new HashMap<String,AdaptorConstants>();
        map.put(ENVDEV,new AdaptorConstants());
        map.put(ENVQA,new AdaptorConstants());
        map.put(ENVPROD,new AdaptorConstants());
		for(Map.Entry<String, AdaptorConstants> entry:map.entrySet()){
			List<Adaptor> adaptorList=new ArrayList<Adaptor>();
		Filter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(entry.getKey()));
        scan.setFilter(filter);
        try {
			DataRetrievalSpecification.Builder dataRetrievalSpecificationBuilder = new DataRetrievalSpecification.Builder();
			DataRetrievalSpecification dataRetrievalSpecification = dataRetrievalSpecificationBuilder.withTableName(adaptor_metadata).withScan(scan).build();
			hbaseManager.retreiveData(dataRetrievalSpecification);
			ResultScanner scanner = hbaseManager.getResultScanner();
			if (scanner != null) {	
				for(Result result:scanner){
					Adaptor adaptor=new Adaptor();
					 if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_NAME)){
						  adaptor.setName(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_NAME),StandardCharsets.UTF_8.toString()));
					 }
					 if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_HANDLERNAME)){	
						  List<String> handlerList=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_HANDLERNAME),StandardCharsets.UTF_8.toString()).split(COMMA)));							  
						  adaptor.setHandlerList(handlerList);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_HANDLERCLASS)){	
						  List<String> handlerClassList=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME,METADATA_HANDLERCLASS),StandardCharsets.UTF_8.toString()).split(COMMA)));					 
						  adaptor.setHandlerClassList(handlerClassList);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_SINKNAME)){	
						  List<String> sinkList=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_SINKNAME),StandardCharsets.UTF_8.toString()).split(COMMA)));					 
						  adaptor.setSinkList(sinkList);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_SINKCLASS)){	
						  List<String> sinkClassList=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_SINKCLASS),StandardCharsets.UTF_8.toString()).split(COMMA)));					 
						  adaptor.setSinkClassList(sinkClassList);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_CHANNEL)){	
						  List<String> channelList=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_CHANNEL),StandardCharsets.UTF_8.toString()).split(COMMA)));					 
						  adaptor.setChannelList(channelList);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_NONDEFAULTS)){	
						  List<String> nonDefaults=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_NONDEFAULTS),StandardCharsets.UTF_8.toString()).split(COMMA)));					 
						  adaptor.setNonDefaults(nonDefaults);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_MANDATORYFIELDS)){	
						  List<String> mandatoryFields=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_MANDATORYFIELDS),StandardCharsets.UTF_8.toString()).split(COMMA)));					 
						  adaptor.setMandatoryFields(mandatoryFields);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_NONESSENTIALFIELDS)){	
						  List<String> nonessentialFields=new ArrayList<String>(Arrays.asList(new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_NONESSENTIALFIELDS),StandardCharsets.UTF_8.toString()).split(COMMA)));					 
						  adaptor.setNonessentialFields(nonessentialFields);
					  }
					  if(result.containsColumn(METADATA_COLUMN_FAMILY_NAME, METADATA_ENVIRONMENT)){	
						  String environment=new String(result.getValue(METADATA_COLUMN_FAMILY_NAME, METADATA_ENVIRONMENT),StandardCharsets.UTF_8.toString());					 
						  entry.getValue().setEnvironment(environment);
					  }
					 adaptorList.add(adaptor);				
				}
				 entry.getValue().setAdaptorList(adaptorList);
				 adaptorConstantList.add(entry.getValue());
			   }
			}
        catch(Exception e){
        	logger.warn("BIGDIME-MONITORING-SERVICE","The result cannot be parsed",e.getMessage());
        }
        }
		
		return adaptorConstantList;
	}
		
}
	