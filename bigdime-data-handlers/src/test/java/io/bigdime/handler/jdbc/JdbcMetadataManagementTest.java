package io.bigdime.handler.jdbc;


import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.Matchers.anyString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.model.Attribute;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class JdbcMetadataManagementTest {
	
	JdbcMetadataManagement jdbcMetadataManagement; 
	
	@Mock
	JdbcTemplate jdbcTemplate;
	
	@Mock
	JdbcInputDescriptor jdbcInputDescriptor;
	
	@Mock
	Metasegment metasegment;
	
	@BeforeClass
	public void init(){
		initMocks(this);
		jdbcMetadataManagement = new JdbcMetadataManagement();
	}
	
	
	
	@Test
	public void testGetSourceMetadataNotNull(){
		when(jdbcTemplate.query(anyString(), Mockito.any(JdbcMetadata.class))).thenReturn(metasegment);
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("testDB");
		Assert.assertNotNull(jdbcMetadataManagement.getSourceMetadata(jdbcInputDescriptor, jdbcTemplate));
	}
	
	@Test
	public void testCheckSourceMetadataNull(){//		init();
		when(jdbcInputDescriptor.getDatabaseName()).thenReturn("");
		Assert.assertNull(jdbcMetadataManagement.getSourceMetadata(jdbcInputDescriptor, jdbcTemplate));
	}
	
	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testGetColumnListException() {
		jdbcMetadataManagement.getColumnList(jdbcInputDescriptor, null);
	}
	
	@Test
	public void testGetColumnList() {
		Attribute attribute = new Attribute();
		attribute.setAttributeName("testAttributeName");
		attribute.setAttributeType("testAttributeType");
		Set<Attribute> attributeSet = new LinkedHashSet<Attribute>();
		attributeSet.add(attribute);
		Entitee entity = new Entitee();
		entity.setEntityName("testEntityname");
		entity.setDescription("testDescription");
		entity.setVersion(1.0);
		entity.setAttributes(attributeSet);
		Set<Entitee> entitySet = new LinkedHashSet<Entitee>();
		entitySet.add(entity);
		when(metasegment.getEntitees()).thenReturn(entitySet);
		entity.setAttributes(attributeSet);
		Assert.assertNotNull(jdbcMetadataManagement.getColumnList(jdbcInputDescriptor, metasegment));
	}
	
	@Test
	public void testCheckAndUpdateMetadata() throws MetadataAccessException {
		MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
		Entitee entity = new Entitee();
		entity.setDescription("testDescription");
		entity.setVersion(1.0);
		Set<Entitee> entitySet = new LinkedHashSet<Entitee>();
		entitySet.add(entity);
		when(metasegment.getEntitees()).thenReturn(entitySet);
		List<String> columnNamesList = new ArrayList<String>();
		columnNamesList.add("testColumnName1");
		Metasegment metaseg = Mockito.mock(Metasegment.class);
		when(metadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenReturn(metaseg);
		when(metaseg.getEntitees()).thenReturn(entitySet);
		jdbcMetadataManagement.checkAndUpdateMetadata(metasegment, "testTableName", columnNamesList, metadataStore,"testdbName");
	}
	
	@Test
	public void testCheckAndUpdateMetadataWithNullMetasegment() throws MetadataAccessException {
		MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
		Entitee entity = new Entitee();
		entity.setDescription("testDescription");
		entity.setVersion(1.0);
		Set<Entitee> entitySet = new LinkedHashSet<Entitee>();
		entitySet.add(entity);
		when(metasegment.getEntitees()).thenReturn(entitySet);
		List<String> columnNamesList = new ArrayList<String>();
		columnNamesList.add("testColumnName1");
		jdbcMetadataManagement.checkAndUpdateMetadata(metasegment, "testTableName", columnNamesList, metadataStore,"testdbName");
	}

	@Test
	public void testCheckAndUpdatedMetadataWithException() throws Throwable {
		MetadataStore metadataStore = Mockito.mock(MetadataStore.class);
		when(metadataStore.getAdaptorMetasegment(anyString(), anyString(), anyString())).thenThrow(new MetadataAccessException(""));
		jdbcMetadataManagement.checkAndUpdateMetadata(metasegment, "testTableName", null, metadataStore,"testdbName");
	}
	
	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testSetNullColumnList() {
		jdbcMetadataManagement.setColumnList(jdbcInputDescriptor, null);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSetColumnList() {
		Attribute attribute = new Attribute();
		attribute.setAttributeName("testAttributeName");
		attribute.setAttributeType("testAttributeType");
		Set<Attribute> attributeSet = (Set<Attribute>) Mockito.mock(Set.class);
		attributeSet.add(attribute);
		Entitee entity = new Entitee();
		entity.setEntityName("testEntityname");
		entity.setDescription("testDescription");
		entity.setVersion(1.0);
		entity.setAttributes(attributeSet);
		Set<Entitee> entitySet = (Set<Entitee>) Mockito.mock(Set.class);
		entitySet.add(entity);
		Iterator<Entitee> entityIterator = Mockito.mock(Iterator.class);
		when(metasegment.getEntitees()).thenReturn(entitySet);
		when(entitySet.iterator()).thenReturn(entityIterator);
		when(entityIterator.hasNext()).thenReturn(true,false);
		when(entityIterator.next()).thenReturn(entity);
		
		Iterator<Attribute> attributeIterator = Mockito.mock(Iterator.class);
		when(Mockito.mock(Entitee.class).getAttributes()).thenReturn(attributeSet);
		when(attributeSet.iterator()).thenReturn(attributeIterator);
		when(attributeIterator.hasNext()).thenReturn(true,false);
		when(attributeIterator.next()).thenReturn(attribute);
		when(jdbcInputDescriptor.getIncrementedBy()).thenReturn("testAttributeName");
		List<String> columnList = Mockito.mock(List.class);
		when(jdbcInputDescriptor.getColumnList()).thenReturn(columnList);
		when(columnList.size()).thenReturn(2);
		jdbcMetadataManagement.setColumnList(jdbcInputDescriptor, metasegment);
	}
	
	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testSetColumnListWithNullEntitees() {
		when(metasegment.getEntitees()).thenReturn(null);
		jdbcMetadataManagement.setColumnList(jdbcInputDescriptor, metasegment);	
	}

}