/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.handler.jdbc;

import org.mockito.Mock;

import static org.mockito.MockitoAnnotations.initMocks;

import org.springframework.dao.DataAccessException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcMetadataTest {
	
	JdbcMetadata jdbcMetadata;
	
	@Mock
	JdbcInputDescriptor jdbcInputDescriptor;
	@Mock
	ResultSet resultSet;
	
	@Mock
	ResultSetMetaData resultSetMetadata;
	
	@BeforeClass
	public void init() {
		
		initMocks(this);
		jdbcMetadata = new JdbcMetadata(jdbcInputDescriptor);
		
	}
	
	@Test
	public void testExtractData() throws DataAccessException, SQLException{
		when(resultSet.getMetaData()).thenReturn(resultSetMetadata);
		when(resultSetMetadata.getColumnCount()).thenReturn(1);
		Assert.assertNotNull(jdbcMetadata.extractData(resultSet));
	}

}
