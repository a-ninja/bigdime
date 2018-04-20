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
	public void testExtractDataWithDataTypeDECIMAL() throws DataAccessException, SQLException{
		when(resultSet.getMetaData()).thenReturn(resultSetMetadata);
		when(resultSetMetadata.getColumnCount()).thenReturn(1);
		when(resultSetMetadata.getColumnTypeName(anyInt())).thenReturn(JdbcConstants.NUMBER);
		when(resultSetMetadata.getPrecision(anyInt())).thenReturn(0);
		when(resultSetMetadata.getScale(anyInt())).thenReturn(1);
		Assert.assertNotNull(jdbcMetadata.extractData(resultSet));
	}
	
	@Test
	public void testExtractDataWithDataTypeNUMBER() throws DataAccessException, SQLException{
		when(resultSet.getMetaData()).thenReturn(resultSetMetadata);
		when(resultSetMetadata.getColumnCount()).thenReturn(1);
		when(resultSetMetadata.getColumnTypeName(anyInt())).thenReturn(JdbcConstants.NUMBER);
		when(resultSetMetadata.getPrecision(anyInt())).thenReturn(1);
		when(resultSetMetadata.getScale(anyInt())).thenReturn(1);
		Assert.assertNotNull(jdbcMetadata.extractData(resultSet));
	}
	
	@Test
	public void testExtractDataWithDataTypeBIGINT() throws DataAccessException, SQLException{
		when(resultSet.getMetaData()).thenReturn(resultSetMetadata);
		when(resultSetMetadata.getColumnCount()).thenReturn(1);
		when(resultSetMetadata.getColumnTypeName(anyInt())).thenReturn(JdbcConstants.NUMBER);
		when(resultSetMetadata.getScale(anyInt())).thenReturn(0);
		Assert.assertNotNull(jdbcMetadata.extractData(resultSet));
	}

	@Test
	public void testExtractDataWithDataTypeCHAR() throws DataAccessException, SQLException{
		when(resultSet.getMetaData()).thenReturn(resultSetMetadata);
		when(resultSetMetadata.getColumnCount()).thenReturn(1);
		when(resultSetMetadata.getColumnTypeName(anyInt())).thenReturn(JdbcConstants.CHAR);
		when(resultSetMetadata.getPrecision(anyInt())).thenReturn(300);
		Assert.assertNotNull(jdbcMetadata.extractData(resultSet));
	}
}
