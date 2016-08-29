/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Mock;
import org.mockito.Mockito;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.Mockito.*;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.bigdime.runtimeinfo.impl.RuntimeInfoRepositoryService;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;
import io.bigdime.runtimeinfo.repositories.RuntimeInfoRepository;

public class RuntimeInfoRepositoryServiceTest {

	@Mock
	RuntimeInfoDTO mockRuntimeInfo;

	RuntimeInfoRepositoryService runtimeInfoRepositoryServce;

	@Mock
	RuntimeInfoRepository mockRuntimeInfoRepository;
	
	@Mock
	RuntimePropertyDTO mockRuntimePropertyDTO;
	
	

	@BeforeClass
	public void init() {
		initMocks(this);
		runtimeInfoRepositoryServce = new RuntimeInfoRepositoryService();
		ReflectionTestUtils.setField(runtimeInfoRepositoryServce,
				"runtimeInfoRepository", mockRuntimeInfoRepository);
		when(mockRuntimeInfo.getAdaptorName()).thenReturn("testAdaptorName");
		when(mockRuntimeInfo.getEntityName()).thenReturn("testEntityName");
		when(mockRuntimeInfo.getInputDescriptor()).thenReturn("testDescriptor");
	}
	
	@Test
	public void testCreate() {
		when(
				mockRuntimeInfoRepository
						.findByAdaptorNameAndEntityNameAndInputDescriptor(
								mockRuntimeInfo.getAdaptorName(), mockRuntimeInfo.getEntityName(), mockRuntimeInfo.getInputDescriptor()))
				.thenReturn(null);
		Assert.assertTrue(runtimeInfoRepositoryServce.create(mockRuntimeInfo));
	}

	@Test
	public void testUpdate() {
		

		when(
				mockRuntimeInfoRepository
						.findByAdaptorNameAndEntityNameAndInputDescriptor(
								mockRuntimeInfo.getAdaptorName(), mockRuntimeInfo.getEntityName(), mockRuntimeInfo.getInputDescriptor()))
				.thenReturn(Mockito.mock(RuntimeInfoDTO.class));
		Set<RuntimePropertyDTO> runtimePropertyDTO = new HashSet<RuntimePropertyDTO>();
		runtimePropertyDTO.add(mockRuntimePropertyDTO);
		when(mockRuntimeInfo.getRuntimeProperties()).thenReturn(runtimePropertyDTO);
		
		Assert.assertTrue(runtimeInfoRepositoryServce.create(mockRuntimeInfo));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetAdapterEntries() {
		
		when(mockRuntimeInfoRepository.findByAdaptorNameAndEntityName(anyString(), anyString())).thenReturn(Mockito.mock(List.class));
		Assert.assertNotNull(runtimeInfoRepositoryServce.get("testAdaptorName", "testEntityName"));
	}
	
	@Test
	public void testGetAdaptorDescriptorEntry() {
		when(mockRuntimeInfoRepository.findByAdaptorNameAndEntityNameAndInputDescriptor(anyString(), anyString(), anyString())).thenReturn(mockRuntimeInfo);
		Assert.assertNotNull(runtimeInfoRepositoryServce.get("testAdaptorName", "testEntityName","testDescriptor"));
	}
}