/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.stereotype.Component;

import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.runtime.ObjectEntityMapper;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;

@Component("RuntimeInfoObjectEntityMapper")
public class ObjectEntityMapperImpl implements ObjectEntityMapper {

	@Override
	public List<RuntimeInfo> mapObjectList(
			List<RuntimeInfoDTO> runtimeInfoDTOList) {
		// TODO Auto-generated method stub
		if (runtimeInfoDTOList.size() > 0) {
			Map<String, String> properties = new HashMap<String, String>();
			List<RuntimeInfo> runtimeInfoList = new ArrayList<RuntimeInfo>();
			for (RuntimeInfoDTO runtimeInfoDTO : runtimeInfoDTOList) {
				RuntimeInfo runtimeInfo = new RuntimeInfo();
				runtimeInfo.setRuntimeId(Integer.toString(runtimeInfoDTO
						.getRuntimeId()));
				runtimeInfo.setAdaptorName(runtimeInfoDTO.getAdaptorName());
				runtimeInfo.setEntityName(runtimeInfoDTO.getEntityName());
				runtimeInfo.setInputDescriptor(runtimeInfoDTO
						.getInputDescriptor());
				runtimeInfo.setStatus(runtimeInfoDTO.getStatus());
				if (runtimeInfoDTO.getRuntimeProperties().size() > 0) {
					for (RuntimePropertyDTO runtimePropertyDTO : runtimeInfoDTO
							.getRuntimeProperties())
						properties.put(runtimePropertyDTO.getKey(),
								runtimePropertyDTO.getValue());
					runtimeInfo.setProperties(properties);
				}
				runtimeInfoList.add(runtimeInfo);
			}
			return runtimeInfoList;
		}
		return null;

	}

	@Override
	public RuntimeInfo mapObject(RuntimeInfoDTO runtimeInfoDTO) {
		if (runtimeInfoDTO != null) {
			RuntimeInfo runtimeInfo = new RuntimeInfo();
			runtimeInfo.setRuntimeId(Integer.toString(runtimeInfoDTO
					.getRuntimeId()));
			runtimeInfo.setAdaptorName(runtimeInfoDTO.getAdaptorName());
			runtimeInfo.setEntityName(runtimeInfoDTO.getEntityName());
			runtimeInfo.setInputDescriptor(runtimeInfoDTO.getInputDescriptor());
			Map<String, String> properties = new HashMap<String, String>();
			runtimeInfo.setStatus(runtimeInfoDTO.getStatus());
			if (runtimeInfoDTO.getRuntimeProperties().size() > 0) {
				for (RuntimePropertyDTO runtimePropertyDTO : runtimeInfoDTO
						.getRuntimeProperties())
					properties.put(runtimePropertyDTO.getKey(),
							runtimePropertyDTO.getValue());
				runtimeInfo.setProperties(properties);
			}
			return runtimeInfo;
		}
		return null;
	}

	@Override
	public RuntimeInfoDTO mapEntityObject(RuntimeInfo runtimeInfo) {
		if (runtimeInfo != null) {
			RuntimeInfoDTO runtimeInfoDTO = new RuntimeInfoDTO();
			runtimeInfoDTO.setAdaptorName(runtimeInfo.getAdaptorName());
			runtimeInfoDTO.setEntityName(runtimeInfo.getEntityName());
			runtimeInfoDTO.setInputDescriptor(runtimeInfo.getInputDescriptor());
			runtimeInfoDTO.setStatus(runtimeInfo.getStatus());
			Set<RuntimePropertyDTO> runtimePropertyDTOSet = new HashSet<RuntimePropertyDTO>();
			if (runtimeInfo.getProperties() != null
					&& runtimeInfo.getProperties().entrySet() != null) {
				for (Entry<String, String> entry : runtimeInfo.getProperties()
						.entrySet()) {
					RuntimePropertyDTO runtimePropertyDTO = new RuntimePropertyDTO();
					runtimePropertyDTO.setKey(entry.getKey());
					runtimePropertyDTO.setValue(entry.getValue());
					runtimePropertyDTOSet.add(runtimePropertyDTO);
				}
			}
			if (runtimePropertyDTOSet.size() > 0)
				runtimeInfoDTO.setRuntimeProperties(runtimePropertyDTOSet);
			return runtimeInfoDTO;
		}
		return null;
	}

}