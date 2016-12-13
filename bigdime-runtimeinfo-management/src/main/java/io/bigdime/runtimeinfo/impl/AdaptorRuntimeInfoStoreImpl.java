/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.core.runtimeinfo.RuntimeInfo;
import io.bigdime.core.runtimeinfo.RuntimeInfoStore;
import io.bigdime.core.runtimeinfo.RuntimeInfoStoreException;
import io.bigdime.runtime.ObjectEntityMapper;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;


/**
 * Bigdime's implementation of RuntimeStore interface.
 * 
 * @author Neeraj Jain, Pavan Sabinikari
 * 
 * @param <T>
 */

@Component
public class AdaptorRuntimeInfoStoreImpl implements RuntimeInfoStore<RuntimeInfo> {

	private static Logger logger = LoggerFactory.getLogger(AdaptorRuntimeInfoStoreImpl.class);

	private static final String SOURCENAME = "RUNTIME_INFO-API";

	@Autowired
	private RuntimeInfoRepositoryService runtimeInfoRepositoryService;

	@Autowired
	@Qualifier("RuntimeInfoObjectEntityMapper")
	ObjectEntityMapper objectEntityMapper;

	@Override
	public List<RuntimeInfo> getAll(String adaptorName, String entityName) throws RuntimeInfoStoreException {
		if (adaptorName == null || entityName == null) {
			logger.warn(SOURCENAME, "get all entry",
					"Unable to get entries for adaptorName: {}, entityName: {} due to invalid arguments", adaptorName,
					entityName);
			throw new IllegalArgumentException("Provided argument is not valid");
		} else {
			List<RuntimeInfoDTO> runtimeInfoDTOList = runtimeInfoRepositoryService.get(adaptorName, entityName);
			if (runtimeInfoDTOList.size() > 0) {
				return objectEntityMapper.mapObjectList(runtimeInfoDTOList);
			}

		}
		return null;
	}

	@Override
	public synchronized boolean delete(RuntimeInfo adaptorRuntimeInfo) {
		if (adaptorRuntimeInfo == null) {
			logger.warn(SOURCENAME, "delete entry", "Unable to delete entry due to invalid arguments");
			throw new IllegalArgumentException("Provided argument is not valid");
		} else {
			RuntimeInfoDTO runtimeInfoDTO = runtimeInfoRepositoryService.get(adaptorRuntimeInfo.getAdaptorName(),
					adaptorRuntimeInfo.getEntityName(), adaptorRuntimeInfo.getInputDescriptor());
			return runtimeInfoRepositoryService.delete(runtimeInfoDTO);
		}
	}

	@Override
	public synchronized boolean put(RuntimeInfo adaptorRuntimeInfo) throws RuntimeInfoStoreException {
		if (adaptorRuntimeInfo == null) {
			logger.warn(SOURCENAME, "put entry", "Unable to create entry due to invalid arguments");
			throw new IllegalArgumentException("Provided argument is not valid");
		} else {
			RuntimeInfoDTO runtimeInfoDTO = objectEntityMapper.mapEntityObject(adaptorRuntimeInfo);
			return runtimeInfoRepositoryService.create(runtimeInfoDTO);
		}

	}

	@Override
	public List<RuntimeInfo> getAll(String adaptorName, String entityName, Status status)
			throws RuntimeInfoStoreException {
		List<RuntimeInfo> runtimeInfoList = null;
		if (adaptorName == null || entityName == null || status == null) {
			logger.warn(SOURCENAME, "get all entry",
					"Unable to get entries for adaptorName: {}, entityName: {}, status: {} due to invalid arguments",
					adaptorName, entityName, status);
			throw new IllegalArgumentException("Provided argument is not valid");
		} else {
			runtimeInfoList = new ArrayList<RuntimeInfo>();
			for (RuntimeInfoDTO adaptorRuntimeInformationDTO : runtimeInfoRepositoryService.get(adaptorName,
					entityName))
				if (adaptorRuntimeInformationDTO.getStatus().equals(status))
					runtimeInfoList.add(objectEntityMapper.mapObject(adaptorRuntimeInformationDTO));
		}
		return runtimeInfoList;
	}

	@Override
	public List<RuntimeInfo> getAll(String adaptorName, String entityName, String inputDescriptorPrefix)
			throws RuntimeInfoStoreException {

		List<RuntimeInfo> runtimeInfoList = null;
		if (adaptorName == null || entityName == null || inputDescriptorPrefix == null) {
			logger.warn(SOURCENAME, "get all entry",
					"Unable to get entries for adaptorName: {}, entityName: {}, inputDescriptorStatus: {} due to invalid arguments",
					adaptorName, entityName, inputDescriptorPrefix);
			throw new IllegalArgumentException("Provided argument is not valid");
		} else {
			runtimeInfoList = new ArrayList<RuntimeInfo>();
			for (RuntimeInfoDTO adaptorRuntimeInformationDTO : runtimeInfoRepositoryService.getByStartsWith(adaptorName,
					entityName, inputDescriptorPrefix))
				runtimeInfoList.add(objectEntityMapper.mapObject(adaptorRuntimeInformationDTO));
		}
		return runtimeInfoList;

	}

	@Override
	public RuntimeInfo get(String adaptorName, String entityName, String descriptor) throws RuntimeInfoStoreException {
		if (adaptorName == null || entityName == null || descriptor == null) {
			logger.warn(SOURCENAME, "get entry",
					"Unable to get entry due to invalid arguments adaptorName={}, entityName={},descriptor={}",
					adaptorName, entityName, descriptor);
			throw new IllegalArgumentException("Provided argument are not valid");

		} else {
			return objectEntityMapper.mapObject(runtimeInfoRepositoryService.get(adaptorName, entityName, descriptor));

		}

	}

	@Override
	public RuntimeInfo getLatest(String adaptorName, String entityName) throws RuntimeInfoStoreException {
		if (adaptorName == null || entityName == null) {
			logger.warn(SOURCENAME, "get all entry",
					"Unable to get entries for adaptorName: {}, entityName: {} due to invalid arguments", adaptorName,
					entityName);
			throw new IllegalArgumentException("Provided argument is not valid");
		} else
			return objectEntityMapper.mapObject(runtimeInfoRepositoryService.getLatestRecord(adaptorName, entityName));
	}

	@Override
	public RuntimeInfo getById(int runtimeInfoId) throws RuntimeInfoStoreException {
		return objectEntityMapper.mapObject(runtimeInfoRepositoryService.getById(runtimeInfoId));
	}

}