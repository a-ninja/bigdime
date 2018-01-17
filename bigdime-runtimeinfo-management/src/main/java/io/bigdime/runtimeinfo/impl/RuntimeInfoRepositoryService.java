/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.impl;

import io.bigdime.alert.Logger;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;
import io.bigdime.runtimeinfo.DTO.RuntimePropertyDTO;
import io.bigdime.runtimeinfo.repositories.RuntimeInfoRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import java.util.List;

@Component
public class RuntimeInfoRepositoryService {

	private static Logger logger = LoggerFactory.getLogger(RuntimeInfoRepositoryService.class);

	private static final String SOURCENAME = "RUNTIME_INFO-API";

	@Autowired
	private RuntimeInfoRepository runtimeInfoRepository;

	public synchronized boolean delete(RuntimeInfoDTO adaptorRuntimeInfo) {
		boolean isCreatedOrUpdated = false;
		Assert.notNull(adaptorRuntimeInfo);
		RuntimeInfoDTO adaptorRuntimeInformation = runtimeInfoRepository
				.findByAdaptorNameAndEntityNameAndInputDescriptor(adaptorRuntimeInfo.getAdaptorName(),
						adaptorRuntimeInfo.getEntityName(), adaptorRuntimeInfo.getInputDescriptor());
		if (adaptorRuntimeInformation == null) {
			logger.info(SOURCENAME, "nothing to delete, not record found",
					"adaptorName: {} entityName: {} inputDescriptor: {}", adaptorRuntimeInfo.getAdaptorName(),
					adaptorRuntimeInfo.getEntityName(), adaptorRuntimeInfo.getInputDescriptor());
			adaptorRuntimeInfo.setCreatedAt();
			adaptorRuntimeInfo.setUpdatedAt();
			runtimeInfoRepository.save(adaptorRuntimeInfo);
			isCreatedOrUpdated = false;
		} else {
			runtimeInfoRepository.delete(adaptorRuntimeInformation);
			isCreatedOrUpdated = true;
		}
		return isCreatedOrUpdated;
	}

	public synchronized boolean create(RuntimeInfoDTO adaptorRuntimeInfo) {
		boolean isCreatedOrUpdated = false;
		Assert.notNull(adaptorRuntimeInfo);
		RuntimeInfoDTO adaptorRuntimeInformation = runtimeInfoRepository
				.findByAdaptorNameAndEntityNameAndInputDescriptor(adaptorRuntimeInfo.getAdaptorName(),
						adaptorRuntimeInfo.getEntityName(), adaptorRuntimeInfo.getInputDescriptor());
		if (adaptorRuntimeInformation == null) {
			logger.debug(SOURCENAME, "creating new Runtime Info entry",
					"Creaiting new Runtime Info enrty for adaptorName: {}", adaptorRuntimeInfo.getAdaptorName());
			adaptorRuntimeInfo.setCreatedAt();
			adaptorRuntimeInfo.setUpdatedAt();
			runtimeInfoRepository.save(adaptorRuntimeInfo);
			isCreatedOrUpdated = true;
		} else {
			RuntimeInfoDTO repoAdaptorRuntimeInformation = runtimeInfoRepository
					.findByAdaptorNameAndEntityNameAndInputDescriptor(adaptorRuntimeInfo.getAdaptorName(),
							adaptorRuntimeInfo.getEntityName(), adaptorRuntimeInfo.getInputDescriptor());

			repoAdaptorRuntimeInformation.setAdaptorName(adaptorRuntimeInfo.getAdaptorName());
			repoAdaptorRuntimeInformation.setEntityName(adaptorRuntimeInfo.getEntityName());
			repoAdaptorRuntimeInformation.setInputDescriptor(adaptorRuntimeInfo.getInputDescriptor());
			repoAdaptorRuntimeInformation.setNumOfAttempts(adaptorRuntimeInfo.getNumOfAttempts());
			repoAdaptorRuntimeInformation.setStatus(adaptorRuntimeInfo.getStatus());

			if (adaptorRuntimeInfo.getRuntimeProperties() != null) {
				for (RuntimePropertyDTO runtimePropertyDTO : adaptorRuntimeInfo.getRuntimeProperties()) {
					logger.debug(SOURCENAME, "Updating existing Runtime Info entry",
							"Updating existing Runtime Info entry for adaproName:{}",
							adaptorRuntimeInfo.getAdaptorName());
					// RuntimeInfoDTO repoAdaptorRuntimeInformation =
					// runtimeInfoRepository
					// .findByAdaptorNameAndEntityNameAndInputDescriptor(
					// adaptorRuntimeInfo.getAdaptorName(),
					// adaptorRuntimeInfo.getEntityName(),
					// adaptorRuntimeInfo.getInputDescriptor());
					//
					// repoAdaptorRuntimeInformation.setAdaptorName(adaptorRuntimeInfo.getAdaptorName());
					// repoAdaptorRuntimeInformation.setEntityName(adaptorRuntimeInfo.getEntityName());
					// repoAdaptorRuntimeInformation.setInputDescriptor(adaptorRuntimeInfo.getInputDescriptor());
					// repoAdaptorRuntimeInformation.setNumOfAttempts(adaptorRuntimeInfo.getNumOfAttempts());
					// repoAdaptorRuntimeInformation.setStatus(adaptorRuntimeInfo.getStatus());

					if (repoAdaptorRuntimeInformation.getRuntimeProperties() != null
							&& repoAdaptorRuntimeInformation.getRuntimeProperties().size() > 0) {
						for (RuntimePropertyDTO repoRuntimePropertyDTO : repoAdaptorRuntimeInformation
								.getRuntimeProperties()) {
							if (repoRuntimePropertyDTO.getKey().equalsIgnoreCase(runtimePropertyDTO.getKey())) {
								runtimePropertyDTO.setRuntimePropertyId(repoRuntimePropertyDTO.getRuntimePropertyId());
								repoAdaptorRuntimeInformation.getRuntimeProperties().remove(repoRuntimePropertyDTO);
								break;
							}
						}
					}

					repoAdaptorRuntimeInformation.getRuntimeProperties().add(runtimePropertyDTO);
					// repoAdaptorRuntimeInformation.setUpdatedAt();
					// runtimeInfoRepository.save(repoAdaptorRuntimeInformation);
					// isCreatedOrUpdated = true;
				}
			}
			repoAdaptorRuntimeInformation.setUpdatedAt();
			runtimeInfoRepository.save(repoAdaptorRuntimeInformation);
			isCreatedOrUpdated = true;

			// else {
			// logger.warn(SOURCENAME, "NOT updating existing Runtime Info
			// entry",
			// "BUG TO BE FIXED",
			// adaptorRuntimeInfo.getAdaptorName());
			// }
		}
		return isCreatedOrUpdated;

	}

	/*
	 * public boolean isExists(String adaptorName, String entityName) {
	 * 
	 * Assert.notNull(adaptorName); Assert.notNull(entityName);
	 * 
	 * List<RuntimeInfo> adaptorRuntimeInformationList = runtimeInfoRepository
	 * .findByAdaptorNameAndEntityName(adaptorName, entityName);
	 * 
	 * if (adaptorRuntimeInformationList == null ||
	 * adaptorRuntimeInformationList.size() == 0) return false; else return
	 * true;
	 * 
	 * }
	 */

	public List<RuntimeInfoDTO> get(String adaptorName, String entityName) {

		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		return runtimeInfoRepository.findByAdaptorNameAndEntityName(adaptorName, entityName);

	}

	public List<RuntimeInfoDTO> getByStartsWith(String adaptorName, String entityName, String descriptor) {

		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		Assert.notNull(descriptor);

		return runtimeInfoRepository.findByAdaptorNameAndEntityNameAndInputDescriptorStartsWith(adaptorName, entityName,
				descriptor);

	}
	public RuntimeInfoDTO get(String adaptorName, String entityName, String descriptor) {

		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		Assert.notNull(descriptor);

		return runtimeInfoRepository.findByAdaptorNameAndEntityNameAndInputDescriptor(adaptorName, entityName,
				descriptor);

	}

	public RuntimeInfoDTO getLatestRecord(String adaptorName, String entityName) {
		Assert.notNull(adaptorName);
		Assert.notNull(entityName);
		return runtimeInfoRepository.findFirstByAdaptorNameAndEntityNameOrderByRuntimeIdDesc(adaptorName, entityName);
	}
	
	public RuntimeInfoDTO getById(int runtimeInfoId) {
		return runtimeInfoRepository.findOne(runtimeInfoId);
	}

}