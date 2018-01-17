/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.repositories;

import java.util.List;

import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;
import io.bigdime.runtimeinfo.DTO.RuntimeInfoDTO;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

/**
 * Class RuntimeInfoRepository
 *
 * This repository interface which extends JpaRepository behaves as a DAO object.
 * It provides an elegant way to access the runtime-info tables available in the repository.
 *
 * @author Neeraj Jain, psabinikari
 * @version 1.0
 *
 */

@Transactional
public interface RuntimeInfoRepository extends
		JpaRepository<RuntimeInfoDTO, Integer> {

	List<RuntimeInfoDTO> findByAdaptorNameAndEntityName(String adaptorName,
			String entityName);

	RuntimeInfoDTO findByAdaptorNameAndEntityNameAndStatus(String adaptorName,
			String entityName, Status status);

	RuntimeInfoDTO findByAdaptorNameAndEntityNameAndInputDescriptor(
			String adaptorName, String entityName, String descriptor);

	List<RuntimeInfoDTO> findByAdaptorNameAndEntityNameAndInputDescriptorStartsWith(
			String adaptorName, String entityName, String descriptor);

	RuntimeInfoDTO findFirstByAdaptorNameAndEntityNameOrderByUpdatedAtDesc(String adaptorName,String entityName);
	RuntimeInfoDTO findFirstByAdaptorNameAndEntityNameOrderByRuntimeIdDesc(String adaptorName,String entityName);

}
