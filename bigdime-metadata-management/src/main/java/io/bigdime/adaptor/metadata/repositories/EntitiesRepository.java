/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.repositories;

import io.bigdime.adaptor.metadata.dto.EntiteeDTO;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Class EntitiesRepository
 * 
 * This is a repository for Entities which extends JpaRepository. It provides an
 * elegant way to access the Entity table available in repository.
 * 
 * @author Neeraj Jain, psabinikari
 * @version 1.0
 * 
 */
@Transactional
public interface EntitiesRepository extends JpaRepository<EntiteeDTO, Integer> {

	EntiteeDTO findByIdAndEntityName(int id, String entityName);
//	EntiteeDTO findByAdaptorNameAndEntityName(String adaptorName, String entityName);

}