/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.runtimeinfo.DTO;


import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.springframework.stereotype.Component;

import io.bigdime.core.runtimeinfo.RuntimeInfoStore.Status;

/**
 * Encapsulates information that needs to be serialized by adaptor by the
 * RuntimeInfo store.
 * 
 * @author Neeraj Jain
 *
 */
@Entity
@Table(name = "RUNTIME_INFO")
@Component
public class RuntimeInfoDTO {

	@Column(name = "RUNTIME_INFO_ID")
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int runtimeId;
	@Column(name = "ADAPTOR_NAME")
	private String adaptorName;
	@Column(name = "ENTITY_NAME")
	private String entityName;
	@Column(name = "INPUT_DESCRIPTOR")
	private String inputDescriptor;
	@Column(name = "STATUS")
	@Enumerated(EnumType.STRING)
	private Status status;
	@Column(name = "NUM_OF_ATTEMPTS")
	private int numOfAttempts;
	/**
	 * Maps Adaptor to Runtime properties.
	 */
	@OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER,orphanRemoval=true)
	@JoinColumn(name = "RUNTIME_INFO_ID")
	private Set<RuntimePropertyDTO> runtimePropertiesDTO = new HashSet<RuntimePropertyDTO>();

	@Column(name = "CREATED_AT")
	private Date createdAt;
	
	

	@Column(name = "CREATED_BY")
	private String createdBy;
	
	@Column(name = "UPDATED_AT")
	private Date updatedAt;
	
	@Column(name = "UPDATED_BY")
	private String updatedBy;
     
	

	

	public RuntimeInfoDTO() {
		super();
		
	}

	public RuntimeInfoDTO(String adaptorName, String entityName,
			String inputDescriptor, Status status, int numOfAttempts,
			Set<RuntimePropertyDTO> runtimeProperties) {
		super();
		this.adaptorName = adaptorName;
		this.entityName = entityName;
		this.inputDescriptor = inputDescriptor;
		this.status = status;
		this.numOfAttempts = numOfAttempts;
		this.runtimePropertiesDTO = runtimeProperties;
	}

	public int getRuntimeId() {
		return runtimeId;
	}

	public void setRuntimeId(int runtimeId) {
		this.runtimeId = runtimeId;
	}

	public String getAdaptorName() {
		return adaptorName;
	}

	public void setAdaptorName(String adaptorName) {
		this.adaptorName = adaptorName;
	}

	public String getEntityName() {
		return entityName;
	}

	public void setEntityName(String entityName) {
		this.entityName = entityName;
	}

	public String getInputDescriptor() {
		return inputDescriptor;
	}

	public void setInputDescriptor(String inputDescriptor) {
		this.inputDescriptor = inputDescriptor;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}
	
	public int getNumOfAttempts() {
		return numOfAttempts;
	}

	public void setNumOfAttempts(int numOfAttempts) {
		this.numOfAttempts = numOfAttempts;
	}
	
	public Set<RuntimePropertyDTO> getRuntimeProperties() {
		return runtimePropertiesDTO;
	}

	public void setRuntimeProperties(Set<RuntimePropertyDTO> runtimePropertiesDTO) {
		this.runtimePropertiesDTO = runtimePropertiesDTO;
	}
	
	
	public Date getCreatedAt() {
		return (Date) createdAt.clone();
	}

	public void setCreatedAt() {
		this.createdAt = new Date();
	}
	
	
	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	public Date getUpdatedAt() {
		
		return (Date) updatedAt.clone();
	}

	public void setUpdatedAt() {
		this.updatedAt = new Date();
	}

	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	
	

}