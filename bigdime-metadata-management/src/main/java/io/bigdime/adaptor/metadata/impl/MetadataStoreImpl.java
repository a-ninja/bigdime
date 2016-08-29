/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.adaptor.metadata.impl;

import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;

import io.bigdime.adaptor.metadata.MetadataAccessException;
import io.bigdime.adaptor.metadata.MetadataStore;
import io.bigdime.adaptor.metadata.ObjectEntityMapper;
import io.bigdime.adaptor.metadata.dto.EntiteeDTO;
import io.bigdime.adaptor.metadata.dto.MetasegmentDTO;
import io.bigdime.adaptor.metadata.model.Entitee;
import io.bigdime.adaptor.metadata.model.Metasegment;
import io.bigdime.alert.LoggerFactory;
import io.bigdime.alert.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Class MetadataStoreImpl is an implementation class for MATADATA-API which can
 * be accessed by any client. METADATA-API provides the flexibility to maintain
 * the adaptor meta data in the repository. In big-dime environment adaptor
 * maintains the meta data of its data by invoking these methods to store it in
 * repository and cache. Cache maintenance is implemented using HashMap
 * implementation. This returns the requested data if available in cache else
 * fetches from repository.Below are the operations currently available.
 * 
 * Insert - inserts the data into repository and cache. update - delete the
 * existing record and inserts new record in repository and cache. remove -
 * delete the record from repository and cache. get - get the request data if
 * available in cache or else from repository.
 * 
 * 
 * @author Neeraj Jain, Pavan Sabinikari
 * 
 * @version 1.0
 * 
 */
@Component
@Scope("prototype")
public class MetadataStoreImpl implements MetadataStore {

	private static Logger logger = LoggerFactory
			.getLogger(MetadataStoreImpl.class);

	@Autowired
	private MetadataRepositoryService repoService;

	private static final String SOURCENAME = "METADATA-API";

	private Map<String, Metasegment> cachedSchemaDetails = new HashMap<String, Metasegment>();
	private Set<String> entitees;
	private Map<String, Set<String>> adaptorEntities = new HashMap<String, Set<String>>();
	private List<Metasegment> adaptorSchemasList;

	@Autowired
	private ObjectEntityMapper objectEntityMapper;

	/**
	 * Load data into cache. This is a spring post construct method which loads
	 * the repository data into cache soon after the initialization of Spring.
	 * 
	 * @throws MetadataAccessException
	 */
	@PostConstruct
	public void loadCache() throws MetadataAccessException {

		logger.debug(SOURCENAME, "Start Loading cache",
				"Loading all the Metasegment with its associated schema details into cache");
		if (repoService.getAllSegments().isEmpty()) {
			logger.debug(
					SOURCENAME,
					"No metasegments available in repository to load into the cache",
					"Value from repository: {}", repoService.getAllSegments());
		} else

			for (Metasegment metasegment : objectEntityMapper
					.mapMetasegmentListObject(repoService.getAllSegments())) {
				loadCacheWithSchemaDetails(metasegment);

			}

		logger.debug(SOURCENAME, "Loading cache completed successfully",
				"Metasegments that were loaded are: {}",
				repoService.getAllSegments());
	}

	/**
	 * Below method loads cache(Hashmap) with key:
	 * adaptorName.schemaType.entityName and value: metasegment object.
	 * 
	 * @param metasegment
	 * @throws MetadataAccessException
	 */

	public void loadCacheWithSchemaDetails(Metasegment metasegment)
			throws MetadataAccessException {

		Assert.notNull(metasegment);
		if (metasegment.getEntitees() == null) {
			Metasegment metasegs = new Metasegment();

			metasegs.setAdaptorName(metasegment.getAdaptorName());
			metasegs.setSchemaType(metasegment.getSchemaType());
			metasegs.setDatabaseName(metasegment.getDatabaseName());
			metasegs.setDatabaseLocation(metasegment.getDatabaseLocation());
			metasegs.setDescription(metasegment.getDescription());
			metasegs.setRepositoryType(metasegment.getRepositoryType());
			metasegs.setCreatedAt(metasegment.getCreatedAt());
			metasegs.setCreatedBy(metasegment.getCreatedBy());
			metasegs.setUpdatedAt(metasegment.getUpdatedAt());
			metasegs.setUpdatedBy(metasegment.getUpdatedBy());

			cachedSchemaDetails.put(getKey(metasegment, ""), metasegs);
			loadAdaptorEntities(metasegment);

		} else {
			Assert.notNull(metasegment.getEntitees());
			for (Entitee entity : metasegment.getEntitees()) {

				Metasegment metasegs = new Metasegment();
				Set<Entitee> entitySet = new HashSet<Entitee>();
				entitySet.add(entity);
				metasegs.setAdaptorName(metasegment.getAdaptorName());
				metasegs.setSchemaType(metasegment.getSchemaType());
				metasegs.setDatabaseName(metasegment.getDatabaseName());
				metasegs.setDatabaseLocation(metasegment.getDatabaseLocation());
				metasegs.setDescription(metasegment.getDescription());
				metasegs.setRepositoryType(metasegment.getRepositoryType());
				metasegs.setCreatedAt(metasegment.getCreatedAt());
				metasegs.setCreatedBy(metasegment.getCreatedBy());
				metasegs.setUpdatedAt(metasegment.getUpdatedAt());
				metasegs.setUpdatedBy(metasegment.getUpdatedBy());
				metasegs.setEntitees(entitySet);
				cachedSchemaDetails.put(
						getKey(metasegment, entity.getEntityName()), metasegs);
				loadAdaptorEntities(metasegment);
			}
		}

		// Just printing values for test. will remove this statement later
		/*logger.debug(SOURCENAME,
				"Printing Cached Metasegments and its associated entity",
				"Listing below");
		for (Entry<String, Metasegment> entry : cachedSchemaDetails.entrySet()) {
			logger.debug(SOURCENAME, "Metasegment with its Entity details",
					"Key: {} ; Value: {}", entry.getKey(), entry.getValue());

		}*/

		// Just printing values for check. Will remove this statements later
		/*logger.debug(SOURCENAME,
				"Printing Cached Metasegment with associated entities list",
				"Listing below");
		for (Entry<String, Set<String>> entry : adaptorEntities.entrySet()) {
			logger.debug(SOURCENAME, "Metasegment with its entities list",
					"Key: {} ; Value: {}", entry.getKey(), entry.getValue());

		}*/
	}

	/**
	 * Load cache(HashMap) with key : adaptorName and value: Entity names set
	 * Throws MetadataAccessException in case of exception during the process.
	 * 
	 * @param metasegment
	 * @throws MetadataAccessException
	 */
	public void loadAdaptorEntities(Metasegment metasegment)
			throws MetadataAccessException {

		logger.debug(SOURCENAME, "Start Loading cache ",
				"Loading metadata with its associated entity list");
		if (adaptorEntities.containsKey(getAdaptorName(metasegment))
				&& !adaptorEntities.get(getAdaptorName(metasegment)).isEmpty()) {

			entitees = adaptorEntities.get(getAdaptorName(metasegment));
			for (Entitee entity : metasegment.getEntitees()) {

				entitees.add(entity.getEntityName().toLowerCase());

			}

		} else {
			entitees = new HashSet<String>();
			if (metasegment.getEntitees() != null)
				for (Entitee entity : metasegment.getEntitees()) {

					entitees.add(entity.getEntityName().toLowerCase());
				}
			adaptorEntities.put(getAdaptorName(metasegment).toLowerCase(),
					entitees);
		}
		logger.debug(SOURCENAME, "Loading cache completed",
				"Metasegment and entity list that were loaded are: {}",
				metasegment.getEntitees());
	}

	/**
	 * Removes entry from cache if exists.
	 * 
	 * @param metasegment
	 * @param deleteSegment
	 * @throws MetadataAccessException
	 */
	public void removeFromCache(Metasegment metasegment, boolean deleteSegment)
			throws MetadataAccessException {

		Assert.notNull(metasegment);
		Assert.notNull(metasegment.getAdaptorName());
		Assert.notNull(metasegment.getSchemaType());
		if (deleteSegment) {
			if (metasegment.getEntitees() != null)
				for (Entitee entity : metasegment.getEntitees()) {
					if (cachedSchemaDetails.containsKey(getKey(metasegment,
							entity.getEntityName())))
						logger.debug(
								SOURCENAME,
								"removing metasegment with associted entity details from cache",
								" value :{} ", cachedSchemaDetails.get(getKey(
										metasegment, entity.getEntityName())));
					cachedSchemaDetails.remove(getKey(metasegment,
							entity.getEntityName()));
				}
			else
				cachedSchemaDetails.remove(getKey(metasegment, ""));

		}

		if (adaptorEntities.containsKey(getAdaptorName(metasegment))
				&& !adaptorEntities.get(getAdaptorName(metasegment)).isEmpty()) {

			entitees = adaptorEntities.get(getAdaptorName(metasegment));
			for (Entitee entity : metasegment.getEntitees()) {

				if (entitees.contains(entity.getEntityName().toLowerCase()))
					entitees.remove(entity.getEntityName().toLowerCase());

			}

		}

	}

	/**
	 * Insert/Update details into repository and Cache
	 * 
	 * @param Metasegment
	 * @return void
	 * @throws MetadataAccessException
	 * 
	 * 
	 */
	public void put(Metasegment metasegment) throws MetadataAccessException {

		if (metasegment == null) {
			logger.warn(SOURCENAME,
					"Unable to insert/update metadata repository",
					"Provided value is: " + null);
			throw new IllegalArgumentException("Provided argument is null");
		} else {
			synchronized (this) {

				logger.debug(SOURCENAME,
						"Start insert/update metadata repository",
						"Provided metasegment details are {}", metasegment);

				if (repoService.schemaExists(objectEntityMapper
						.mapMetasegmentEntity(metasegment))) {
					logger.debug(SOURCENAME, "put metasegment",
							"Provided Metasegment and associated entities already exists in repository");

					if (repoService.checkUpdateEligibility(objectEntityMapper
							.mapMetasegmentEntity(metasegment))) {
						MetasegmentDTO metasegmentDTO = repoService
								.checkAttributesAssociation(objectEntityMapper
										.mapMetasegmentEntity(metasegment));
						//repoService.remove(objectEntityMapper
						//		.mapMetasegmentEntity(metasegment), false);
						// remove it from cache

						removeFromCache(
								objectEntityMapper
										.mapMetasegmentObject(metasegmentDTO),
								false);
						repoService.createOrUpdateMetasegment(metasegmentDTO,
								true);

						logger.debug(SOURCENAME, "put metasegment",
								"Updated schema details into datastore and cache successfully");
						loadCacheWithSchemaDetails(objectEntityMapper
								.mapMetasegmentObject(metasegmentDTO));
					} else
						logger.debug(SOURCENAME, "Update existing schema",
								"Not eligible to update due to version incompatibility");

				} else {
					logger.debug(SOURCENAME, "put metasegment",
							"Creating new entry in repository");
					repoService.createOrUpdateMetasegment(objectEntityMapper
							.mapMetasegmentEntity(metasegment), false);
					logger.debug(SOURCENAME, "",
							"Inserted schema details into datastore and cache successfully");
					loadCacheWithSchemaDetails(metasegment);
				}

			}
		}
	}

	/**
	 * Removes Metasegment details(associated entities and its corresponding
	 * attributes) that are available in repository. Throws
	 * MetadataAccessException in case of exception while processing.
	 * 
	 * @param metasegment
	 * @throws MetadataAccessException
	 * 
	 */
	public void remove(Metasegment metasegment) throws MetadataAccessException {
		if (metasegment == null) {
			logger.warn(SOURCENAME,
					"Remove Metasegment from metadata repository",
					"Provided value is: " + null);
			throw new IllegalArgumentException("provided argument is null");
		} else {
			repoService.remove(
					objectEntityMapper.mapMetasegmentEntity(metasegment), true);
			removeFromCache(metasegment, true);
		}

	}

	/**
	 * Get adaptor Metasegment details(associated entities and its corresponding
	 * attributes) that are available in repository. Returns null if adaptor
	 * does not exist. Throws MetadataAccessException during the execution.
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @param entityName
	 * @return Metasegment object if exists else null
	 * @throws metadataAccessException
	 */

	@Override
	public Metasegment getAdaptorMetasegment(String adaptorName,
			String schemaType, String entityName)
			throws MetadataAccessException {
		if (cachedSchemaDetails.containsKey(adaptorName.toLowerCase() + "."
				+ schemaType.toLowerCase() + "." + entityName.toLowerCase())) {
			logger.debug(
					SOURCENAME,
					"Getting data from cache",
					"Given key: {}.{}.{} ; value found: {}",
					adaptorName.toLowerCase(),
					schemaType.toLowerCase(),
					entityName.toLowerCase(),
					cachedSchemaDetails.get(adaptorName.toLowerCase() + "."
							+ schemaType.toLowerCase() + "."
							+ entityName.toLowerCase()));

			return cachedSchemaDetails
					.get(adaptorName.toLowerCase() + "."
							+ schemaType.toLowerCase() + "."
							+ entityName.toLowerCase());
		}
		logger.debug(SOURCENAME, "Getting MetaSchema from repository",
				"Given key: {}.{}.{} ; value found: {}",
				adaptorName.toLowerCase(), schemaType.toLowerCase(),
				entityName.toLowerCase(),
				repoService.getSchema(adaptorName, schemaType, entityName));

		return objectEntityMapper.mapMetasegmentObject(repoService.getSchema(
				adaptorName, schemaType, entityName));
	}

	/**
	 * Get adaptor Metaschema details that are available in repository. Returns
	 * null if entity is not associated to a given adaptor.
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @param entityName
	 * @return Metasegment object if exists else null
	 * @throws metadataAccessException
	 */
	@Override
	public Entitee getAdaptorEntity(String adaptorName, String schemaType,
			String entityName) throws MetadataAccessException {
		if (StringUtils.hasText(adaptorName) && StringUtils.hasText(schemaType)
				&& StringUtils.hasText(entityName)) {
			Metasegment metasegment = getAdaptorMetasegment(adaptorName,
					schemaType, entityName);
			logger.debug(SOURCENAME, "Get adaptor Entity",
					"Metasegment from repository: {}", metasegment);
			if (metasegment == null) {
				logger.warn(SOURCENAME, "Getting adatper schema Details",
						"adaptor: {} is not available in the repository",
						adaptorName);

			} else {
				if(metasegment.getEntitees() != null)
				for (final Entitee entity : metasegment.getEntitees())
					return entity;
			}
		} else
			logger.warn(SOURCENAME, "Get adaptor Entity",
					"provided adaptorName or schemaType or entityName is null or empty");

		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Get Entities list associate to an adaptor and schemaType. Return empty
	 * Set if no entity exists for a given adaptor.
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @return Set of Entities Object if exists else empty Set
	 */
	@Override
	public Set<Entitee> getAdaptorEntities(String adaptorName, String schemaType)
			throws MetadataAccessException {
		// TODO Auto-generated method stub
		logger.debug(SOURCENAME, "Getting adaptorEntities from repository",
				"Given key: {}.{} ; value found: {}", adaptorName, schemaType);
		return objectEntityMapper.mapEntiteeSetObject(repoService
				.getAllEntites(adaptorName, schemaType));
	}

	/**
	 * Gets distinct data sources if exists. If no data sources exists then
	 * return empty Set
	 * 
	 * @return list of data Sources if exists else empty Set
	 */
	public Set<String> getDataSources() throws MetadataAccessException {
		logger.debug(SOURCENAME,
				"Getting distinct data sources from repository",
				"Data Sources List:{}", repoService.getDistinctDataSources());
		Set<String> dataSources = repoService.getDistinctDataSources();
		if (dataSources.isEmpty())
			logger.warn(SOURCENAME, "get data sources",
					"No Data souces found, Data Source List: " + null);
		return dataSources;
	}

	/**
	 * Get list of Metasegments along with associated entities for a given
	 * adaptor
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @return list of Metasegment if exists else null object
	 * @throws MetadataAccessException
	 */
	@Override
	public List<Metasegment> getAdaptorMetasegments(String adaptorName,
			String schemaType) throws MetadataAccessException {
		adaptorSchemasList = new ArrayList<Metasegment>();
		logger.debug(
				SOURCENAME,
				"Getting adaptorMetasegments from cache",
				"Associated entities for a given adaptorName: {} and schemType : {} are : {}   ",
				adaptorName,
				schemaType,
				adaptorEntities.get(adaptorName.toLowerCase() + "."
						+ schemaType.toLowerCase()));
		entitees = adaptorEntities.get(adaptorName.toLowerCase() + "."
				+ schemaType.toLowerCase());
		if (entitees == null)
			logger.warn(SOURCENAME, "adaptor is not available",
					"adaptor : {} with schemaType: {} is not available",
					adaptorName, schemaType);
		if (entitees != null)
			for (String entityName : entitees)
				adaptorSchemasList.add(cachedSchemaDetails.get(adaptorName
						.toLowerCase()
						+ "."
						+ schemaType.toLowerCase()
						+ "."
						+ entityName.toLowerCase()));
		return adaptorSchemasList;
	}

	/**
	 * Get List of Entities for a given adaptor
	 * 
	 * @param adaptorName
	 * @param schemaType
	 * @return Entity List of String if exists else null
	 */
	@Override
	public Set<String> getAdaptorEntityList(String adaptorName,
			String schemaType) throws MetadataAccessException {
		logger.debug(
				SOURCENAME,
				"Getting adaptor entity list from cache",
				"adaptorName: {} with schemaType: {} and associated entities are: {}",
				adaptorName,
				schemaType,
				adaptorEntities.get(adaptorName.toLowerCase() + "."
						+ schemaType.toLowerCase()));
		return adaptorEntities.get(adaptorName.toLowerCase() + "."
				+ schemaType.toLowerCase());
	}

	/**
	 * Key formation for a cached metasegment.
	 * 
	 * @param metasegment
	 * @return Key string
	 * @throws MetadataAccessException
	 */
	public String getKey(Metasegment metasegment, String entityName) {
		logger.debug(SOURCENAME,
				"creating  unique cache key for Metadatasegments ",
				" Unique key is: {}", metasegment.getAdaptorName()
						.toLowerCase()
						+ "."
						+ metasegment.getSchemaType().toLowerCase()
						+ "."
						+ entityName.toLowerCase());
		return metasegment.getAdaptorName().toLowerCase() + "."
				+ metasegment.getSchemaType().toLowerCase() + "."
				+ entityName.toLowerCase();
	}

	/**
	 * Key formation cached entities list.
	 * 
	 * @param metasegment
	 * @return Key String
	 * @throws MetadataAccessException
	 */
	public String getAdaptorName(Metasegment metasegment) {
		logger.debug(SOURCENAME, "creating unique key for adaptor entity list",
				"Unique key is: {}", metasegment.getAdaptorName().toLowerCase()
						+ "." + metasegment.getSchemaType().toLowerCase());
		return metasegment.getAdaptorName().toLowerCase() + "."
				+ metasegment.getSchemaType().toLowerCase();
	}

	/**
	 * This method is useful to register the adaptor details.
	 * 
	 * return list of metasegment if exists else null
	 */
	@Override
	public List<Metasegment> createDatasourceIfNotExist(String adaptorName,
			String schemaType) throws MetadataAccessException {
		synchronized (this) {
			List<MetasegmentDTO> metasegmentsDTO = repoService
					.getAllSegments(adaptorName);
			if (metasegmentsDTO.size() == 0) {
				final MetasegmentDTO metaSegmentDTO = new MetasegmentDTO();
				metaSegmentDTO.setAdaptorName(adaptorName);
				metaSegmentDTO.setIsDataSource("Y");
				metaSegmentDTO.setSchemaType(schemaType);
				metaSegmentDTO.setCreatedAt(new Date());
				metaSegmentDTO.setUpdatedAt(new Date());
				metaSegmentDTO.setEntitees(new HashSet<EntiteeDTO>());
				logger.info(SOURCENAME, "inserting new metasegment",
						"metasegment.adaptorName={}",
						metaSegmentDTO.getAdaptorName());
				put(objectEntityMapper.mapMetasegmentObject(metaSegmentDTO));
				metasegmentsDTO = new ArrayList<>();
				metasegmentsDTO.add(metaSegmentDTO);
			}
			return objectEntityMapper.mapMetasegmentListObject(metasegmentsDTO);
		}
	}

}