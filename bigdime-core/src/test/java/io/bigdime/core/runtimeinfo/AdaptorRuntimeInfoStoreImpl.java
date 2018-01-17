/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.runtimeinfo;

import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.springframework.stereotype.Component;


/**
 * Bigdime's implementation of RuntimeStore interface.
 * 
 * @author Neeraj Jain
 *
 * @param <RuntimeInfo>
 */
@Component
public class AdaptorRuntimeInfoStoreImpl implements RuntimeInfoStore<RuntimeInfo> {

	@Override
	public List<RuntimeInfo> getAll(String adaptorName, String entityName) throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}

	@Override
	public boolean put(RuntimeInfo adaptorRuntimeInfo) throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}

	@Override
	public List<RuntimeInfo> getAll(String adaptorName, String entityName, Status status)
			throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}

	@Override
	public List<RuntimeInfo> getAll(String adaptorName, String entityName, String inputDescriptor) throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}

	@Override
	public RuntimeInfo get(String adaptorName, String entityName, String descriptor) throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}

	@Override
	public RuntimeInfo getLatest(String adaptorName, String entityName) throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}

	@Override
	public boolean delete(RuntimeInfo runtimeInfo) throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}

	@Override
	public RuntimeInfo getById(int runtimeInfoId) throws RuntimeInfoStoreException {
		throw new NotImplementedException();
	}
}
