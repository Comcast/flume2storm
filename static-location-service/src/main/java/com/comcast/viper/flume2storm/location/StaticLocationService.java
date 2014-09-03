/**
 * Copyright 2014 Comcast Cable Communications Management, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.comcast.viper.flume2storm.location;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * A simple implementation of LocationService that gets the list of
 * ServiceProviders from a file. No subsequent ServiceProvider
 * registration/unregistration is permitted.
 * 
 * @param SP
 *            The actual class for the ServiceProvider
 */
public class StaticLocationService<SP extends ServiceProvider<?>> extends AbstractLocationService<SP> {
	// private static final long serialVersionUID = -7763531231393422301L;
	public static final String SERVICE_PROVIDER_BASE = "service.providers";
	public static final String SERVICE_PROVIDER_BASE_SEPARATOR = " ";
	private static final Logger LOG = LoggerFactory.getLogger(StaticLocationService.class);
	private final ServiceProviderSerialization<SP> serialization;

	public StaticLocationService(ServiceProviderConfigurationLoader<SP> spLoader, Configuration configuration, String nameSpace,
			final ServiceProviderSerialization<SP> ser) {
		Configuration spBaseConfig = configuration.subset(nameSpace);
		String spString = spBaseConfig.getString(SERVICE_PROVIDER_BASE);
		LOG.trace("Service providers to load: {}", spString);
		Configuration spConfig = spBaseConfig.subset(SERVICE_PROVIDER_BASE);
		Builder<SP> spBuilders = ImmutableList.builder();
		for (String spName : StringUtils.split(spString, SERVICE_PROVIDER_BASE_SEPARATOR)) {
			LOG.trace("Loading service provider: {}", spName);
			Configuration providerConfig = spConfig.subset(spName);
			SP serviceProvider = spLoader.load(providerConfig);
			if (serviceProvider != null) {
				LOG.debug("Loaded service provider {}: {}", spName, serviceProvider);
				spBuilders.add(serviceProvider);
			} else {
				LOG.error("Failed to load service provider identified with {}", spName);
			}
		}
		serviceProviderManager.set(spBuilders.build());
		this.serialization = ser;
	}

	/**
	 * @see com.comcast.viper.flume2storm.location.LocationService#start()
	 */
	@Override
	public boolean start() {
		// Nothing to do
		return true;
	}

	/**
	 * @see com.comcast.viper.flume2storm.location.LocationService#stop()
	 */
	@Override
	public boolean stop() {
		// Nothing to do
		return true;
	}

	/**
	 * @see com.comcast.viper.flume2storm.location.LocationService#register(com.comcast.viper.flume2storm.location.ServiceProvider)
	 */
	public void register(SP serviceProvider) {
		throw new NotImplementedException("StaticLocationService cannot register ServiceProvider dynamically");
	}

	/**
	 * @see com.comcast.viper.flume2storm.location.LocationService#unregister(com.comcast.viper.flume2storm.location.ServiceProvider)
	 */
	public void unregister(SP serviceProvider) {
		throw new NotImplementedException("StaticLocationService cannot unregister ServiceProvider dynamically");
	}

	/**
	 * @see com.comcast.viper.flume2storm.location.LocationService#getSerialization()
	 */
	public ServiceProviderSerialization<SP> getSerialization() {
		return serialization;
	}
}
