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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.comcast.viper.flume2storm.F2SConfigurationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

/**
 * An implementation of LocationService that gets the list of ServiceProviders
 * from a static configuration. No subsequent ServiceProvider
 * registration/unregistration is permitted.
 * 
 * @param <SP>
 *          The actual class for the ServiceProvider
 */
public class StaticLocationService<SP extends ServiceProvider<?>> extends AbstractLocationService<SP> {
  private static final Logger LOG = LoggerFactory.getLogger(StaticLocationService.class);
  private final ServiceProviderSerialization<SP> serialization;

  /**
   * @param configuration
   *          The configuration that contains the {@link ServiceProvider}
   * @param serialization
   *          The {@link ServiceProviderSerialization} to use
   * @throws F2SConfigurationException
   *           If the configuration is invalid
   */
  public StaticLocationService(Configuration configuration, final ServiceProviderSerialization<SP> serialization)
      throws F2SConfigurationException {
    this.serialization = serialization;
    StaticLocationServiceConfiguration staticLocationServiceConfig = StaticLocationServiceConfiguration
        .from(configuration);

    String configLoaderClassName = staticLocationServiceConfig.getConfigurationLoaderClassName();
    ServiceProviderConfigurationLoader<SP> spLoader = null;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends ServiceProviderConfigurationLoader<SP>> loaderFactoryClass = (Class<? extends ServiceProviderConfigurationLoader<SP>>) Class
          .forName(configLoaderClassName);
      spLoader = loaderFactoryClass.newInstance();
    } catch (Exception e) {
      throw new F2SConfigurationException("Failed to load ServiceProviderConfigurationLoader class", e);
    }
    assert spLoader != null;

    String spString = configuration.getString(StaticLocationServiceConfiguration.SERVICE_PROVIDER_LIST);
    LOG.trace("Service providers to load: {}", spString);
    if (spString == null)
      return;
    Configuration spConfig = configuration.subset(staticLocationServiceConfig.getServiceProviderBase());
    Builder<SP> spBuilders = ImmutableList.builder();
    for (String spName : StringUtils
        .split(spString, StaticLocationServiceConfiguration.SERVICE_PROVIDER_LIST_SEPARATOR)) {
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
    // Nothing to do
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#unregister(com.comcast.viper.flume2storm.location.ServiceProvider)
   */
  public void unregister(SP serviceProvider) {
    // Nothing to do
  }

  /**
   * @see com.comcast.viper.flume2storm.location.LocationService#getSerialization()
   */
  public ServiceProviderSerialization<SP> getSerialization() {
    return serialization;
  }
}
