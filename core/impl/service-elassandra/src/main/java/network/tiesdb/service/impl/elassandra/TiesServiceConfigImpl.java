/*
 * Copyright 2017 Ties BV
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
package network.tiesdb.service.impl.elassandra;

import java.util.List;

import network.tiesdb.context.api.TiesServiceConfig;
import network.tiesdb.context.api.TiesTransportConfig;
import network.tiesdb.context.api.annotation.TiesConfigElement;
import network.tiesdb.service.api.TiesServiceFactory;

/**
 * TiesDB service configuration implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
@TiesConfigElement({ TiesServiceConfigImpl.BINDING, TiesServiceConfigImpl.SHORT_BINDING })
public class TiesServiceConfigImpl implements TiesServiceConfig {

	static final String BINDING = "network.tiesdb.Service";
	static final String SHORT_BINDING = "TiesService";

	private boolean serviceStopCritical = true;

	private List<TiesTransportConfig> transports;

	public TiesServiceConfigImpl() {
		// NOP Is not empty config values
	}

	public TiesServiceConfigImpl(String value) {
		// NOP If this constructor is called then config values is empty and we
		// should use default
	}

	@Override
	public TiesServiceFactory getTiesServiceFactory() {
		return new TiesServiceFactoryImpl();
	}

	@Override
	public Boolean isServiceStopCritical() {
		return serviceStopCritical;
	}

	public void setServiceStopCritical(boolean serviceStopCritical) {
		this.serviceStopCritical = serviceStopCritical;
	}

	@Override
	public List<TiesTransportConfig> getTransportConfigs() {
		return transports;
	}

	public void setTransports(List<TiesTransportConfig> transports) {
		this.transports = transports;
	}
}