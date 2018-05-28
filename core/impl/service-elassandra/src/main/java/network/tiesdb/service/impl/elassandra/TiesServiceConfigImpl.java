/**
 * Copyright © 2017 Ties BV
 *
 * This file is part of Ties.DB project.
 *
 * Ties.DB project is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Ties.DB project is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with Ties.DB project. If not, see <https://www.gnu.org/licenses/lgpl-3.0>.
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