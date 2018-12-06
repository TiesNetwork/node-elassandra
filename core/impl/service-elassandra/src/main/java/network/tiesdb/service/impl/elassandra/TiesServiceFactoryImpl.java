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

import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.service.api.TiesServiceFactory;

/**
 * TiesDB basic service factory implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesServiceFactoryImpl implements TiesServiceFactory {

    private final TiesServiceConfigImpl config;

    public TiesServiceFactoryImpl(TiesServiceConfigImpl config) {
        this.config = config;
    }

    @Override
    public TiesServiceDaemonImpl createServiceDaemon(String name) throws TiesConfigurationException {
        return new TiesServiceDaemonImpl(name, config);
    }

}
