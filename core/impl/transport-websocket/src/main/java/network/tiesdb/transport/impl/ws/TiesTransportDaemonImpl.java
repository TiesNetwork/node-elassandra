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
package network.tiesdb.transport.impl.ws;

import network.tiesdb.context.api.TiesTransportConfig;
import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.exception.TiesException;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.transport.api.TiesTransport;
import network.tiesdb.transport.api.TiesTransportDaemon;

/**
 * TiesDB WebSock transport daemon implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesTransportDaemonImpl extends TiesTransportImpl implements TiesTransportDaemon {

    public TiesTransportDaemonImpl(TiesService service, TiesTransportConfig config) throws TiesConfigurationException {
        super(service, config);
    }

    @Override
    public TiesTransport getTiesTransport() throws TiesConfigurationException {
        return this;
    }

    @Override
    public void init() throws TiesException {
        super.initInternal();
    }

    @Override
    public void start() throws TiesException {
        super.startInternal();
    }

    @Override
    public void stop() throws TiesException {
        super.stopInternal();
    }

    @Override
    public TiesTransportDaemon getDaemon() {
        return this;
    }

}
