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

import static network.tiesdb.util.Safecheck.nullsafe;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.context.api.TiesTransportConfig;
import network.tiesdb.exception.TiesException;
import network.tiesdb.handler.api.TiesHandler;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.transport.api.TiesTransport;
import network.tiesdb.transport.impl.ws.netty.WebSocketServer;

/**
 * TiesDB WebSock transport implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public abstract class TiesTransportImpl implements TiesTransport {

    private static final TiesTransportImplVersion IMPLEMENTATION_VERSION = TiesTransportImplVersion.v_0_0_1_prealpha;

    private final TiesTransportConfig config;
    private final WebSocketServer server;
    private final TiesHandler handler;

    public TiesTransportImpl(TiesService tiesService, TiesTransportConfig config) {
        if (null == tiesService) {
            throw new NullPointerException("The tiesService should not be null");
        }
        if (null == config) {
            throw new NullPointerException("The config should not be null");
        }
        this.handler = nullsafe(nullsafe(config.getHandlerConfig()).getTiesHandlerFactory()).createHandler(tiesService);
        this.config = config;
        this.server = new WebSocketServer(this);
    }

    @Override
    public TiesTransportConfig getTiesTransportConfig() {
        return config;
    }

    protected void startInternal() throws TiesException {
        try {
            server.start();
        } catch (CertificateException | SSLException | InterruptedException e) {
            throw new TiesException("Can't start TiesDB Transport", e);
        }
    }

    protected void initInternal() throws TiesException {
        server.init();
    }

    protected void stopInternal() throws TiesException {
        server.stop();
    }

    @Override
    public TiesVersion getVersion() {
        return IMPLEMENTATION_VERSION;
    }

    @Override
    public TiesHandler getHandler() {
        return handler;
    }

}
