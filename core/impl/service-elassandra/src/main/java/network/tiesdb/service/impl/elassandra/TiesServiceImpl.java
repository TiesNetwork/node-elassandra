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

import static network.tiesdb.util.Safecheck.nullsafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.context.api.TiesServiceConfig;
import network.tiesdb.context.api.TiesTransportConfig;
import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.exception.TiesException;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.impl.elassandra.scope.TiesServiceScopeImpl;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.transport.api.TiesTransport;
import network.tiesdb.transport.api.TiesTransportDaemon;

/**
 * TiesDB service implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public abstract class TiesServiceImpl implements TiesService, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TiesServiceImpl.class);

    private static final TiesServiceImplVersion IMPLEMENTATION_VERSION = TiesServiceImplVersion.v_0_0_1_prealpha;

    protected final TiesServiceConfig config;

    protected final String name;

    private final AtomicReference<List<TiesTransport>> transportsRef = new AtomicReference<>();
    private final TiesMigrationListenerImpl migrationListener;

    public TiesServiceImpl(String name, TiesServiceConfig config) {
        if (null == config) {
            throw new NullPointerException("The config should not be null");
        }
        this.config = config;
        this.name = name;
        this.migrationListener = createTiesMigrationListener();
    }

    protected TiesMigrationListenerImpl createTiesMigrationListener() {
        return new TiesMigrationListenerImpl(this);
    }

    @Override
    public TiesServiceConfig getTiesServiceConfig() {
        return config;
    }

    public void run() {
        try {
            logger.trace("Running TiesDB Service...");
            runInternal();
            logger.trace("TiesDB Service is ready and waiting for connections");
        } catch (Throwable e) {
            logger.error("TiesDB Service failed with exception", e);
            if (null == config.isServiceStopCritical() || config.isServiceStopCritical()) {
                logger.debug("TiesDB Service is vital, execution will be stopped", e);
                System.exit(-1);
            }
        }
    }

    protected void initInternal() throws TiesException {
        initTransportDaemons();
    }

    protected void initTransportDaemons() throws TiesConfigurationException {
        logger.trace("Creating TiesDB Service Transport Daemons...");
        List<TiesTransportConfig> transportsConfigs = config.getTransportConfigs();
        List<TiesTransport> transports = new ArrayList<>(transportsConfigs.size());
        for (TiesTransportConfig tiesTransportConfig : transportsConfigs) {
            try {
                TiesTransportDaemon transportDaemon = tiesTransportConfig.getTiesTransportFactory().createTransportDaemon(this,
                        tiesTransportConfig);
                transports.add(transportDaemon.getTiesTransport());
            } catch (TiesConfigurationException e) {
                logger.error("Failed to create TiesDB Transport Daemon", e);
            }
        }
        if (!transportsRef.compareAndSet(null, Collections.unmodifiableList(transports))) {
            throw new TiesConfigurationException("TiesDB Transports have already been initialized");
        }
    }

    protected void stopInternal() {
        logger.trace("Stopping TiesDB Service Transports...");
        List<TiesTransport> transports = transportsRef.get();
        if (null == transports) {
            logger.trace("No TiesDB Service Transports to stop");
        } else {
            for (TiesTransport tiesTransport : transports) {
                try {
                    nullsafe(tiesTransport.getDaemon()).stop();
                } catch (Throwable e) {
                    logger.error("Failed to stop TiesDB Transport", e);
                }
            }
        }
        migrationListener.unregisterMigrationListener();
    }

    private void runInternal() throws TiesException {
        migrationListener.registerMigrationListener();
        logger.trace("Starting TiesDB Service Transports...");
        List<TiesTransport> transports = transportsRef.get();
        if (null == transports) {
            logger.trace("No TiesDB Service Transports to start");
        } else {
            for (TiesTransport tiesTransport : transports) {
                try {
                    TiesTransportDaemon daemon = nullsafe(tiesTransport.getDaemon());
                    daemon.init();
                    daemon.start();
                } catch (Throwable e) {
                    logger.error("Failed to init and start TiesDB Transport", e);
                }
            }
        }
    }

    @Override
    public TiesVersion getVersion() {
        return IMPLEMENTATION_VERSION;
    }

    @Override
    public List<TiesTransport> getTransports() {
        List<TiesTransport> transports = transportsRef.get();
        return null == transports ? Collections.<TiesTransport>emptyList() : transports;
    }

    @Override
    public TiesServiceScope newServiceScope() {
        return new TiesServiceScopeImpl(this);
    }

}