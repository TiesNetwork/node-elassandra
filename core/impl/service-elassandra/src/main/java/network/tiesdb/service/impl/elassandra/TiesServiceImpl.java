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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.context.api.TiesSchemaConfig;
import network.tiesdb.context.api.TiesServiceConfig;
import network.tiesdb.context.api.TiesTransportConfig;
import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.exception.TiesException;
import network.tiesdb.schema.api.TiesSchema;
import network.tiesdb.schema.api.TiesSchemaFactory;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.impl.elassandra.schema.TiesServiceSchemaImpl;
import network.tiesdb.service.impl.elassandra.scope.TiesServiceScopeImpl;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.transport.api.TiesTransportServer;

/**
 * TiesDB service implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public abstract class TiesServiceImpl implements TiesService, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TiesServiceImpl.class);

    private static final TiesServiceImplVersion IMPLEMENTATION_VERSION = TiesServiceImplVersion.v_0_0_1_prealpha;

    protected final TiesServiceConfig config;

    private final AtomicReference<List<TiesTransportServer>> transportsRef = new AtomicReference<>();
    private final AtomicReference<TiesServiceSchemaImpl> schemaImplRef = new AtomicReference<>();
    private final TiesMigrationListenerImpl migrationListener;

    public TiesServiceImpl(TiesServiceConfig config) {
        if (null == config) {
            throw new NullPointerException("The config should not be null");
        }
        this.config = config;
        this.migrationListener = createTiesMigrationListener();
    }

    protected TiesMigrationListenerImpl createTiesMigrationListener() {
        return new TiesMigrationListenerImpl(this);
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
        initTiesSchema();
    }

    protected void initTiesSchema() throws TiesConfigurationException {
        logger.trace("Creating TiesDB Schema Connection...");
        TiesSchemaConfig schemaConfig = config.getSchemaConfig();
        if (null == schemaConfig) {
            throw new TiesConfigurationException("No TiesDB Schema configuration found");
        }
        TiesSchemaFactory schemaFactory = schemaConfig.getTiesSchemaFactory();
        requireNonNull(schemaFactory, "TiesDB Schema Factory not found");
        TiesSchema schema = schemaFactory.createSchema(this);
        TiesServiceSchemaImpl schemaImpl = new TiesServiceSchemaImpl(schema);
        if (!schemaImplRef.compareAndSet(null, schemaImpl)) {
            throw new TiesConfigurationException("TiesDB Schema have already been initialized");
        }
        schemaImpl.init();
    }

    private void checkDatabaseStructures() throws TiesConfigurationException {
        logger.trace("Checking TiesDB Structures...");
        TiesSchemaUtil.check();
    }

    protected void initTransportDaemons() throws TiesConfigurationException {
        logger.trace("Creating TiesDB Service Transport Daemons...");
        List<TiesTransportConfig> transportsConfigs = config.getTransportConfigs();
        List<TiesTransportServer> transports = new ArrayList<>(transportsConfigs.size());
        for (TiesTransportConfig tiesTransportConfig : transportsConfigs) {
            try {
                TiesTransportServer transportDaemon = tiesTransportConfig.getTiesTransportFactory().createTransportServer(this);
                transports.add(transportDaemon);
            } catch (TiesConfigurationException e) {
                logger.error("Failed to create TiesDB Transport Daemon", e);
            }
        }
        if (!transportsRef.compareAndSet(null, Collections.unmodifiableList(transports))) {
            throw new TiesConfigurationException("TiesDB Transports have already been initialized");
        }
    }

    protected void stopInternal() {
        stopSchema();
        stopTiesTransports();
        migrationListener.unregisterMigrationListener();
    }

    private void runInternal() throws TiesException {
        migrationListener.registerMigrationListener();
        checkDatabaseStructures();
        startSchema();
        startTiesTransports();
    }

    private void startSchema() throws TiesConfigurationException {
        logger.trace("Starting TiesDB Service Schema...");
        getSchemaImpl().start();
    }

    private void stopSchema() {
        logger.trace("Stopping TiesDB Service Schema...");
        try {
            getSchemaImpl().stop();
        } catch (TiesConfigurationException e) {
            logger.debug("No TiesDB Service Schema was found");
        }
    }

    public TiesServiceSchemaImpl getSchemaImpl() throws TiesConfigurationException {
        TiesServiceSchemaImpl schemaImpl = schemaImplRef.get();
        if (null == schemaImpl) {
            throw new TiesConfigurationException("TiesDB Schema have not been initialized");
        }
        return schemaImpl;
    }

    private void startTiesTransports() throws TiesConfigurationException {
        logger.trace("Starting TiesDB Service Transports...");
        List<TiesTransportServer> transports = transportsRef.get();
        if (null == transports) {
            throw new TiesConfigurationException("No TiesDB Service Transports to start");
        } else {
            int count = 0;
            for (TiesTransportServer daemon : transports) {
                try {
                    daemon.init();
                    daemon.start();
                    count++;
                } catch (Throwable e) {
                    logger.error("Failed to init and start TiesDB Transport", e);
                }
            }
            if (0 >= count) {
                throw new TiesConfigurationException("Not a single TiesDB Service Transports has been successfully started");
            }
        }
    }

    private void stopTiesTransports() {
        logger.trace("Stopping TiesDB Service Transports...");
        List<TiesTransportServer> transports = transportsRef.get();
        if (null == transports) {
            logger.trace("No TiesDB Service Transports to stop");
        } else {
            for (TiesTransportServer daemon : transports) {
                try {
                    daemon.stop();
                } catch (Throwable e) {
                    logger.error("Failed to stop TiesDB Transport", e);
                }
            }
        }
    }

    @Override
    public TiesVersion getVersion() {
        return IMPLEMENTATION_VERSION;
    }

    @Override
    public TiesServiceScope newServiceScope() {
        return new TiesServiceScopeImpl(this);
    }

}