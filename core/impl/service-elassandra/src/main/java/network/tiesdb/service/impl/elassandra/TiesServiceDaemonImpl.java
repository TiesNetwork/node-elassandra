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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.context.api.TiesServiceConfig;
import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.exception.TiesException;
import network.tiesdb.service.api.TiesServiceDaemon;

/**
 * TiesDB basic service daemon implementation.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesServiceDaemonImpl extends TiesServiceImpl implements Runnable, TiesServiceDaemon {

    private static final Logger logger = LoggerFactory.getLogger(TiesServiceDaemonImpl.class);

    private final Thread tiesServiceThread;

    public TiesServiceDaemonImpl(String name, TiesServiceConfig config) throws TiesConfigurationException {
        super(name, config);
        logger.trace("Creating TiesDB Service Daemon...");
        ThreadGroup tiesThreadGroup = new ThreadGroup(Thread.currentThread().getThreadGroup(), name);
        tiesServiceThread = new Thread(tiesThreadGroup, this, "TiesServiceDaemon:" + name);
        tiesServiceThread.setDaemon(false);
        logger.trace("TiesDB Service Daemon created successfully");
    }

    @Override
    public void run() {
        logger.trace("Starting TiesDB Service Daemon...");
        try {
            super.initInternal();
            super.run();
        } catch (TiesException e) {
            logger.error("Failed to start TiesDB Service", e);
            System.exit(-2);
        }
        logger.trace("TiesDB Service Daemon started successfully");
    }

    @Override
    public void start() throws TiesException {
        logger.trace("Starting TiesDB Service Daemon...");
        try {
            tiesServiceThread.start();
        } catch (Throwable e) {
            throw new TiesException("Failed to start TiesDB Service", e);
        }
        logger.trace("TiesDB Service Daemon started successfully");
    }

    @Override
    public void stop() throws TiesException {
        logger.trace("Stopping TiesDB Service Daemon...");
        try {
            super.stopInternal();
            tiesServiceThread.join();
        } catch (Throwable e) {
            logger.debug("Failed to stop TiesDB Service Daemon", e);
            logger.trace("Interrupting TiesDB Service Daemon...");
            tiesServiceThread.interrupt();
            throw new TiesException("TiesDB Service Daemon stopped by interrupt", e);
        }
        logger.trace("TiesDB Service Daemon stopped successfully");
    }

    @Override
    public void init() throws TiesException {
        logger.trace("Initializing TiesDB Service Daemon...");
        // NOP: Internal TiesDB Service initialization occurs in a tiesServiceThread
        logger.trace("TiesDB Service Daemon initialized successfully");
    }

    @Override
    public TiesServiceImpl getService() throws TiesConfigurationException {
        return this;
    }

    @Override
    public TiesServiceDaemonImpl getDaemon() {
        return this;
    }

    @Override
    public String getName() {
        return name;
    }

}
