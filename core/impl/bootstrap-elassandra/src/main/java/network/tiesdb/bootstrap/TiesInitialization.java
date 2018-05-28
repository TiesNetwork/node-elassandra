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
package network.tiesdb.bootstrap;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.context.api.TiesServiceConfig;
import network.tiesdb.bootstrap.util.TiesContextHandler;
import network.tiesdb.context.api.TiesContext;
import network.tiesdb.exception.TiesStartupException;
import network.tiesdb.service.api.TiesServiceDaemon;
import network.tiesdb.service.api.TiesServiceFactory;

/**
 * TiesDB initialization.
 * 
 * <P>Main logic for TiesDB service initialization outside of the main thread.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesInitialization implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(TiesInitialization.class);

	public static final String DEFAULT_CONFIG_FILE_NAME = "tiesdb.yaml";
	private static final String DEFAULT_CONFIG_CONTEXT_FACTORY = "network.tiesdb.context.impl.yaml.YAMLContextFactory";

	@Override
	public void run() {
		logger.trace("Starting TiesDB boot sequence...");
		try {
			initialize();
		} catch (TiesStartupException e) {
			logger.error("An unrecoverable error occurred during the TiesDB initialization process", e);
			System.exit(e.getExitCode());
		} catch (Throwable e) {
			logger.error("An unexpected error occurred during the TiesDB initialization process", e);
			System.exit(-2);
		}
		logger.trace("Boot sequence of TiesDB finished successfully");
	}

	public void initialize() throws Throwable {
		logger.trace("Starting TiesDB initialization process...");
		logger.trace("Loading TiesDB context...");
		TiesContextHandler contextHandler = TiesContextHandler.getFactory().createHandler(
				TiesContext.getTiesContextFactory(DEFAULT_CONFIG_CONTEXT_FACTORY),
				new File(getConfigDir() + File.separator + DEFAULT_CONFIG_FILE_NAME).toURI().toURL());
		logger.trace("TiesDB context loaded successfully...");
		if (!TiesDaemon.instance.context.compareAndSet(null, contextHandler.getDelegate())) {
			throw new IllegalStateException("TiesDaemon context has already been initialized");
		}
		logger.trace("TiesDB context contains {} services", contextHandler.getConfigsNames().size());
		for (String name : contextHandler.getConfigsNames()) {
			logger.trace("Found TiesDB service \"{}\" configuration", name);
			TiesServiceConfig config = contextHandler.getConfig(name);
			TiesServiceFactory tiesServiceFactory = config.getTiesServiceFactory();
			if (null == tiesServiceFactory) {
				logger.error("Can't find TiesServiceFactory for service \"{}\"", name);
			} else {
				logger.trace("Launching TiesDB service \"{}\"", name);
				try {
					TiesServiceDaemon service = tiesServiceFactory.createServiceDaemon(name, config);
					service.init();
					TiesShutdown.addShutdownHook(service);
					service.start();
					logger.info("TiesDB service \"{}\" launched successfully", name);
				} catch (Throwable e) {
					logger.error("TiesDB service \"{}\" launch failed", name, e);
				}
			}
		}
		logger.trace("TiesDB initialization process compleated successfully...");
	}

	private static String getConfigDir() {
		String cassandra_conf = System.getenv("CASSANDRA_CONF");
		if (null == cassandra_conf) {
			cassandra_conf = System.getProperty("cassandra.conf",
					System.getProperty("path.conf", getHomeDir() + File.separator + "conf"));
		}
		return cassandra_conf;
	}

	private static String getHomeDir() {
		String cassandra_home = System.getenv("CASSANDRA_HOME");
		if (null == cassandra_home) {
			cassandra_home = System.getProperty("cassandra.home", System.getProperty("path.home"));
			if (null == cassandra_home)
				throw new IllegalStateException(
						"Cannot start, environnement variable CASSANDRA_HOME and system properties cassandra.home"
								+ " or path.home are null. Please set one of these to start properly");
		}
		return cassandra_home;
	}

}
