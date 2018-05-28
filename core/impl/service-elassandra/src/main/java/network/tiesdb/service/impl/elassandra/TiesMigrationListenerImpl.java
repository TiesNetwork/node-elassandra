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

import org.apache.cassandra.service.MigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Cassandra migration listener for TiesDB.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesMigrationListenerImpl extends MigrationListener {

	private static final Logger logger = LoggerFactory.getLogger(TiesMigrationListenerImpl.class);

	private final TiesServiceImpl service;

	public TiesMigrationListenerImpl(TiesServiceImpl service) {
		super();
		this.service = service;
	}

	@Override
	public void onCreateKeyspace(String ksName) {
		logger.debug("TiesDB keyspace created {}", ksName);
		super.onCreateKeyspace(ksName);
	}

	@Override
	public void onDropKeyspace(String ksName) {
		logger.debug("TiesDB keyspace removed {}", ksName);
		super.onDropKeyspace(ksName);
	}

	void registerMigrationListener() {
		logger.trace("Waiting for MigrationManager is ready...");
		MigrationManager.waitUntilReadyForBootstrap();
		logger.trace("MigrationManager is ready");
		logger.trace("Registering {}...", TiesMigrationListenerImpl.class.getSimpleName());
		MigrationManager.instance.register(this);
		logger.debug("{} registered successfully for {}", this, service);
	}

	void unregisterMigrationListener() {
		logger.trace("Waiting for MigrationManager is ready...");
		MigrationManager.waitUntilReadyForBootstrap();
		logger.trace("MigrationManager is ready");
		logger.trace("Unregistering {}...", TiesMigrationListenerImpl.class.getSimpleName());
		MigrationManager.instance.unregister(this);
		logger.debug("{} unregistered successfully for {}", this, service);
	}
}