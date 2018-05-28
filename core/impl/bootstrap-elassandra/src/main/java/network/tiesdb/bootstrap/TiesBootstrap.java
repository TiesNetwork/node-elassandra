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

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.exception.TiesStartupException;

/**
 * Bootstrap class for TiesDB.
 * 
 * <P>Contains boot and initialization sequences for TiesDB bootstrapping.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesBootstrap {

	private static final Logger logger = LoggerFactory.getLogger(TiesBootstrap.class);

	private static final String DEFAULT_DELEGATE_CLASS_NAME = "org.apache.cassandra.service.ElassandraDaemon";

	public void init(String... args) throws TiesStartupException {
		if (args.length > 0 && args[0].equals("--no-storage")) {
			logger.debug("Found TiesDB --no-storage option. No storage capability will be exposed");
			initTiesDb();
		} else {
			logger.trace("Searching storage system class");
			String[] delegateArgs = args;
			String delegateClassName = DEFAULT_DELEGATE_CLASS_NAME;
			if (args.length > 0) {
				delegateClassName = args[0];
				delegateArgs = Arrays.copyOfRange(args, 1, args.length);
			}
			Class<?> delegateClass;
			try {
				delegateClass = Thread.currentThread().getContextClassLoader().loadClass(delegateClassName);
			} catch (ClassNotFoundException e) {
				throw new TiesStartupException(1, "Could not find delegate startup class \"" + delegateClassName + "\"", e);
			}
			logger.debug("Storage system class found {}", delegateClass.getName());
			initTiesDb();
			logger.debug("Switching to the storage system boot process");
			try {
				delegateClass.getDeclaredMethod("main", String[].class).invoke(delegateClass, (Object) delegateArgs);
			} catch (Throwable e) {
				throw new TiesStartupException(2,
						"Could not start storage system boot process from class \"" + delegateClass.getName() + "\"",
						e);
			}
		}
	}

	// TODO add a parameter or make another function for synchronous
	// initialization
	private void initTiesDb() {
		ThreadGroup tiesThreadGroup = new ThreadGroup(Thread.currentThread().getThreadGroup(), "TiesDB");
		Thread tiesInitThread = new Thread(tiesThreadGroup, new TiesInitialization(), "TiesInitialization");
		tiesInitThread.setDaemon(false);
		tiesInitThread.start();
	}
}
