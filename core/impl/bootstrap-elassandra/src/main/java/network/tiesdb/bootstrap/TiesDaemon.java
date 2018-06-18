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

import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.logback.LogbackESLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.context.api.TiesContext;
import network.tiesdb.exception.TiesStartupException;

/**
 * TiedDB Daemon Main Class.
 * 
 * <P>
 * Entry point to run TiesDB with underlying services.
 * 
 * @author Anton Filatov (filatov@ties.network)
 */
public class TiesDaemon extends TiesBootstrap {

    private static final Logger logger = LoggerFactory.getLogger(TiesDaemon.class);

    static {
        try {
            ESLoggerFactory.setDefaultFactory(new LogbackESLoggerFactory());
        } catch (Exception e) {
            System.err.println("Failed to configure logging " + e.toString());
            e.printStackTrace(System.err);
        }
    }

    public static TiesDaemon instance = new TiesDaemon();

    public final AtomicReference<TiesContext> context = new AtomicReference<>();

    public static void main(String[] args) {
        try {
            instance.init(args);
        } catch (TiesStartupException e) {
            logger.error("Could not start TiesDB", e);
            System.exit(e.getExitCode());
        }
    }

}
