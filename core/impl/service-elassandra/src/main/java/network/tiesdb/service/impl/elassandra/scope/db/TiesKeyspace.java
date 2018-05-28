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
package network.tiesdb.service.impl.elassandra.scope.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.api.TiesVersion;

public final class TiesKeyspace {

    private static final Logger logger = LoggerFactory.getLogger(TiesKeyspace.class);

    public static final TiesVersion UNREADABLE_VERSION = TiesDatabaseStructureVersion.v_0_0_0_unknown;

    public static final TiesVersion NULL_VERSION = TiesDatabaseStructureVersion.v_0_0_0_missing;

    private TiesKeyspace() {
    }

}
