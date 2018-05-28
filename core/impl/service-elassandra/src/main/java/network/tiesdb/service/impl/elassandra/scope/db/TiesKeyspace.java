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
