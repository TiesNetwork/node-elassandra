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

import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;
import network.tiesdb.exception.TiesConfigurationException;

public final class TiesSchema {

    public static class FieldDescription {

        private final String name;
        private final String type;

        public FieldDescription(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return "Field [name=" + name + ", type=" + type + "]";
        }
    }

    public static final String ENTRY_HEADER = "ENTRY_HEADER";

    public static enum HeaderField {

        TIM, //
        VER, //
        NET, //
        FHS, //
        OHS, //
        SNR, //
        SIG, //
        HSH, //

        ;

        public static HeaderField valueOfIgnoreCase(String str) {
            return valueOf(str.toUpperCase());
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(TiesSchema.class);

    private static final String KEYSPACE = "ties_schema";
    private static final String KEYSPACE_REPLICATION = "{ 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'DC1': '1' }";
    private static final String TABLE = "fields";

    private static final String SCHEMA_TABLESPACE_NAME = "tablespace_name";
    private static final String SCHEMA_TABLE_NAME = "table_name";
    private static final String SCHEMA_FIELD_NAME = "field_name";
    private static final String SCHEMA_FIELD_TYPE = "field_type";

    private TiesSchema() {
    }

    public static void check() throws TiesConfigurationException {
        LOG.debug("Checking TiesDB schema");
        awaitStorageService();

        LOG.debug("Checking TiesDB schema keyspace");
        Keyspace ks = Schema.instance.getKeyspaceInstance(KEYSPACE);
        if (null == ks) {
            createSchemaKeyspace();
            ks = Schema.instance.getKeyspaceInstance(KEYSPACE);
        }
        if (null == ks) {
            throw new TiesConfigurationException("Ties schema keyspace " + KEYSPACE + " not found");
        }
        LOG.debug("TiesDB schema keyspace found");

        LOG.debug("Checking TiesDB schema table");
        CFMetaData sch = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        if (null == sch) {
            createSchemaTable();
            sch = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        }
        if (null == sch) {
            throw new TiesConfigurationException("Ties schema table " + KEYSPACE + "." + TABLE + " not found");
        }
        LOG.debug("TiesDB schema table found");
    }

    private static void createSchemaKeyspace() throws TiesConfigurationException {
        LOG.debug("Creating TiesDB schema keyspace: {}", KEYSPACE);
        QueryProcessor.execute(//
                "CREATE KEYSPACE " + KEYSPACE //
                        + " WITH REPLICATION = " + KEYSPACE_REPLICATION//
                        + " AND DURABLE_WRITES = true", //
                ConsistencyLevel.ALL);
    }

    private static void createSchemaTable() throws TiesConfigurationException {
        LOG.debug("Creating TiesDB schema table: {}", TABLE);
        QueryProcessor.execute(//
                "CREATE TABLE " + KEYSPACE + "." + TABLE + " (\n"//
                        + SCHEMA_TABLESPACE_NAME + " text,\n"//
                        + SCHEMA_TABLE_NAME + " text,\n"//
                        + SCHEMA_FIELD_NAME + " text,\n"//
                        + SCHEMA_FIELD_TYPE + " text,\n"//
                        + " PRIMARY KEY (("//
                        + SCHEMA_TABLESPACE_NAME + "),"//
                        + SCHEMA_TABLE_NAME + ","//
                        + SCHEMA_FIELD_NAME//
                        + " )"//
                        + ")", //
                ConsistencyLevel.ALL);
    }

    private static void awaitStorageService() throws TiesConfigurationException {
        LOG.debug("Waiting for cassandra StorageService");
        try {
            int count = 300;
            while (!StorageService.instance.isInitialized() && --count > 0) {
                Thread.sleep(100);
            }
            if (count == 0) {
                throw new TimeoutException("StorageService has not been ready for 30 seconds");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new TiesConfigurationException("Await for StorageService failed", e);
        }
        LOG.debug("Cassandra StorageService is ready");
    }

    public static void loadFieldDescriptions(String tablespaceName, String tableName, Consumer<FieldDescription> c) {
        UntypedResultSet result = QueryProcessor.execute(//
                "select " + SCHEMA_FIELD_NAME + ", " + SCHEMA_FIELD_TYPE//
                        + " from " + KEYSPACE + "." + TABLE//
                        + " where " + SCHEMA_TABLESPACE_NAME + " = ?" //
                        + " and " + SCHEMA_TABLE_NAME + " = ?" //
                        + " order by " + SCHEMA_TABLE_NAME + ", " + SCHEMA_FIELD_NAME + " asc", //
                ConsistencyLevel.ALL, new Object[] { //
                        tablespaceName, //
                        tableName });
        for (UntypedResultSet.Row row : result) {
            c.accept(new FieldDescription(row.getString(SCHEMA_FIELD_NAME), row.getString(SCHEMA_FIELD_TYPE)));
        }
    }

}
