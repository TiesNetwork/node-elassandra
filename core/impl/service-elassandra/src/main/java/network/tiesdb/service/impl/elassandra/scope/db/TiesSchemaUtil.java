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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.lib.crypto.digest.DigestManager;
import com.tiesdb.lib.crypto.digest.api.Digest;
import com.tiesdb.lib.crypto.encoder.EncoderManager;
import com.tiesdb.lib.crypto.encoder.api.Encoder;

import io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue.Consumer;
import network.tiesdb.exception.TiesConfigurationException;
import network.tiesdb.service.scope.api.TiesServiceScopeException;

public final class TiesSchemaUtil {

    private static final String AND = " AND ";

    public static class SchemaDescription {

        private String tablespace;
        private String table;
        private UUID version;
        private Date scheduled;
        private Date finished;

        public String getTablespace() {
            return tablespace;
        }

        public String getTable() {
            return table;
        }

        public UUID getVersion() {
            return version;
        }

        public Date getScheduled() {
            return scheduled;
        }

        public Date getFinished() {
            return finished;
        }

        @Override
        public String toString() {
            return "SchemaDescription [tablespace=" + tablespace + ", table=" + table + ", version=" + version + ", scheduled=" + scheduled
                    + ", finished=" + finished + "]";
        }

        public boolean isUpdating() {
            return null == finished && null != scheduled;
        }

        public boolean isError() {
            return null != finished && null == scheduled;
        }

        public boolean isDisabled() {
            return null == finished && null == scheduled;
        }

        public boolean isOk() {
            return null != finished && null != scheduled;
        }

    }

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
            return "FieldDescription [name=" + name + ", type=" + type + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            FieldDescription other = (FieldDescription) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            if (type == null) {
                if (other.type != null)
                    return false;
            } else if (!type.equals(other.type))
                return false;
            return true;
        }

    }

    public static final String ENTRY_HEADER = "ENTRY_HEADER";
    public static final String ENTRY_VERSION = "VERSION";

    public static enum HeaderField {

        TIM, //
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

    private static final Logger LOG = LoggerFactory.getLogger(TiesSchemaUtil.class);

    private static final String KEYSPACE = "ties_schema";
    private static final String KEYSPACE_REPLICATION = "{ 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'DC1': '1' }";
    private static final String FIELDS_TABLE = "fields";
    private static final String SCHEMAS_TABLE = "schemas";

    private static final String SCHEMA_TABLESPACE_NAME = "tablespace_name";
    private static final String SCHEMA_TABLE_NAME = "table_name";
    private static final String SCHEMA_FIELD_NAME = "field_name";
    private static final String SCHEMA_FIELD_TYPE = "field_type";
    private static final String SCHEMA_VERSION = "version";
    private static final String SCHEMA_UPDATE_SCHEDULED = "update_scheduled";
    private static final String SCHEMA_UPDATE_FINISHED = "update_finished";

    private static final int DEFAULT_FIELDS_SYNC_RETRY = 3;
    private static final int DEFAULT_CREATION_RETRY = 2;

    private static final Encoder NAME_ENCODER = EncoderManager.getEncoder(EncoderManager.BASE32_NP);

    private static final String TYPE_ENTRY_HEADER = "ENTRY_HEADER";

    public static String getNameId(String prefix, String name) {
        return prefix + getNameId(name);
    }

    public static String getNameId(String name) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Digest digest = DigestManager.getDigest(DigestManager.KECCAK_224);
            digest.update(name.getBytes(Charset.forName("UTF-8")));
            byte[] nameHash = new byte[digest.getDigestSize()];
            digest.doFinal(nameHash, 0);
            NAME_ENCODER.encode(nameHash, b -> baos.write(b));
            return baos.toString();
        } catch (IOException e) {
            return null;
        }
    }

    private TiesSchemaUtil() {
    }

    public static void check() throws TiesConfigurationException {
        LOG.debug("Checking TiesDB schema");
        awaitStorageService();
        {
            LOG.debug("Checking TiesDB schema keyspace");
            Keyspace ks = Schema.instance.getKeyspaceInstance(KEYSPACE);
            if (null == ks) {
                createSchemaKeyspace();
                ks = Schema.instance.getKeyspaceInstance(KEYSPACE);
            }
            if (null == ks) {
                throw new TiesConfigurationException("Ties schema keyspace `" + KEYSPACE + "` not found");
            }
            LOG.debug("TiesDB schema keyspace found");
        }
        {
            LOG.debug("Checking TiesDB schemas table");
            CFMetaData sch = Schema.instance.getCFMetaData(KEYSPACE, SCHEMAS_TABLE);
            if (null == sch) {
                createSchemasTable();
                sch = Schema.instance.getCFMetaData(KEYSPACE, SCHEMAS_TABLE);
            }
            if (null == sch) {
                throw new TiesConfigurationException("TiesDB schemas table `" + KEYSPACE + "`.`" + SCHEMAS_TABLE + "` not found");
            }
        }
        {
            LOG.debug("Checking TiesDB fields table");
            CFMetaData sch = Schema.instance.getCFMetaData(KEYSPACE, FIELDS_TABLE);
            if (null == sch) {
                createFieldsTable();
                sch = Schema.instance.getCFMetaData(KEYSPACE, FIELDS_TABLE);
            }
            if (null == sch) {
                throw new TiesConfigurationException("TiesDB fields table `" + KEYSPACE + "`.`" + FIELDS_TABLE + "` not found");
            }
        }
        LOG.debug("TiesDB schema table found");
    }

    private static void createSchemaKeyspace() throws TiesConfigurationException {
        LOG.debug("Creating TiesDB schema keyspace: `{}`", KEYSPACE);
        QueryProcessor.execute(//
                "CREATE KEYSPACE " + KEYSPACE //
                        + " WITH REPLICATION = " + KEYSPACE_REPLICATION //
                        + " AND DURABLE_WRITES = true", //
                ConsistencyLevel.ALL);
    }

    private static void createFieldsTable() throws TiesConfigurationException {
        LOG.debug("Creating TiesDB fields table: `{}`", FIELDS_TABLE);
        QueryProcessor.execute(//
                "CREATE TABLE " + KEYSPACE + "." + FIELDS_TABLE + " (\n" //
                        + SCHEMA_TABLESPACE_NAME + " text,\n" //
                        + SCHEMA_TABLE_NAME + " text,\n" //
                        + SCHEMA_FIELD_NAME + " text,\n" //
                        + SCHEMA_FIELD_TYPE + " text,\n" //
                        + SCHEMA_VERSION + " uuid,\n" //
                        + " PRIMARY KEY (("//
                        + SCHEMA_TABLESPACE_NAME + "," //
                        + SCHEMA_TABLE_NAME + "," //
                        + SCHEMA_VERSION + ")," //
                        + SCHEMA_FIELD_NAME //
                        + ")\n" //
                        + ")", //
                ConsistencyLevel.ALL);
    }

    private static void createSchemasTable() throws TiesConfigurationException {
        LOG.debug("Creating TiesDB schemas table: `{}`", FIELDS_TABLE);
        QueryProcessor.execute(//
                "CREATE TABLE " + KEYSPACE + "." + SCHEMAS_TABLE + " (\n"//
                        + SCHEMA_TABLESPACE_NAME + " text,\n"//
                        + SCHEMA_TABLE_NAME + " text,\n"//
                        + SCHEMA_VERSION + " uuid,\n"//
                        + SCHEMA_UPDATE_SCHEDULED + " timestamp,\n"//
                        + SCHEMA_UPDATE_FINISHED + " timestamp,\n"//
                        + " PRIMARY KEY (("//
                        + SCHEMA_TABLESPACE_NAME + ","//
                        + SCHEMA_TABLE_NAME + "))\n"//
                        + ")", //
                ConsistencyLevel.ALL);
    }

    private static void awaitStorageService() throws TiesConfigurationException {
        LOG.debug("Waiting for cassandra StorageService");
        try {
            int count = 600;
            while (!StorageService.instance.isInitialized() && --count > 0) {
                Thread.sleep(100);
            }
            if (count == 0) {
                throw new TimeoutException("StorageService has not been ready for 60 seconds");
            }
        } catch (InterruptedException | TimeoutException e) {
            throw new TiesConfigurationException("Await for StorageService failed", e);
        }
        LOG.debug("Cassandra StorageService is ready");
    }

    private static SchemaDescription readSchemaDescription(UntypedResultSet.Row row, SchemaDescription schema) {
        schema.tablespace = row.getString(SCHEMA_TABLESPACE_NAME);
        schema.table = row.getString(SCHEMA_TABLE_NAME);
        schema.version = row.getUUID(SCHEMA_VERSION);
        schema.scheduled = !row.has(SCHEMA_UPDATE_SCHEDULED) ? null : row.getTimestamp(SCHEMA_UPDATE_SCHEDULED);
        schema.finished = !row.has(SCHEMA_UPDATE_FINISHED) ? null : row.getTimestamp(SCHEMA_UPDATE_FINISHED);
        return schema;
    }

    public static SchemaDescription refreshSchemaDescription(SchemaDescription schema) {
        UntypedResultSet result = QueryProcessor.execute(//
                "SELECT " + SCHEMA_TABLESPACE_NAME + ", " //
                        + SCHEMA_TABLE_NAME + ", " //
                        + SCHEMA_VERSION + ", " //
                        + SCHEMA_UPDATE_SCHEDULED + ", " //
                        + SCHEMA_UPDATE_FINISHED //
                        + " FROM " + KEYSPACE + "." + SCHEMAS_TABLE //
                        + " WHERE " + SCHEMA_TABLESPACE_NAME + " = ?" //
                        + AND + SCHEMA_TABLE_NAME + " = ?" //
                , //
                ConsistencyLevel.ALL, new Object[] { //
                        schema.tablespace, //
                        schema.table, //
                });
        for (UntypedResultSet.Row row : result) {
            return readSchemaDescription(row, schema);
        }
        return null;
    }

    public static void loadScheduledSchemaDescriptions(Date date, Consumer<SchemaDescription> c) {
        UntypedResultSet result = QueryProcessor.execute(//
                "SELECT " + SCHEMA_TABLESPACE_NAME + ", " //
                        + SCHEMA_TABLE_NAME + ", " //
                        + SCHEMA_VERSION + ", " //
                        + SCHEMA_UPDATE_SCHEDULED + ", " //
                        + SCHEMA_UPDATE_FINISHED //
                        + " FROM " + KEYSPACE + "." + SCHEMAS_TABLE //
                        + " WHERE " + SCHEMA_UPDATE_SCHEDULED + " <= ?" //
                        + AND + SCHEMA_UPDATE_FINISHED + " > 0" //
                        + " ALLOW FILTERING" //
                , //
                ConsistencyLevel.LOCAL_QUORUM, new Object[] { //
                        date, //
                });
        for (UntypedResultSet.Row row : result) {
            c.accept(readSchemaDescription(row, new SchemaDescription()));
        }
    }

    public static void loadUpdatingSchemaDescriptions(Consumer<SchemaDescription> c) {
        UntypedResultSet result = QueryProcessor.execute(//
                "SELECT " + SCHEMA_TABLESPACE_NAME + ", " //
                        + SCHEMA_TABLE_NAME + ", " //
                        + SCHEMA_VERSION + ", " //
                        + SCHEMA_UPDATE_SCHEDULED + ", " //
                        + SCHEMA_UPDATE_FINISHED //
                        + " FROM " + KEYSPACE + "." + SCHEMAS_TABLE //
                        + " WHERE " + SCHEMA_UPDATE_SCHEDULED + " > 0" //
                        + AND + SCHEMA_UPDATE_FINISHED + " = NULL" //
                        + " ALLOW FILTERING" //
                , //
                ConsistencyLevel.LOCAL_QUORUM);
        for (UntypedResultSet.Row row : result) {
            c.accept(readSchemaDescription(row, new SchemaDescription()));
        }
    }

    public static void loadFailedSchemaDescriptions(Consumer<SchemaDescription> c) {
        UntypedResultSet result = QueryProcessor.execute(//
                "SELECT " + SCHEMA_TABLESPACE_NAME + ", " //
                        + SCHEMA_TABLE_NAME + ", " //
                        + SCHEMA_VERSION + ", " //
                        + SCHEMA_UPDATE_SCHEDULED + ", " //
                        + SCHEMA_UPDATE_FINISHED //
                        + " FROM " + KEYSPACE + "." + SCHEMAS_TABLE //
                        + " WHERE " + SCHEMA_UPDATE_SCHEDULED + " = NULL" //
                        + AND + SCHEMA_UPDATE_FINISHED + " > 0" //
                        + " ALLOW FILTERING" //
                , //
                ConsistencyLevel.LOCAL_QUORUM);
        for (UntypedResultSet.Row row : result) {
            c.accept(readSchemaDescription(row, new SchemaDescription()));
        }
    }

    public static void loadDisabledSchemaDescriptions(Consumer<SchemaDescription> c) {
        UntypedResultSet result = QueryProcessor.execute(//
                "SELECT " + SCHEMA_TABLESPACE_NAME + ", " //
                        + SCHEMA_TABLE_NAME + ", " //
                        + SCHEMA_VERSION + ", " //
                        + SCHEMA_UPDATE_SCHEDULED + ", " //
                        + SCHEMA_UPDATE_FINISHED //
                        + " FROM " + KEYSPACE + "." + SCHEMAS_TABLE //
                        + " WHERE " + SCHEMA_UPDATE_SCHEDULED + " = NULL" //
                        + AND + SCHEMA_UPDATE_FINISHED + " = NULL" //
                        + " ALLOW FILTERING" //
                , //
                ConsistencyLevel.LOCAL_QUORUM);
        for (UntypedResultSet.Row row : result) {
            c.accept(readSchemaDescription(row, new SchemaDescription()));
        }
    }

    public static void updateSchemaDescriptionStart(SchemaDescription schema, Date date) throws TiesServiceScopeException {

        if (null == schema.finished || null == schema.scheduled) {
            throw new TiesServiceScopeException("Schema wrong state. Can't start schema: " + schema);
        }

        Map<String, Object> update = new HashMap<>();
        update.put(SCHEMA_UPDATE_FINISHED, null);
        update.put(SCHEMA_UPDATE_SCHEDULED, date);

        Map<String, Object> conditions = new HashMap<>();
        conditions.put(SCHEMA_UPDATE_FINISHED, schema.finished);
        conditions.put(SCHEMA_UPDATE_SCHEDULED, schema.scheduled);
        conditions.put(SCHEMA_VERSION, schema.version);

        saveSchemaDescription(schema, update, conditions);
    }

    public static void updateSchemaDescriptionSucces(SchemaDescription schema, Date date, int delayTime, TimeUnit delayTimeUnit)
            throws TiesServiceScopeException {
        updateSchemaDescriptionSucces(schema, null, date, delayTime, delayTimeUnit);
    }

    public static void updateSchemaDescriptionSucces(SchemaDescription schema, UUID newVersion, Date date, int delayTime,
            TimeUnit delayTimeUnit) throws TiesServiceScopeException {

        if (null != schema.finished || null == schema.scheduled) {
            throw new TiesServiceScopeException("Schema wrong state. Can't success schema: " + schema);
        }

        Map<String, Object> update = new HashMap<>();
        update.put(SCHEMA_UPDATE_FINISHED, date);
        update.put(SCHEMA_UPDATE_SCHEDULED, new Date(date.getTime() + delayTimeUnit.toMillis(delayTime)));

        Map<String, Object> conditions = new HashMap<>();
        conditions.put(SCHEMA_UPDATE_FINISHED, schema.finished);
        conditions.put(SCHEMA_UPDATE_SCHEDULED, schema.scheduled);
        conditions.put(SCHEMA_VERSION, schema.version);

        if (null != newVersion) {
            update.put(SCHEMA_VERSION, newVersion);
        }

        saveSchemaDescription(schema, update, conditions);
    }

    public static void updateSchemaDescriptionError(SchemaDescription schema, Date date) throws TiesServiceScopeException {

        if (null != schema.finished || null == schema.scheduled) {
            throw new TiesServiceScopeException("Schema wrong state. Can't error schema: " + schema);
        }

        Map<String, Object> update = new HashMap<>();
        update.put(SCHEMA_UPDATE_FINISHED, date);
        update.put(SCHEMA_UPDATE_SCHEDULED, null);

        Map<String, Object> conditions = new HashMap<>();
        conditions.put(SCHEMA_UPDATE_FINISHED, schema.finished);
        conditions.put(SCHEMA_UPDATE_SCHEDULED, schema.scheduled);
        conditions.put(SCHEMA_VERSION, schema.version);

        saveSchemaDescription(schema, update, conditions);
    }

    public static void updateSchemaDescriptionRestart(SchemaDescription schema, Date date) throws TiesServiceScopeException {
        updateSchemaDescriptionRestart(schema, date, false);
    }

    public static void updateSchemaDescriptionRestart(SchemaDescription schema, Date date, boolean withDisabled)
            throws TiesServiceScopeException {

        if ((!withDisabled && null == schema.finished) || null != schema.scheduled) {
            throw new TiesServiceScopeException("Schema wrong state. Can't restart schema: " + schema);
        }

        Map<String, Object> update = new HashMap<>();
        update.put(SCHEMA_UPDATE_FINISHED, null);
        update.put(SCHEMA_UPDATE_SCHEDULED, date);

        Map<String, Object> conditions = new HashMap<>();
        conditions.put(SCHEMA_UPDATE_FINISHED, schema.finished);
        conditions.put(SCHEMA_UPDATE_SCHEDULED, schema.scheduled);
        conditions.put(SCHEMA_VERSION, schema.version);

        saveSchemaDescription(schema, update, conditions);
    }

    public static void updateSchemaDescriptionReschedule(SchemaDescription schema, Date date, int defaultUpdateDelay,
            TimeUnit defaultUpdateDelayUnit) throws TiesServiceScopeException {
        updateSchemaDescriptionReschedule(schema, date, defaultUpdateDelay, defaultUpdateDelayUnit, false);
    }

    public static void updateSchemaDescriptionReschedule(SchemaDescription schema, Date date, int delayTime, TimeUnit delayTimeUnit,
            boolean withDisabled) throws TiesServiceScopeException {

        if ((!withDisabled && null == schema.finished) || null != schema.scheduled) {
            throw new TiesServiceScopeException("Schema wrong state. Can't restart schema: " + schema);
        }

        Map<String, Object> update = new HashMap<>();
        update.put(SCHEMA_UPDATE_FINISHED, null != schema.finished ? schema.finished : date);
        update.put(SCHEMA_UPDATE_SCHEDULED, new Date(date.getTime() + delayTimeUnit.toMillis(delayTime)));

        Map<String, Object> conditions = new HashMap<>();
        conditions.put(SCHEMA_UPDATE_FINISHED, schema.finished);
        conditions.put(SCHEMA_UPDATE_SCHEDULED, schema.scheduled);
        conditions.put(SCHEMA_VERSION, schema.version);

        saveSchemaDescription(schema, update, conditions);
    }

    private static void saveSchemaDescription(SchemaDescription schema, Map<String, Object> update, Map<String, Object> conditions)
            throws TiesServiceScopeException {
        if (update.isEmpty()) {
            return;
        }

        List<Object> values = new LinkedList<>();
        StringBuilder query = new StringBuilder("UPDATE " + KEYSPACE + "." + SCHEMAS_TABLE + " SET "); //

        if (!update.isEmpty()) {
            for (Map.Entry<String, Object> e : update.entrySet()) {
                query.append(e.getKey());
                query.append(" = ?, ");
                values.add(e.getValue());
            }
            query.setLength(query.length() - 2);
        }

        query.append(//
                " WHERE " + SCHEMA_TABLESPACE_NAME + " = ? " + //
                        AND + SCHEMA_TABLE_NAME + " = ? ");
        values.add(schema.tablespace);
        values.add(schema.table);

        if (!conditions.isEmpty()) {
            query.append(" IF ");
            for (Map.Entry<String, Object> e : conditions.entrySet()) {
                query.append(e.getKey());
                query.append(" = ?" + AND);
                values.add(e.getValue());
            }
            query.setLength(query.length() - AND.length());
        } else {
            query.append(" IF EXISTS");
        }

        UntypedResultSet result = QueryProcessor.execute(query.toString(), ConsistencyLevel.ALL, values.toArray());

        if (result.isEmpty()) {
            throw new TiesServiceScopeException("No update result found");
        } else if (result.size() > 1) {
            throw new TiesServiceScopeException("Multiple updates results found");
        } else if (!result.one().getBoolean("[applied]")) {
            throw new TiesServiceScopeException("Update failed");
        }

        refreshSchemaDescription(schema);
    }

    private static UUID getSchemaVersion(String tablespaceName, String tableName) {
        UntypedResultSet result = QueryProcessor.execute(//
                "SELECT " + SCHEMA_VERSION//
                        + " FROM " + KEYSPACE + "." + SCHEMAS_TABLE//
                        + " WHERE " + SCHEMA_TABLESPACE_NAME + " = ?" //
                        + AND + SCHEMA_TABLE_NAME + " = ?" //
                , //
                ConsistencyLevel.LOCAL_QUORUM, new Object[] { //
                        tablespaceName, //
                        tableName, //
                });
        for (UntypedResultSet.Row row : result) {
            return row.getUUID(SCHEMA_VERSION);
        }
        return null;
    }

    public static void loadFieldDescriptions(String tablespaceName, String tableName, Consumer<FieldDescription> c) {
        UUID schemaVersion = getSchemaVersion(tablespaceName, tableName);
        if (null == schemaVersion) {
            return;
        }
        UntypedResultSet result;
        {
            int retryCount = DEFAULT_FIELDS_SYNC_RETRY;
            UUID schemaVersionCheck;
            do {
                result = QueryProcessor.execute(//
                        "SELECT " + SCHEMA_FIELD_NAME + ", " //
                                + SCHEMA_FIELD_TYPE//
                                + " FROM " + KEYSPACE + "." + FIELDS_TABLE//
                                + " WHERE " + SCHEMA_TABLESPACE_NAME + " = ?" //
                                + AND + SCHEMA_TABLE_NAME + " = ?" //
                                + AND + SCHEMA_VERSION + " = ?" //
                                + " ORDER BY " //
                                + SCHEMA_FIELD_NAME //
                                + " ASC" //
                        , //
                        ConsistencyLevel.LOCAL_QUORUM, new Object[] { //
                                tablespaceName, //
                                tableName, //
                                schemaVersion, //
                        });
                schemaVersionCheck = getSchemaVersion(tablespaceName, tableName);
            } while (!schemaVersion.equals(schemaVersionCheck) && null != (schemaVersion = schemaVersionCheck) && --retryCount > 0);
            if (0 >= retryCount) {
                throw new IllegalStateException("Retry failed for schema fields retrieve. Can't read fields consistently");
            }
        }
        if (null == schemaVersion) {
            return;
        }
        for (UntypedResultSet.Row row : result) {
            c.accept(new FieldDescription(row.getString(SCHEMA_FIELD_NAME), row.getString(SCHEMA_FIELD_TYPE)));
        }
    }

    public static void storeFieldDescription(String tablespaceName, String tableName, UUID schemaVersion, FieldDescription fieldDescription)
            throws TiesServiceScopeException {
        UntypedResultSet result = QueryProcessor.execute(//
                "INSERT INTO " + KEYSPACE + "." + FIELDS_TABLE + " ("//
                        + SCHEMA_TABLESPACE_NAME + ", " //
                        + SCHEMA_TABLE_NAME + ", " //
                        + SCHEMA_VERSION + ", " //
                        + SCHEMA_FIELD_NAME + ", " //
                        + SCHEMA_FIELD_TYPE//
                        + ") VALUES (" //
                        + "?, " //
                        + "?, " //
                        + "?, " //
                        + "?, " //
                        + "?" //
                        + ") IF NOT EXISTS" //
                , //
                ConsistencyLevel.ALL, new Object[] { //
                        tablespaceName, //
                        tableName, //
                        schemaVersion, //
                        fieldDescription.name, //
                        fieldDescription.type, //
                });

        if (result.isEmpty()) {
            throw new TiesServiceScopeException("No update result found");
        } else if (result.size() > 1) {
            throw new TiesServiceScopeException("Multiple updates results found");
        } else if (!result.one().getBoolean("[applied]")) {
            throw new TiesServiceScopeException("Update failed");
        }
    }

    public static void storeSchemaDescription(String tablespaceName, String tableName, UUID schemaVersion, Date date, int delayTime,
            TimeUnit delayTimeUnit) throws TiesServiceScopeException {
        UntypedResultSet result = QueryProcessor.execute(//
                "INSERT INTO " + KEYSPACE + "." + SCHEMAS_TABLE + " ("//
                        + SCHEMA_TABLESPACE_NAME + ", " //
                        + SCHEMA_TABLE_NAME + ", " //
                        + SCHEMA_VERSION + ", " //
                        + SCHEMA_UPDATE_FINISHED + ", " //
                        + SCHEMA_UPDATE_SCHEDULED //
                        + ") VALUES (" //
                        + "?, " //
                        + "?, " //
                        + "?, " //
                        + "?, " //
                        + "?" //
                        + ") IF NOT EXISTS" //
                , //
                ConsistencyLevel.ALL, new Object[] { //
                        tablespaceName, //
                        tableName, //
                        schemaVersion, //
                        date, //
                        new Date(date.getTime() + delayTimeUnit.toMillis(delayTime)), //
                });

        if (result.isEmpty()) {
            throw new TiesServiceScopeException("No update result found");
        } else if (result.size() > 1) {
            throw new TiesServiceScopeException("Multiple updates results found");
        } else if (!result.one().getBoolean("[applied]")) {
            throw new TiesServiceScopeException("Update failed");
        }
    }

    public static void removeFieldDescriptionsByVersion(String tablespaceName, String tableName, UUID schemaVersion)
            throws TiesServiceScopeException {
        QueryProcessor.execute(//
                "DELETE FROM " + KEYSPACE + "." + FIELDS_TABLE //
                        + " WHERE " + SCHEMA_TABLESPACE_NAME + " = ?" //
                        + AND + SCHEMA_TABLE_NAME + " = ?" //
                        + AND + SCHEMA_VERSION + " = ?" //
                , //
                ConsistencyLevel.ALL, new Object[] { //
                        tablespaceName, //
                        tableName, //
                        schemaVersion, //
                });
    }

    public static void createTiesDBStorage(String tablespaceName, String tableName, List<FieldDescription> primaryIndex) {

        if (primaryIndex.isEmpty()) {
            throw new IllegalArgumentException("Table PrimaryIndex should not be empty");
        }
        LinkedList<FieldDescription> cachedDescriptions = new LinkedList<>();
        loadFieldDescriptions(tablespaceName, tableName, cachedDescriptions::add);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);

        Keyspace ks;
        {
            int ksCreationRetry = DEFAULT_CREATION_RETRY;
            while (null == (ks = Schema.instance.getKeyspaceInstance(tablespaceNameId)) && ksCreationRetry-- > 0) {
                createTiesDBTablespace(tablespaceName);
            }
            if (null == ks) {
                throw new IllegalStateException("Keyspace `" + tablespaceNameId + "`(" + tablespaceName + ") was not found for table `"
                        + tableNameId + "`(" + tableName + ") creation ");
            }
        }

        CFMetaData kst = ks.getMetadata().getTableOrViewNullable(tableNameId);
        if (null == kst) {
            StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS \"");
            query.append(tablespaceNameId);
            query.append("\".\"");
            query.append(tableNameId);
            query.append("\" (\"");
            query.append(ENTRY_HEADER);
            query.append("\" \"");
            query.append(TYPE_ENTRY_HEADER);
            query.append("\",\"");
            query.append(ENTRY_VERSION);
            query.append("\" varint,");

            for (FieldDescription field : primaryIndex) {
                query.append('"');
                query.append(getNameId("FLD", field.name));
                query.append("\" ");
                query.append(TiesTypeHelper.mapToCassandraType(field.type));
                query.append(",\"");
                query.append(getNameId("HSH", field.name));
                query.append("\" blob, \"");
                query.append(getNameId("VAL", field.name));
                query.append("\" blob,");
            }
            query.append("PRIMARY KEY (");
            for (FieldDescription field : primaryIndex) {
                query.append('"');
                query.append(getNameId("FLD", field.name));
                query.append("\",");
            }
            query.setLength(query.length() - 1);
            query.append("))");

            QueryProcessor.execute(query.toString(), ConsistencyLevel.ALL);

        } else if (kst.isView()) {
            throw new IllegalStateException("Expected table `" + tablespaceNameId + "`.`" + tableNameId + "`(" + tablespaceName + "."
                    + tableName + ") but view with the same name is already exists");
        }
    }

    private static void createTiesDBTablespace(String tablespaceName) {
        String tablespaceNameId = getNameId("TIE", tablespaceName);
        Keyspace ks = Schema.instance.getKeyspaceInstance(tablespaceNameId);
        if (null == ks) {
            QueryProcessor.execute(//
                    "CREATE KEYSPACE IF NOT EXISTS \"" + tablespaceNameId + "\"" //
                            + " WITH REPLICATION = { 'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'DC1': '1' }" //
                            + AND + "DURABLE_WRITES = true" //
                    , //
                    ConsistencyLevel.ALL);

            QueryProcessor.execute(//
                    "CREATE TYPE IF NOT EXISTS \"" + tablespaceNameId + "\".\"" + TYPE_ENTRY_HEADER + "\" (" //
                            + " tim timestamp," //
                            + " net smallint," //
                            + " fhs blob," //
                            + " ohs blob," //
                            + " snr blob," //
                            + " sig blob," //
                            + " hsh blob" //
                            + ")" //
                    , //
                    ConsistencyLevel.ALL);
        }
    }

    public static void refreshTiesDBStorage(String tablespaceName, String tableName, FieldDescription fieldDescription) {
        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);

        CFMetaData tableMeta = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);
        if (null == tableMeta) {
            throw new IllegalStateException("Table `" + tablespaceNameId + "`.`" + tableNameId + "`(" + tablespaceName + "." + tableName
                    + ") was not found in cassandra");
        }
        String fieldNameId = getNameId("FLD", fieldDescription.getName());
        ColumnDefinition columnDefinition = tableMeta.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
        if (null == columnDefinition) {
            QueryProcessor.execute(//
                    "ALTER TABLE \"" + tablespaceNameId + "\".\"" + tableNameId + "\" ADD (" //
                            + "\"" + getNameId("FLD", fieldDescription.name) + "\" "//
                            + TiesTypeHelper.mapToCassandraType(fieldDescription.type) + "," //
                            + "\"" + getNameId("HSH", fieldDescription.name) + "\" blob," //
                            + "\"" + getNameId("VAL", fieldDescription.name) + "\" blob" //
                            + ")" //
                    , //
                    ConsistencyLevel.ALL);
        }
    }

}
