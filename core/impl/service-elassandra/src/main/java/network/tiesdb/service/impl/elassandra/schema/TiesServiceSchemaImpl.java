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
package network.tiesdb.service.impl.elassandra.schema;

import static network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.*;

import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.api.TiesDaemon;
import network.tiesdb.schema.api.TiesSchema;
import network.tiesdb.schema.api.TiesSchema.Field;
import network.tiesdb.schema.api.TiesSchema.Index;
import network.tiesdb.schema.api.TiesSchema.IndexType;
import network.tiesdb.schema.api.TiesSchema.Table;
import network.tiesdb.schema.api.TiesSchema.Tablespace;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.FieldDescription;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.SchemaDescription;
import network.tiesdb.service.scope.api.TiesServiceScopeException;

public class TiesServiceSchemaImpl implements TiesDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(TiesServiceSchemaImpl.class);

    private static final int EXECUTOR_CORE_POOL_SIZE = 2;

    private static final int DEFAULT_UPDATE_DELAY = 10;
    private static final TimeUnit DEFAULT_UPDATE_DELAY_UNIT = TimeUnit.MINUTES;

    private static final int DEFAULT_ERROR_UPDATE_DELAY = (int) (DEFAULT_UPDATE_DELAY * 1.5);
    private static final TimeUnit DEFAULT_ERROR_UPDATE_DELAY_UNIT = DEFAULT_UPDATE_DELAY_UNIT;

    private static final long DEFAULT_CHECK_DELAY = 5;
    private static final TimeUnit DEFAULT_CHECK_DELAY_UNIT = TimeUnit.MINUTES;

    private static volatile boolean isCleanFailed = false;
    private static boolean isClean = false;

    private final AtomicReference<ScheduledExecutorService> schedulerRef = new AtomicReference<ScheduledExecutorService>();
    private final AtomicReference<ScheduledFuture<?>> checkerRef = new AtomicReference<ScheduledFuture<?>>();

    private final TiesSchema schema;

    public TiesServiceSchemaImpl(TiesSchema schema) {
        this.schema = schema;
    }

    public void refreshSchema(String tablespaceName, String tableName) {
        LOG.debug("Refreshing schema `{}`.`{}`", tablespaceName, tableName);
        try {

            LinkedList<FieldDescription> fieldDescriptions = new LinkedList<>();
            loadFieldDescriptions(tablespaceName, tableName, fieldDescriptions::add);

            refreshTiesDBStorage(tablespaceName, tableName, fieldDescriptions);

            LOG.debug("Schema `{}`.`{}` refreshed successfully", tablespaceName, tableName);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to refresh schema `" + tablespaceName + "`.`" + tableName + "`", e);
        }
    }

    public void createSchema(String tablespaceName, String tableName) {
        LOG.debug("Creating schema `{}`.`{}`", tablespaceName, tableName);
        try {
            Table t = getTableFromSchema(tablespaceName, tableName);

            if (!t.isDistributed() && null == System.getProperty("network.tiesdb.debug.SingleNode")) {
                throw new IllegalStateException("TiesDB schema `" + tablespaceName + "`.`" + tableName + "` is not distributed");
            }

            Set<Index> indexes = t.getIndexes();
            Index primaryIndex = indexes.stream().filter(i -> IndexType.PRIMARY.equals(i.getType())).findAny().orElse(null);
            if (null == primaryIndex) {
                throw new IllegalStateException("TiesDB schema `" + tablespaceName + "`.`" + tableName + "` primary index missing");
            }

            UUID newSchemaVersion = UUID.randomUUID();

            try {
                for (String fieldName : t.getFieldNames()) {
                    Field f = t.getField(fieldName);
                    storeFieldDescription(tablespaceName, tableName, newSchemaVersion,
                            new FieldDescription(f.getName(), f.getType().toLowerCase()));
                }
                storeSchemaDescription(tablespaceName, tableName, newSchemaVersion, new Date(), DEFAULT_UPDATE_DELAY,
                        DEFAULT_UPDATE_DELAY_UNIT);

                createTiesDBStorage(//
                        tablespaceName, //
                        tableName, //
                        primaryIndex.getFields().stream() //
                                .map(f -> new FieldDescription(f.getName(), f.getType()))//
                                .collect(Collectors.toList()) //
                );

                refreshSchema(tablespaceName, tableName);

            } catch (Throwable e) {
                try {
                    removeFieldDescriptionsByVersion(tablespaceName, tableName, newSchemaVersion);
                } catch (Throwable ee) {
                    ee.addSuppressed(e);
                    throw ee;
                }
                throw e;
            }

            LOG.debug("Schema `{}`.`{}` created successfully", tablespaceName, tableName);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to create schema `" + tablespaceName + "`.`" + tableName + "`", e);
        }
    }

    private ScheduledExecutorService getScheduler() {
        ScheduledExecutorService scheduler = schedulerRef.get();
        if (null == scheduler || scheduler.isShutdown()) {
            throw new IllegalStateException("TiesServiceSchema scheduler was not registered or is already stopped.");
        }
        return scheduler;
    }

    public void start() {
        LOG.debug("Starting TiesServiceSchema...");
        runCleanerOnce();
        checkerRef.updateAndGet((checker) -> {
            if (null == checker) {
                LOG.debug("Scheduling TiesServiceSchema checker...");
                checker = getScheduler().scheduleAtFixedRate(new AllSchemaChecker(), 0, DEFAULT_CHECK_DELAY, DEFAULT_CHECK_DELAY_UNIT);
                LOG.debug("TiesServiceSchema checker scheduled");
            }
            return checker;
        });
        LOG.debug("TiesServiceSchema started");
    }

    private void runCleanerOnce() {
        if (!isCleanFailed) {
            runCleanerOnce(getScheduler(), new AllSchemaCleaner());
        }
    }

    private static synchronized void runCleanerOnce(ScheduledExecutorService scheduler, Runnable cleaner) {
        if (!isClean && !isCleanFailed) {
            try {
                scheduler.submit(cleaner).get();
                isClean = true;
            } catch (Throwable e) {
                LOG.warn("Schema cleaning failed", e);
                isCleanFailed = true;
            }
        }
    }

    public void stop() {
        LOG.trace("Stopping TiesServiceSchema...");

        ScheduledExecutorService scheduler = schedulerRef.getAndSet(null);
        if (null != scheduler) {
            LOG.trace("Stopping TiesServiceSchema scheduler...");
            scheduler.shutdown();
            LOG.trace("TiesServiceSchema scheduler stopped");
        }

        LOG.debug("TiesServiceSchema stopped");
    }

    public void init() {
        LOG.debug("Initializing TiesServiceSchema...");

        LOG.trace("Creating TiesServiceSchema scheduler");
        ScheduledExecutorService scheduler = //
                Executors.newScheduledThreadPool(EXECUTOR_CORE_POOL_SIZE, (r) -> new Thread(r, "TiesServiceSchemaUpdateScheduler"));
        LOG.trace("Registering TiesServiceSchema created scheduler");
        if (!schedulerRef.compareAndSet(null, scheduler)) {
            LOG.trace("TiesServiceSchema created scheduler registration failed");
            LOG.trace("Stopping TiesServiceSchema created scheduler...");
            scheduler.shutdownNow();
            LOG.trace("TiesServiceSchema created scheduler stopped");
            throw new IllegalStateException("TiesServiceSchema scheduler is already registered");
        }

        LOG.debug("TiesServiceSchema initialized");
    }

    public class AllSchemaChecker implements Runnable {

        @Override
        public void run() {
            LOG.trace("Checking for scheduled schema updates...");
            Date now = new Date();
            loadScheduledSchemaDescriptions(now, sd -> {
                try {
                    updateSchemaDescriptionStart(sd, now);
                    getScheduler().execute(new SchemaUpdater(sd));
                } catch (Throwable e) {
                    LOG.error("Can't start update schema: {}", sd, e);
                }
            });
            LOG.trace("Checking for schema update errors...");
            loadFailedSchemaDescriptions(sd -> {
                try {
                    updateSchemaDescriptionReschedule(sd, now, DEFAULT_ERROR_UPDATE_DELAY, DEFAULT_ERROR_UPDATE_DELAY_UNIT);
                } catch (Throwable e) {
                    LOG.error("Can't restart update schema: {}", sd, e);
                }
            });
            LOG.trace("All schema updates checked");
        }

    }

    public static class AllSchemaCleaner implements Runnable {

        @Override
        public void run() {
            LOG.debug("Cleaning for any schema update garbage...");
            Date now = new Date();
            loadUpdatingSchemaDescriptions(schema -> {
                try {
                    updateSchemaDescriptionError(schema, now);
                } catch (TiesServiceScopeException e) {
                    LOG.error("Can't clean schema state: {}", schema, e);
                }
            });
            LOG.debug("All schema update garbage removed");
        }

    }

    public class SchemaUpdater implements Runnable {

        private final SchemaDescription sd;

        public SchemaUpdater(SchemaDescription sd) {
            this.sd = sd;
        }

        @Override
        public void run() {
            LOG.debug("Start updating schema: {}", sd);
            Date now = new Date();
            try {
                if (sd.isDisabled()) {
                    LOG.debug("Update canceled for disabled schema: {}", sd);
                    return;
                }
                if (!sd.isUpdating()) {
                    LOG.debug("Update canceled for schema: {}", sd);
                    return;
                }

                LinkedList<FieldDescription> cachedDescriptions = new LinkedList<>();
                loadFieldDescriptions(sd.getTablespace(), sd.getTable(), cachedDescriptions::add);

                LinkedList<FieldDescription> contractDescriptions = new LinkedList<>();
                {
                    Table t = getTableFromSchema(sd.getTablespace(), sd.getTable());
                    for (String fieldName : t.getFieldNames()) {
                        Field f = t.getField(fieldName);
                        contractDescriptions.add(new FieldDescription(f.getName(), f.getType().toLowerCase()));
                    }
                }
                LOG.debug("Finished loading data for schema: {}", sd);
                if (cachedDescriptions.equals(contractDescriptions)) {
                    updateSchemaDescriptionSucces(sd, now, DEFAULT_UPDATE_DELAY, DEFAULT_UPDATE_DELAY_UNIT);
                    LOG.debug("Update succeeded with no changes for schema: {}", sd);
                } else {
                    try {
                        checkForInvalidModifications(cachedDescriptions, contractDescriptions);
                    } catch (Throwable e) {
                        throw new TiesServiceScopeException(
                                "Illegal schema `" + sd.getTablespace() + "`.`" + sd.getTable() + "` modification detected", e);
                    }
                    {
                        UUID oldSchemaVersion = sd.getVersion(), newSchemaVersion = UUID.randomUUID();
                        try {
                            for (FieldDescription fieldDescription : contractDescriptions) {
                                storeFieldDescription(sd.getTablespace(), sd.getTable(), newSchemaVersion, fieldDescription);
                            }
                            if (contractDescriptions.removeAll(cachedDescriptions) && !contractDescriptions.isEmpty()) {
                                refreshTiesDBStorage(sd.getTablespace(), sd.getTable(), contractDescriptions);
                            }
                            updateSchemaDescriptionSucces(sd, newSchemaVersion, now, DEFAULT_UPDATE_DELAY, DEFAULT_UPDATE_DELAY_UNIT);
                            removeFieldDescriptionsByVersion(sd.getTablespace(), sd.getTable(), oldSchemaVersion);
                        } catch (Throwable e) {
                            try {
                                removeFieldDescriptionsByVersion(sd.getTablespace(), sd.getTable(), newSchemaVersion);
                            } catch (Throwable ee) {
                                ee.addSuppressed(e);
                                throw ee;
                            }
                            throw e;
                        }
                    }
                }
            } catch (Throwable e) {
                LOG.error("Update failed for schema: {}", sd, e);
                try {
                    updateSchemaDescriptionError(sd, now);
                } catch (Throwable ee) {
                    LOG.error("Setting error state failed for schema: {}", sd, ee);
                }
            }
        }

    }

    private Table getTableFromSchema(String tablespaceName, String tableName) throws IllegalArgumentException {
        Tablespace ts = schema.getTablespace(tablespaceName);
        if (null == ts) {
            throw new IllegalArgumentException( //
                    "Tablespace `" + tablespaceName + "` was not found in contract");
        }
        Table t = ts.getTable(tableName);
        if (null == t) {
            throw new IllegalArgumentException( //
                    "Table `" + tablespaceName + "`.`" + tableName + "` was not found in contract");
        }
        return t;
    }

    private static void checkForInvalidModifications(LinkedList<FieldDescription> refList, LinkedList<FieldDescription> conList) {
        Iterator<FieldDescription> refIter = refList.iterator();
        Iterator<FieldDescription> conIter = conList.iterator();
        while (refIter.hasNext() && conIter.hasNext()) {
            FieldDescription ref = refIter.next();
            FieldDescription con;
            do {
                con = conIter.next();
            } while (!ref.equals(con) && conIter.hasNext());
        }
        if (refIter.hasNext()) {
            FieldDescription fieldDescription = refIter.next();
            throw new IllegalStateException(
                    "Field `" + fieldDescription.getName() + "`:" + fieldDescription.getType() + " was deleted from contract");
        }
    }

}
