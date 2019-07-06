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
package network.tiesdb.service.impl.elassandra.scope;

import static java.util.Objects.requireNonNull;
import static network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.ENTRY_HEADER;
import static network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.ENTRY_VERSION;
import static network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.getNameId;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UserType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.lib.crypto.digest.DigestManager;
import com.tiesdb.lib.crypto.digest.api.Digest;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.service.impl.elassandra.TiesServiceImpl;
import network.tiesdb.service.impl.elassandra.scope.db.ByteArrayType;
import network.tiesdb.service.impl.elassandra.scope.db.CassandraTypeHelper;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.FieldDescription;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchemaUtil.HeaderField;
import network.tiesdb.service.impl.elassandra.scope.db.TiesTypeHelper;
import network.tiesdb.service.scope.api.TiesEntryHeader;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeModification;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry.FieldValue;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry.FieldHash;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Filter;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Function;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Function.Argument;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Selector;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Result;
import network.tiesdb.service.scope.api.TiesServiceScopeResult;
import network.tiesdb.service.scope.api.TiesServiceScopeSchema;
import network.tiesdb.service.scope.api.TiesServiceScopeSchema.FieldSchema;

public class TiesServiceScopeImpl implements TiesServiceScope {

    private static final Logger LOG = LoggerFactory.getLogger(TiesServiceScopeImpl.class);

    private static abstract class ResultField implements Result.Field {

        private final FieldDescription dsc;

        public ResultField(FieldDescription dsc) {
            this.dsc = dsc;
        }

        @Override
        public String getName() {
            return dsc.getName();
        }

        @Override
        public String getType() {
            return dsc.getType();
        }

    }

    private static class ResultValueField extends ResultField implements Result.Field.ValueField {

        private final Object value;

        public ResultValueField(FieldDescription dsc, Object value) {
            super(dsc);
            this.value = value;
        }

        @Override
        public Object getFieldValue() {
            return value;
        }

        @Override
        public String toString() {
            return "ResultValueField [name=" + getName() + ", type=" + getType() + ", value" + printValue(value) + "]";
        }

    }

    private static class ResultRawField extends ResultField implements Result.Field.RawField {

        private final byte[] rawValue;

        public ResultRawField(FieldDescription dsc, byte[] rawValue) {
            super(dsc);
            this.rawValue = rawValue;
        }

        @Override
        public byte[] getRawValue() {
            return rawValue;
        }

        @Override
        public String toString() {
            return "ResultRawField [name=" + getName() + ", type=" + getType() + ", rawValue" + printHexValue(rawValue) + "]";
        }

        @Override
        public byte[] getHash() {
            Digest digest = DigestManager.getDigest(DigestManager.KECCAK_256);
            digest.update(getValue());
            byte[] hash = new byte[digest.getDigestSize()];
            digest.doFinal(hash);
            return hash;
        }

    }

    private static String printValue(Object value) {
        if (null == value) {
            return " is null";
        }
        if (value instanceof byte[]) {
            printHexValue((byte[]) value);
        }
        return value.toString();
    }

    private static String printHexValue(byte[] value) {
        if (null == value) {
            return " is null";
        }
        return "=0x" + formatHexValue(value);
    }

    private static String formatHexValue(byte[] value) {
        if (null == value) {
            return "";
        }
        if (value.length <= 64) {
            return DatatypeConverter.printHexBinary(value);
        } else {
            return DatatypeConverter.printHexBinary(Arrays.copyOfRange(value, 0, 32)) + "..." //
                    + DatatypeConverter.printHexBinary(Arrays.copyOfRange(value, value.length - 32, value.length)) //
                    + "(" + value.length + ")";
        }
    }

    private static class ResultHashField extends ResultField implements Result.Field.HashField {

        private final ByteBuffer hash;

        public ResultHashField(FieldDescription dsc, ByteBuffer hash) {
            super(dsc);
            this.hash = hash;
        }

        @Override
        public byte[] getHash() {
            return hash.array();
        }

        @Override
        public String toString() {
            return "ResultHashField [name=" + getName() + ", type=" + getType() + ", hash" + printHexValue(getHash()) + "]";
        }

    }

    @FunctionalInterface
    static interface CheckedAction<E extends Throwable> {

        void act() throws E;

    }

    @FunctionalInterface
    static interface CheckedSupplier<T, E extends Throwable> {

        T get() throws E;

        default CheckedSupplier<T, E> butFirst(CheckedAction<? extends E> before) {
            Objects.requireNonNull(before);
            return () -> {
                before.act();
                return get();
            };
        }

        default CheckedSupplier<T, E> andThen(CheckedAction<? extends E> after) {
            Objects.requireNonNull(after);
            return () -> {
                T result = get();
                after.act();
                return result;
            };
        }
    }

    @FunctionalInterface
    private static interface CheckedPredicate<T, E extends Throwable> {

        boolean test(T t) throws E;

    }

    private final TiesServiceImpl service;

    public TiesServiceScopeImpl(TiesServiceImpl service) {
        this.service = service;
        LOG.debug(this + " is opened");
    }

    @Override
    public void close() throws IOException {
        LOG.debug(this + " is closed");
    }

    @Override
    public TiesVersion getServiceVersion() {
        return service.getVersion();
    }

    private static String format(String type, byte[] data) {
        switch (type.toUpperCase()) {
        case "STRING":
            return "\"" + new String(data) + "\"";
        default:
            return formatHexValue(data);
        }
    }

    private static String createValuePlaceholders(int size) {
        StringBuilder sb = new StringBuilder();
        while (size-- > 0) {
            sb.append(", ?");
        }
        return sb.substring(2);
    }

    private static String concat(Collection<String> fieldNames, String delim) {
        if (fieldNames.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (String name : fieldNames) {
            sb.append(delim);
            sb.append('"');
            sb.append(name);
            sb.append('"');
        }
        return sb.substring(delim.length());
    }

    private static void addHeader(TiesEntryHeader tiesEntryHeader, List<String> fieldNames, List<Object> fieldValues, CFMetaData cfMetaData)
            throws TiesServiceScopeException {
        fieldNames.add(ENTRY_HEADER);
        ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(ENTRY_HEADER, true));
        if (null == columnDefinition) {
            throw new TiesServiceScopeException("No " + ENTRY_HEADER + " column found");
        }
        if (!(columnDefinition.type instanceof UserType)) {
            throw new TiesServiceScopeException("Type of " + ENTRY_HEADER + " column should be UserType");
        }
        UserType type = (UserType) columnDefinition.type;
        ByteBuffer[] components = new ByteBuffer[type.size()];
        for (int i = 0; i < type.size(); i++) {
            @SuppressWarnings("unchecked")
            AbstractType<Object> fieldType = (AbstractType<Object>) type.fieldType(i);
            Object fieldFormattedValue = TiesTypeHelper.formatToCassandraType(getHeaderField(tiesEntryHeader, type.fieldNameAsString(i)),
                    fieldType);
            if (null != fieldFormattedValue) {
                LOG.debug("FormattedValue {} ({})", fieldFormattedValue, fieldFormattedValue.getClass());
            } else {
                LOG.debug("FormattedValue null");
            }
            components[i] = fieldType.decompose(fieldFormattedValue);
        }
        fieldValues.add(UserType.buildValue(components));

        // Add entry version
        fieldNames.add(ENTRY_VERSION);
        fieldValues.add(tiesEntryHeader.getEntryVersion());
    }

    private static Object getHeaderField(TiesEntryHeader h, String name) throws TiesServiceScopeException {
        try {
            HeaderField headerField = HeaderField.valueOfIgnoreCase(name.toUpperCase());
            switch (headerField) {
            case TIM:
                return h.getEntryTimestamp();
            case OHS:
                return h.getEntryOldHash();
            case FHS:
                return h.getEntryFldHash();
            case NET:
                return h.getEntryNetwork();
            case SNR:
                return h.getSigner();
            case SIG:
                return h.getSignature();
            case HSH:
                return h.getHash();
            default:
                throw new TiesServiceScopeException("Unknown header field" + headerField);
            }
        } catch (Exception e) {
            throw new TiesServiceScopeException("Can't get header field " + name, e);
        }
    }

    @SafeVarargs
    private static <T, E extends Throwable> T retry(CheckedSupplier<T, E>... suppliers) throws E {
        return retry((T o) -> null != o, suppliers);
    }

    @SafeVarargs
    private static <T, E extends Throwable> T retry(CheckedPredicate<T, E> condition, CheckedSupplier<T, E>... suppliers) throws E {
        T result = null;
        for (CheckedSupplier<T, E> supplier : suppliers) {
            result = supplier.get();
            if (condition.test(result)) {
                break;
            }
        }
        return result;
    }

    private void refreshSchema(String tablespaceName, String tableName) {
        try {
            service.getSchemaImpl().refreshSchema(tablespaceName, tableName);
        } catch (Throwable e) {
            LOG.warn("Schema `{}`.`{}` refreshing error", tablespaceName, tableName, e);
        }
    }

    private void createSchema(String tablespaceName, String tableName) {
        try {
            service.getSchemaImpl().createSchema(tablespaceName, tableName);
        } catch (Throwable e) {
            LOG.warn("Schema `{}`.`{}` creation error", tablespaceName, tableName, e);
        }
    }

    @Override
    public void insert(TiesServiceScopeModification modificationRequest) throws TiesServiceScopeException {

        TiesServiceScopeModification.Entry entry = modificationRequest.getEntry();
        String tablespaceName = entry.getTablespaceName();
        String tableName = entry.getTableName();
        LOG.debug("Insert into `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CheckedSupplier<CFMetaData, RuntimeException> getSchema = () -> {
            return Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);
        };
        CFMetaData cfMetaData = retry(//
                getSchema, //
                getSchema.butFirst(() -> createSchema(tablespaceName, tableName)));

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        Map<String, Entry.FieldValue> entryFields = entry.getFieldValues();

        if (entryFields.isEmpty()) {
            return;
        }

        ArrayList<String> partKeyColumnsNames;
        {
            List<ColumnDefinition> partKeyColumns = cfMetaData.partitionKeyColumns();
            partKeyColumnsNames = new ArrayList<>(partKeyColumns.size());
            for (ColumnDefinition columnDefinition : partKeyColumns) {
                partKeyColumnsNames.add(columnDefinition.name.toString().toUpperCase());
            }
        }

        List<String> keyNames = new ArrayList<>(partKeyColumnsNames.size());
        List<Object> keyValues = new ArrayList<>(keyNames.size());
        List<String> fieldNames = new ArrayList<>(entryFields.size());
        List<Object> fieldValues = new ArrayList<>(entryFields.size());

        addHeader(entry.getHeader(), fieldNames, fieldValues, cfMetaData);

        for (String fieldName : entryFields.keySet()) {

            String fieldNameId = getNameId("FLD", fieldName);
            String fieldHashNameId = getNameId("HSH", fieldName);
            String fieldValueNameId = getNameId("VAL", fieldName);

            CheckedSupplier<ColumnDefinition, RuntimeException> getColumn = () -> {
                return cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            };
            ColumnDefinition columnDefinition = retry( //
                    getColumn, //
                    getColumn.butFirst(() -> refreshSchema(tablespaceName, tableName)));

            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            Entry.FieldValue fieldValue = entryFields.get(fieldName);
            requireNonNull(fieldValue);
            LOG.debug("Field {} ({}) Value ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            Object fieldFormattedValue = TiesTypeHelper.formatToCassandraType(fieldValue.get(),
                    columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
            if (null != fieldFormattedValue) {
                LOG.debug("FormattedValue {} ({})", fieldFormattedValue, fieldFormattedValue.getClass());
            } else {
                LOG.debug("FormattedValue null");
            }

            if (partKeyColumnsNames.remove(fieldNameId)) {
                LOG.debug("KeyField {}", fieldName);
                keyNames.add(fieldNameId);
                keyValues.add(fieldFormattedValue);
                fieldNames.add(fieldHashNameId);
                fieldValues.add(ByteBuffer.wrap(fieldValue.getHash()));
                fieldNames.add(fieldValueNameId);
                fieldValues.add(ByteBuffer.wrap(fieldValue.getBytes()));
            } else {
                LOG.debug("DataField {}", fieldName);
                fieldNames.add(fieldNameId);
                fieldValues.add(fieldFormattedValue);
                // fieldValues.add(columnType.compose(ByteBuffer.wrap(fieldValue.getBytes())));
                fieldNames.add(fieldHashNameId);
                fieldValues.add(ByteBuffer.wrap(fieldValue.getHash()));
                fieldNames.add(fieldValueNameId);
                fieldValues.add(ByteBuffer.wrap(fieldValue.getBytes()));
            }
        }

        UntypedResultSet result = retry(//
                r -> !(null == r || r.isEmpty() || r.size() > 1) && r.one().getBoolean("[applied]"), //
                () -> {
                    List<String> allNames = new LinkedList<>();
                    allNames.addAll(keyNames);
                    allNames.addAll(fieldNames);

                    List<Object> allValues = new LinkedList<>();
                    allValues.addAll(keyValues);
                    allValues.addAll(fieldValues);

                    String query = String.format("INSERT INTO \"%s\".\"%s\"\n" //
                            + "(%s)\n" //
                            + "VALUES (%s)\n" //
                            + "IF NOT EXISTS", //
                            tablespaceNameId, tableNameId, //
                            concat(allNames, ", "), //
                            createValuePlaceholders(allNames.size()));
                    LOG.debug("Insert query {}", query);

                    UntypedResultSet insertResult = QueryProcessor.execute(query, ConsistencyLevel.ALL, allValues.toArray());
                    if (LOG.isTraceEnabled()) {
                        for (UntypedResultSet.Row row : insertResult) {
                            LOG.trace("Insert result row {}", row);
                            for (ColumnSpecification col : row.getColumns()) {
                                LOG.trace("Insert result row col {} = {}", col.name, col.type.compose(row.getBlob(col.name.toString())));
                            }
                        }
                    }
                    return insertResult;
                }, () -> {
                    LOG.trace("Insert failed trying to upsert...");
                    List<Object> allValues = new LinkedList<>();
                    allValues.addAll(fieldValues);
                    allValues.addAll(keyValues);

                    String query = String.format("UPDATE \"%s\".\"%s\"" //
                            + " SET %s = ?" //
                            + " WHERE %s = ?" //
                            + " IF \"" + ENTRY_VERSION + "\" = 0", //
                            tablespaceNameId, tableNameId, //
                            concat(fieldNames, " = ?, "), //
                            concat(keyNames, " = ? AND ") //
                    );
                    LOG.debug("Upsert query {}", query);

                    UntypedResultSet insertResult = QueryProcessor.execute(query, ConsistencyLevel.ALL, allValues.toArray());
                    if (LOG.isDebugEnabled()) {
                        for (UntypedResultSet.Row row : insertResult) {
                            LOG.debug("Upsert result row {}", row);
                            for (ColumnSpecification col : row.getColumns()) {
                                ByteBuffer bytes = row.getBlob(col.name.toString());
                                LOG.debug("Upsert result row col {} = {}", col.name, (null == bytes ? null : col.type.compose(bytes)));
                            }
                        }
                    }
                    return insertResult;
                });

        if (result.isEmpty()) {
            throw new TiesServiceScopeException("No insertion result found");
        } else if (result.size() > 1) {
            throw new TiesServiceScopeException("Multiple insertion results found");
        } else if (!result.one().getBoolean("[applied]")) {
            {
                partKeyColumnsNames.removeAll(fieldNames);
                if (!partKeyColumnsNames.isEmpty()) {
                    LOG.debug("Missing values for {}", partKeyColumnsNames);
                    Map<String, String> emptyNames = new HashMap<>();

                    TiesSchemaUtil.loadFieldDescriptions(tablespaceName, tableName, fd -> {
                        emptyNames.put(getNameId("FLD", fd.getName()), fd.getName());
                    });

                    List<String> missingKeys = new LinkedList<>();
                    for (String fieldNameId : partKeyColumnsNames) {
                        missingKeys.add(emptyNames.get(fieldNameId));
                    }
                    throw new TiesServiceScopeException(
                            "Missing key fields for `" + tablespaceName + "`.`" + tableName + "`: " + missingKeys);
                }
            }
            throw new TiesServiceScopeException("Insertion failed");
        }
        modificationRequest.setResult(new TiesServiceScopeModification.Result.Success() {
            @Override
            public byte[] getHeaderHash() {
                return entry.getHeader().getHash();
            }
        });
    }

    @Override
    public void update(TiesServiceScopeModification modificationRequest) throws TiesServiceScopeException {

        TiesServiceScopeModification.Entry entry = modificationRequest.getEntry();

        String tablespaceName = entry.getTablespaceName();
        String tableName = entry.getTableName();
        LOG.debug("Update in `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Update failed");
        }

        Map<String, Entry.FieldValue> entryFieldValues = entry.getFieldValues();
        Map<String, Entry.FieldHash> entryFieldHashes = entry.getFieldHashes();
        ArrayList<String> partKeyColumnsNames;
        {
            List<ColumnDefinition> partKeyColumns = cfMetaData.partitionKeyColumns();
            partKeyColumnsNames = new ArrayList<>(partKeyColumns.size());
            for (ColumnDefinition columnDefinition : partKeyColumns) {
                partKeyColumnsNames.add(columnDefinition.name.toString().toUpperCase());
            }
        }

        Map<String, String> emptyNames;
        {
            Collection<ColumnDefinition> columns = cfMetaData.allColumns();

            ArrayList<String> columnNames = new ArrayList<>(columns.size());
            for (ColumnDefinition c : columns) {
                columnNames.add(c.name.toString());
            }

            Map<String, String> names = new HashMap<>();
            TiesSchemaUtil.loadFieldDescriptions(tablespaceName, tableName, fd -> {
                names.put(getNameId("FLD", fd.getName()), fd.getName());
            });

            if (!columnNames.containsAll(names.keySet())) {
                LOG.warn("Fields count missmatch. TiesDB schema need to be updated.");
            }

            emptyNames = names;
        }

        List<String> keyNames = new ArrayList<>(partKeyColumnsNames.size());
        List<Object> keyValues = new ArrayList<>(keyNames.size());
        List<String> fieldNames = new ArrayList<>(entryFieldValues.size());
        List<Object> fieldValues = new ArrayList<>(fieldNames.size());
        List<String> hashNames = new ArrayList<>(entryFieldHashes.size());
        List<Object> hashValues = new ArrayList<>(hashNames.size());

        addHeader(entry.getHeader(), fieldNames, fieldValues, cfMetaData);

        for (Map.Entry<String, FieldValue> entryField : entryFieldValues.entrySet()) {

            String fieldName = entryField.getKey();
            String fieldNameId = getNameId("FLD", fieldName);
            String fieldHashNameId = getNameId("HSH", fieldName);
            String fieldValueNameId = getNameId("VAL", fieldName);

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            emptyNames.remove(fieldNameId);

            Entry.FieldValue fieldValue = entryField.getValue();
            requireNonNull(fieldValue);
            LOG.debug("Field {} ({}) RawValue ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            Object fieldFormattedValue = TiesTypeHelper.formatToCassandraType(fieldValue.get(),
                    columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
            if (null != fieldFormattedValue) {
                LOG.debug("FormattedValue {} ({})", fieldFormattedValue, fieldFormattedValue.getClass());
            } else {
                LOG.debug("FormattedValue null");
            }

            if (partKeyColumnsNames.remove(fieldNameId)) {
                LOG.debug("KeyField {}", fieldName);
                keyNames.add(fieldNameId);
                keyValues.add(fieldFormattedValue);
            } else {
                LOG.debug("DataField {}", fieldName);
                fieldNames.add(fieldNameId);
                fieldValues.add(fieldFormattedValue);
                fieldNames.add(fieldHashNameId);
                fieldValues.add(ByteBuffer.wrap(fieldValue.getHash()));
                fieldNames.add(fieldValueNameId);
                fieldValues.add(ByteBuffer.wrap(fieldValue.getBytes()));
            }
        }

        for (Map.Entry<String, FieldHash> entryField : entryFieldHashes.entrySet()) {

            String fieldName = entryField.getKey();
            String fieldNameId = getNameId("FLD", fieldName);
            String fieldHashNameId = getNameId("HSH", fieldName);

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldHashNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException(
                        "FieldHash `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            emptyNames.remove(fieldNameId);

            Entry.FieldHash fieldHash = entryField.getValue();
            requireNonNull(fieldHash);
            LOG.debug("HashField {} ({}) RawHash {}", fieldName, fieldNameId, format("BYTES", fieldHash.getHash()));

            hashNames.add(fieldHashNameId);
            hashValues.add(ByteBuffer.wrap(fieldHash.getHash()));
        }

        if (!partKeyColumnsNames.isEmpty()) {
            LOG.debug("Missing values for {}", partKeyColumnsNames);
            List<String> missingKeys = new LinkedList<>();
            for (String fieldNameId : partKeyColumnsNames) {
                missingKeys.add(emptyNames.get(fieldNameId));
            }
            throw new TiesServiceScopeException("Missing key fields for `" + tablespaceName + "`.`" + tableName + "`: " + missingKeys);
        }

        String query = String.format(//
                "UPDATE \"%s\".\"%s\"" + //
                        " SET %s = ?" + //
                        " WHERE %s = ?" + //
                        " IF \"" + ENTRY_HEADER + "\"." + HeaderField.HSH.name().toLowerCase() + " = ?" + //
                        " AND \"" + ENTRY_VERSION + "\" = ?" + //
                        "%s" + //
                        "%s", //
                tablespaceNameId, tableNameId, //
                concat(fieldNames, " = ?, "), //
                concat(keyNames, " = ? AND "), //
                (hashNames.isEmpty() ? "" : String.format(" AND %s = ?", concat(hashNames, " = ? AND "))), //
                (emptyNames.isEmpty() ? "" : String.format(" AND %s = NULL", concat(emptyNames.keySet(), " = NULL AND ")))//
        );
        LOG.debug("Update query {}", query);

        fieldValues.addAll(keyValues);
        fieldValues.add(TiesTypeHelper.formatToCassandraType(entry.getHeader().getEntryOldHash(), BytesType.instance));
        fieldValues.add(entry.getHeader().getEntryVersion().subtract(BigInteger.ONE));
        fieldValues.addAll(hashValues);

        UntypedResultSet result = QueryProcessor.execute(query, ConsistencyLevel.ALL, fieldValues.toArray());
        LOG.debug("Update result {}", result);
        if (LOG.isDebugEnabled()) {
            for (UntypedResultSet.Row row : result) {
                LOG.debug("Update result row {}", row);
                for (ColumnSpecification col : row.getColumns()) {
                    ByteBuffer bytes = row.getBlob(col.name.toString());
                    LOG.debug("Update result row col {} = {}", col.name, (null == bytes ? null : col.type.compose(bytes)));
                }
            }
        }
        if (result.isEmpty()) {
            throw new TiesServiceScopeException("No update result found");
        } else if (result.size() > 1) {
            throw new TiesServiceScopeException("Multiple updates results found");
        } else if (!result.one().getBoolean("[applied]")) {
            throw new TiesServiceScopeException("Update failed for " + entry);
        }
        modificationRequest.setResult(new TiesServiceScopeModification.Result.Success() {
            @Override
            public byte[] getHeaderHash() {
                return entry.getHeader().getHash();
            }
        });
    }

    @Override
    public void delete(TiesServiceScopeModification modificationRequest) throws TiesServiceScopeException {

        TiesServiceScopeModification.Entry entry = modificationRequest.getEntry();

        String tablespaceName = entry.getTablespaceName();
        String tableName = entry.getTableName();
        LOG.debug("Delete from `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Delete failed");
        }

        Map<String, Entry.FieldValue> entryFieldValues = entry.getFieldValues();
        Map<String, Entry.FieldHash> entryFieldHashes = entry.getFieldHashes();
        ArrayList<String> partKeyColumnsNames;
        {
            List<ColumnDefinition> partKeyColumns = cfMetaData.partitionKeyColumns();
            partKeyColumnsNames = new ArrayList<>(partKeyColumns.size());
            for (ColumnDefinition columnDefinition : partKeyColumns) {
                partKeyColumnsNames.add(columnDefinition.name.toString().toUpperCase());
            }
        }

        Map<String, String> emptyNames;
        {
            Collection<ColumnDefinition> columns = cfMetaData.allColumns();

            ArrayList<String> columnNames = new ArrayList<>(columns.size());
            for (ColumnDefinition c : columns) {
                columnNames.add(c.name.toString());
            }

            Map<String, String> names = new HashMap<>();
            TiesSchemaUtil.loadFieldDescriptions(tablespaceName, tableName, fd -> {
                names.put(getNameId("FLD", fd.getName()), fd.getName());
            });

            if (!columnNames.containsAll(names.keySet())) {
                LOG.warn("Fields count missmatch. TiesDB schema need to be updated.");
            }

            emptyNames = names;
        }

        List<String> keyNames = new ArrayList<>(partKeyColumnsNames.size());
        List<Object> keyValues = new ArrayList<>(keyNames.size());
        List<String> fieldNames = new ArrayList<>(entryFieldValues.size());
        List<Object> fieldValues = new ArrayList<>(fieldNames.size());

        addHeader(entry.getHeader(), fieldNames, fieldValues, cfMetaData);

        if (!entryFieldHashes.entrySet().isEmpty()) {
            throw new TiesServiceScopeException("Deletion Entry should have only key values");
        }

        for (Map.Entry<String, FieldValue> entryField : entryFieldValues.entrySet()) {

            String fieldName = entryField.getKey();
            String fieldNameId = getNameId("FLD", fieldName);

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            emptyNames.remove(fieldNameId);

            Entry.FieldValue fieldValue = entryField.getValue();
            requireNonNull(fieldValue);
            LOG.debug("Field {} ({}) RawValue ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            if (partKeyColumnsNames.remove(fieldNameId)) {
                LOG.debug("KeyField {}", fieldName);
                keyNames.add(fieldNameId);
                Object fieldFormattedValue = TiesTypeHelper.formatToCassandraType(fieldValue.get(),
                        columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
                if (null != fieldFormattedValue) {
                    LOG.debug("FormattedValue {} ({})", fieldFormattedValue, fieldFormattedValue.getClass());
                } else {
                    LOG.debug("FormattedValue null");
                }
                keyValues.add(fieldFormattedValue);
            } else {
                throw new TiesServiceScopeException("Deletion Entry should have only key values");
            }
        }

        {
            ByteBuffer emptyValue = null;
            Iterator<Map.Entry<String, String>> it = emptyNames.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> emptyField = it.next();
                it.remove();

                String fieldName = emptyField.getValue();
                String fieldNameId = emptyField.getKey();
                String fieldHashNameId = getNameId("HSH", fieldName);
                String fieldValueNameId = getNameId("VAL", fieldName);

                fieldNames.add(fieldNameId);
                fieldValues.add(emptyValue);
                fieldNames.add(fieldHashNameId);
                fieldValues.add(emptyValue);
                fieldNames.add(fieldValueNameId);
                fieldValues.add(emptyValue);
            }
        }

        if (!partKeyColumnsNames.isEmpty()) {
            LOG.debug("Missing values for {}", partKeyColumnsNames);
            List<String> missingKeys = new LinkedList<>();
            for (String fieldNameId : partKeyColumnsNames) {
                missingKeys.add(emptyNames.get(fieldNameId));
            }
            throw new TiesServiceScopeException("Missing key fields for `" + tablespaceName + "`.`" + tableName + "`: " + missingKeys);
        }

        String query = String.format("UPDATE \"%s\".\"%s\"" //
                + " SET %s = ?" //
                + " WHERE %s = ?" //
                + " IF \"" + ENTRY_HEADER + "\"." + HeaderField.HSH.name().toLowerCase() + " = ?" //
                + " AND \"" + ENTRY_VERSION + "\" > 0", //
                tablespaceNameId, tableNameId, //
                concat(fieldNames, " = ?, "), //
                concat(keyNames, " = ? AND ")//
        );
        LOG.debug("Delete query {}", query);

        fieldValues.addAll(keyValues);
        fieldValues.add(TiesTypeHelper.formatToCassandraType(entry.getHeader().getEntryOldHash(), BytesType.instance));

        UntypedResultSet result = QueryProcessor.execute(query, ConsistencyLevel.ALL, fieldValues.toArray());
        LOG.debug("Delete result {}", result);
        if (LOG.isDebugEnabled()) {
            for (UntypedResultSet.Row row : result) {
                LOG.debug("Delete result row {}", row);
                for (ColumnSpecification col : row.getColumns()) {
                    ByteBuffer bytes = row.getBlob(col.name.toString());
                    LOG.debug("Delete result row col {} = {}", col.name, (null == bytes ? null : col.type.compose(bytes)));
                }
            }
        }
        if (result.isEmpty()) {
            throw new TiesServiceScopeException("No delete result found");
        } else if (result.size() > 1) {
            throw new TiesServiceScopeException("Multiple delete results found");
        } else if (!result.one().getBoolean("[applied]")) {
            throw new TiesServiceScopeException("Delete failed");
        }
        modificationRequest.setResult(new TiesServiceScopeModification.Result.Success() {
            @Override
            public byte[] getHeaderHash() {
                return entry.getHeader().getHash();
            }
        });
    }

    @Override
    public void select(TiesServiceScopeRecollection recollectionRequest) throws TiesServiceScopeException {

        Query request = recollectionRequest.getQuery();

        String tablespaceName = request.getTablespaceName();
        String tableName = request.getTableName();
        LOG.debug("Select from `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            LOG.debug("Table `{}`.`{}` does not exist or not yet created", tablespaceName, tableName);
            return;
        }

        List<Object> qv = new LinkedList<>();
        StringBuilder qb = new StringBuilder();

        Argument.Visitor<?> argVisitor = new Argument.Visitor<Void>() {

            @Override
            public Void on(Argument.FunctionArgument a) throws TiesServiceScopeException {
                forFunction(this, qb, a);
                return null;
            }

            @Override
            public Void on(Argument.ValueArgument a) throws TiesServiceScopeException {
                qv.add(a.getValue());
                qb.append('?');
                return null;
            }

            @Override
            public Void on(Argument.FieldArgument a) {
                qb.append('"');
                qb.append(getNameId("FLD", a.getFieldName()));
                qb.append('"');
                return null;
            }

        };

        List<FieldDescription> tiesFields;
        ArrayList<String> tableColumnNames;
        {
            Collection<ColumnDefinition> columns = cfMetaData.allColumns();

            ArrayList<String> columnNames = new ArrayList<>(columns.size());
            for (ColumnDefinition c : columns) {
                columnNames.add(c.name.toString());
            }

            List<FieldDescription> fields = new LinkedList<>();
            TiesSchemaUtil.loadFieldDescriptions(tablespaceName, tableName, fields::add);

            ArrayList<String> fieldNameIds = new ArrayList<>(fields.size());
            for (FieldDescription fd : fields) {
                fieldNameIds.add(getNameId("FLD", fd.getName()));
            }

            if (!columnNames.containsAll(fieldNameIds)) {
                LOG.warn("Fields count missmatch. TiesDB schema need to be updated.");
            }
            tableColumnNames = columnNames;
            tiesFields = fields;
        }

        Map<FieldDescription, String> fieldMap = new HashMap<>();

        AtomicInteger tiesComputesCounter = new AtomicInteger(0);
        List<FieldDescription> tiesComputes = new LinkedList<>();
        Map<FieldDescription, String> aliasMap = new HashMap<>();

        List<Selector> selectors = request.getSelectors();
        qb.append("select ");
        qb.append('"');
        qb.append(ENTRY_HEADER);
        qb.append("\",\"");
        qb.append(ENTRY_VERSION);
        qb.append("\",");
        if (!selectors.isEmpty()) {
            Map<String, String> selectedFields = new HashMap<>();

            for (Selector sel : selectors) {
                if (sel.accept(new Selector.Visitor<Boolean>() {

                    @Override
                    public Boolean on(Selector.FunctionSelector s) throws TiesServiceScopeException {
                        String aliasName = s.getAlias();
                        aliasName = null != aliasName ? aliasName : s.getName();
                        String aliasNameId = "COM" + tiesComputesCounter.incrementAndGet();
                        FieldDescription fd = new FieldDescription(aliasName, s.getType());
                        tiesComputes.add(fd);
                        aliasMap.put(fd, aliasNameId);
                        forFunction(argVisitor, qb, s);
                        qb.append(" as \"");
                        qb.append(aliasNameId);
                        qb.append('"');
                        return true;
                    }

                    @Override
                    public Boolean on(Selector.FieldSelector s) {
                        String fieldName = s.getFieldName();
                        String fieldNameId = getNameId("VAL", fieldName);
                        if (!tableColumnNames.contains(fieldNameId)) {
                            return false;
                        }
                        qb.append('"');
                        qb.append(fieldNameId);
                        qb.append('"');
                        selectedFields.put(fieldName, fieldNameId);
                        return true;
                    }

                })) {
                    qb.append(',');
                }
            }
            for (FieldDescription field : tiesFields) {
                String fieldNameId = selectedFields.remove(field.getName());
                if (null == fieldNameId) {
                    fieldNameId = getNameId("HSH", field.getName());
                }
                if (!tableColumnNames.contains(fieldNameId)) {
                    continue;
                }
                qb.append('"');
                qb.append(fieldNameId);
                qb.append('"');
                qb.append(',');
                fieldMap.put(field, fieldNameId);
            }

            if (!selectedFields.isEmpty()) {
                LOG.warn("Select has unmapped fields: {}", selectedFields);
            }

        } else {
            for (FieldDescription field : tiesFields) {
                String fieldNameId = getNameId("VAL", field.getName());
                qb.append('"');
                qb.append(fieldNameId);
                qb.append('"');
                qb.append(',');
                fieldMap.put(field, fieldNameId);
            }
        }
        qb.setLength(qb.length() - 1);

        qb.append(" from \"");
        qb.append(tablespaceNameId);
        qb.append("\".\"");
        qb.append(tableNameId);
        qb.append("\"");

        qb.append(" where \"");
        List<Filter> filters = request.getFilters();
        if (!filters.isEmpty()) {
            for (Filter filter : filters) {
                forFilter(argVisitor, qb, filter);
                qb.append(" and ");
            }
            // qb.setLength(qb.length() - 5);
            qb.append("\"");
        }

        qb.append(ENTRY_VERSION);
        qb.append("\" > 0");

        qb.append(" ALLOW FILTERING");

        LOG.debug("{}", qb);

        try {
            UntypedResultSet result = QueryProcessor.execute(qb.toString(), ConsistencyLevel.ALL, qv.toArray());
            LOG.debug("Select result {}", result);
            if (LOG.isDebugEnabled()) {
                for (UntypedResultSet.Row row : result) {
                    for (ColumnSpecification col : row.getColumns()) {
                        ByteBuffer bytes = row.getBlob(col.name.toString());
                        LOG.debug("Select result {}({}) = {}", col.name.toString(), col.type.getClass().getSimpleName().toString(),
                                (null == bytes ? null : prettyPrint(col.type.compose(bytes))));
                    }
                }
            }
            List<Result.Entry> entryList = new LinkedList<>();
            for (UntypedResultSet.Row row : result) {
                entryList.add(newResult(row, newEntryHeader(row, cfMetaData), tiesFields, tiesComputes, fieldMap, aliasMap));
            }
            recollectionRequest.setResult(new Result() {

                @Override
                public List<Entry> getEntries() {
                    return entryList;
                }

            });
        } catch (Throwable th) {
            LOG.debug("Failed to execute recollection request: {}", recollectionRequest.getMessageId(), th);
            if (th.getCause().getMessage().startsWith("Undefined column name")) {
                recollectionRequest.setResult(new Result() {
                    @Override
                    public List<Entry> getEntries() {
                        return Collections.emptyList();
                    }
                });
            } else {
                throw th;
            }
        }
    }

    private static final class TiesServiceScopeExceptionWrapper extends RuntimeException {

        private static final long serialVersionUID = -7205017938363533519L;

        private final TiesServiceScopeException cause;

        public TiesServiceScopeExceptionWrapper(TiesServiceScopeException cause) {
            super(cause);
            this.cause = cause;
        }

        public TiesServiceScopeException unwrap() {
            return cause;
        }

    }

    @Override
    public void schema(TiesServiceScopeSchema schemaRequest) throws TiesServiceScopeException {

        String tablespaceName = schemaRequest.getTablespaceName();
        String tableName = schemaRequest.getTableName();
        LOG.debug("Schema for `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);
        if (null == cfMetaData) {
            return;
        }

        HashSet<String> partKeyColumnsNameIds;
        {
            List<ColumnDefinition> partKeyColumns = cfMetaData.partitionKeyColumns();
            partKeyColumnsNameIds = new HashSet<>(partKeyColumns.size());
            for (ColumnDefinition columnDefinition : partKeyColumns) {
                partKeyColumnsNameIds.add(columnDefinition.name.toString().toUpperCase());
            }
        }

        try {
            List<FieldSchema.Field> fieldList = new LinkedList<>();
            TiesSchemaUtil.loadFieldDescriptions(tablespaceName, tableName, fd -> {
                LOG.debug("Field `{}`.`{}`.`{}`:{}", tablespaceName, tableName, fd.getName(), fd.getType());
                fieldList.add(new FieldSchema.Field() {

                    private final boolean primary = partKeyColumnsNameIds.contains(getNameId("FLD", fd.getName()));

                    @Override
                    public String getFieldType() {
                        return fd.getName();
                    }

                    @Override
                    public String getFieldName() {
                        return fd.getType();
                    }

                    @Override
                    public boolean isPrimary() {
                        return primary;
                    }
                });
                try {
                    schemaRequest.setResult(new FieldSchema() {

                        @Override
                        public List<Field> getFields() {
                            return fieldList;
                        }

                    });
                } catch (TiesServiceScopeException e) {
                    throw new TiesServiceScopeExceptionWrapper(e);
                }
            });
        } catch (TiesServiceScopeExceptionWrapper e) {
            throw e.unwrap();
        }

    }

    private static TiesEntryHeader newEntryHeader(Row row, CFMetaData cfMetaData) throws TiesServiceScopeException {
        ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(ENTRY_HEADER, true));
        if (null == columnDefinition) {
            throw new TiesServiceScopeException("No " + ENTRY_HEADER + " column found");
        }
        if (!(columnDefinition.type instanceof UserType)) {
            throw new TiesServiceScopeException("Type of " + ENTRY_HEADER + " column should be UserType");
        }

        UserType type = (UserType) columnDefinition.type;
        ByteBuffer[] components = type.split(row.getBlob(ENTRY_HEADER));

        byte[] snr = newEntryHeaderField(type, components, HeaderField.SNR, ByteArrayType.instance);
        byte[] sig = newEntryHeaderField(type, components, HeaderField.SIG, ByteArrayType.instance);
        byte[] ohs = newEntryHeaderField(type, components, HeaderField.OHS, ByteArrayType.instance);
        byte[] fhs = newEntryHeaderField(type, components, HeaderField.FHS, ByteArrayType.instance);
        byte[] hsh = newEntryHeaderField(type, components, HeaderField.HSH, ByteArrayType.instance);
        BigInteger ver = IntegerType.instance.compose(row.getBlob(ENTRY_VERSION));
        Date tim = newEntryHeaderField(type, components, HeaderField.TIM, TimestampType.instance);
        Short net = newEntryHeaderField(type, components, HeaderField.NET, ShortType.instance);

        return new TiesEntryHeader() {

            @Override
            public byte[] getSigner() {
                return snr;
            }

            @Override
            public byte[] getSignature() {
                return sig;
            }

            @Override
            public byte[] getHash() {
                return hsh;
            }

            @Override
            public BigInteger getEntryVersion() {
                return ver;
            }

            @Override
            public Date getEntryTimestamp() {
                return tim;
            }

            @Override
            public byte[] getEntryOldHash() {
                return ohs;
            }

            @Override
            public short getEntryNetwork() {
                return net;
            }

            @Override
            public byte[] getEntryFldHash() {
                return fhs;
            }

        };

    }

    private static <T> T newEntryHeaderField(UserType type, ByteBuffer[] components, HeaderField field, AbstractType<T> format) {
        return format.compose(components[type.fieldPosition(FieldIdentifier.forUnquoted(field.name()))]);
    }

    private static Result.Entry newResult(Row row, TiesEntryHeader entryHeader, List<FieldDescription> tiesFields,
            List<FieldDescription> tiesComputes, Map<FieldDescription, String> fieldMap, Map<FieldDescription, String> aliasMap)
            throws TiesServiceScopeException {

        List<Result.Field> entryFields = new LinkedList<>();
        List<Result.Field> computedFields = new LinkedList<>();
        {

            for (FieldDescription tiesFieldDescription : tiesFields) {
                String fieldNameId = fieldMap.get(tiesFieldDescription);
                ByteBuffer bytes = row.getBlob(fieldNameId);
                if (null == bytes) {
                    continue;
                }
                switch (fieldNameId.substring(0, 3)) {
                case "VAL": {
                    entryFields.add(new ResultRawField(tiesFieldDescription, bytes.array()));
                    break;
                }
                case "HSH": {
                    entryFields.add(new ResultHashField(tiesFieldDescription, bytes));
                    break;
                }
                default:
                    throw new TiesServiceScopeException("Unknown field prefix for field " + fieldNameId);
                }
            }
            Map<String, AbstractType<?>> typeMap = new HashMap<>();
            row.getColumns().stream().filter(c -> c.name.toString().startsWith("COM")).forEach(t -> typeMap.put(t.name.toString(), t.type));
            for (FieldDescription tiesFieldDescription : tiesComputes) {
                String fieldNameId = aliasMap.get(tiesFieldDescription);
                ByteBuffer bytes = row.getBlob(fieldNameId);
                if (null == bytes) {
                    continue;
                }
                String tiesType = tiesFieldDescription.getType();
                if (null == tiesType) {
                    tiesType = CassandraTypeHelper.getTiesTypeByCassandraType(typeMap.get(fieldNameId));
                    for (ColumnSpecification col : row.getColumns()) {
                        if (fieldNameId.equals(col.name.toString())) {
                            if (null == tiesType) {
                                throw new TiesServiceScopeException(
                                        "Can't detect type for computed field " + tiesFieldDescription.getName());
                            }
                            tiesFieldDescription = new FieldDescription(tiesFieldDescription.getName(), tiesType);
                            break;
                        }
                    }
                }
                AbstractType<?> type = typeMap.get(fieldNameId);
                computedFields.add(new ResultValueField(tiesFieldDescription,
                        TiesTypeHelper.formatFromCassandraType(bytes, type, tiesFieldDescription.getType())));
            }
        }

        return new Result.Entry() {

            @Override
            public TiesEntryHeader getEntryHeader() {
                return entryHeader;
            }

            @Override
            public List<Result.Field> getEntryFields() {
                return entryFields;
            }

            @Override
            public List<Result.Field> getComputedFields() {
                return computedFields;
            }

        };
    }

    private static String prettyPrint(Object o) {
        if (o instanceof ByteBuffer) {
            byte[] buf = new byte[((ByteBuffer) o).remaining()];
            ((ByteBuffer) o).slice().get(buf);
            return formatHexValue(buf);
        }
        return o.toString();
    }

    private static <T> void forArguments(Argument.Visitor<T> v, StringBuilder qb, List<Argument> args) throws TiesServiceScopeException {
        for (Argument arg : args) {
            arg.accept(v);
            qb.append(',');
        }
        qb.setLength(qb.length() - 1);
    }

    private static <T> void forFilter(Argument.Visitor<T> v, StringBuilder qb, Filter fil) throws TiesServiceScopeException {
        String fieldNameId = getNameId("FLD", fil.getFieldName());
        qb.append(fieldNameId);
        qb.append("\" ");
        String operator = fil.getName().toLowerCase();
        qb.append(operator);
        switch (operator.toLowerCase()) {
        case "in": {
            qb.append('(');
            forArguments(v, qb, fil.getArguments());
            qb.append(')');
            break;
        }
        default:
            forArguments(v, qb, fil.getArguments());
        }
    }

    private static <T> void forFunction(Argument.Visitor<T> v, StringBuilder qb, Function fun) throws TiesServiceScopeException {
        String fName = fun.getName();
        qb.append(fName);
        qb.append('(');
        switch (fName.toLowerCase()) {
        case "cast": {
            List<Argument> fArguments = fun.getArguments();
            if (null == fArguments || fArguments.size() != 2) {
                throw new TiesServiceScopeException("CAST should have exactly two arguments");
            }
            Argument[] cArguments = fArguments.toArray(new Argument[2]);
            if (!(cArguments[1] instanceof Argument.ValueArgument)) {
                throw new TiesServiceScopeException("CAST second argument shuld be a type name");
            }
            cArguments[0].accept(v);
            qb.append(" as ");
            qb.append(((Argument.ValueArgument) cArguments[1]).getValue());
            break;
        }
        default:
            forArguments(v, qb, fun.getArguments());
        }
        qb.append(')');
    }

    @Override
    public void result(TiesServiceScopeResult result) throws TiesServiceScopeException {
        throw new TiesServiceScopeException("Node should not handle any result");
    }

}
