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
import static network.tiesdb.service.impl.elassandra.scope.db.TiesSchema.ENTRY_HEADER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import com.tiesdb.lib.crypto.encoder.EncoderManager;
import com.tiesdb.lib.crypto.encoder.api.Encoder;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.impl.elassandra.scope.db.ByteArrayType;
import network.tiesdb.service.impl.elassandra.scope.db.CassandraTypeHelper;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchema;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchema.FieldDescription;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchema.HeaderField;
import network.tiesdb.service.impl.elassandra.scope.db.TiesTypeHelper;
import network.tiesdb.service.scope.api.TiesEntryHeader;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeModification;
import network.tiesdb.service.scope.api.TiesServiceScopeModification.Entry;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Filter;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Function;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Function.Argument;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Query.Selector;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Result;

public class TiesServiceScopeImpl implements TiesServiceScope {

    private static final Logger LOG = LoggerFactory.getLogger(TiesServiceScopeImpl.class);

    private static final Encoder NAME_ENCODER = EncoderManager.getEncoder(EncoderManager.BASE32_NP);

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
        public Object getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "ResultValueField [name=" + getName() + ", type=" + getType() + ", value=" + value + "]";
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
            return "ResultRawField [name=" + getName() + ", type=" + getType() + ", rawValue=" + rawValue + "]";
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
            return "ResultHashField [name=" + getName() + ", type=" + getType() + ", hash=" + DatatypeConverter.printHexBinary(getHash())
                    + "]";
        }

    }

    private final TiesService service;

    public TiesServiceScopeImpl(TiesService service) {
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

    private static String getNameId(String prefix, String name) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Digest digest = DigestManager.getDigest(DigestManager.KECCAK_224);
            digest.update(name.getBytes(Charset.forName("UTF-8")));
            byte[] nameHash = new byte[digest.getDigestSize()];
            digest.doFinal(nameHash, 0);
            NAME_ENCODER.encode(nameHash, b -> baos.write(b));
            return prefix + baos.toString();
        } catch (IOException e) {
            return null;
        }
    }

    private static String format(String type, byte[] data) {
        switch (type.toUpperCase()) {
        case "STRING":
            return "\"" + new String(data) + "\"";
        default:
            return "0x" + DatatypeConverter.printHexBinary(data).toLowerCase();
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
    }

    private static Object getHeaderField(TiesEntryHeader h, String name) throws TiesServiceScopeException {
        try {
            HeaderField headerField = HeaderField.valueOfIgnoreCase(name.toUpperCase());
            switch (headerField) {
            case TIM:
                return h.getEntryTimestamp();
            case VER:
                return h.getEntryVersion();
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

    @Override
    public void insert(TiesServiceScopeModification modificationRequest) throws TiesServiceScopeException {

        TiesServiceScopeModification.Entry entry = modificationRequest.getEntry();
        String tablespaceName = entry.getTablespaceName();
        String tableName = entry.getTableName();
        LOG.debug("Insert into `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        Map<String, Entry.FieldValue> entryFields = entry.getFieldValues();
        List<String> fieldNames = new ArrayList<>(entryFields.size());
        List<Object> fieldValues = new ArrayList<>(entryFields.size());

        addHeader(entry.getHeader(), fieldNames, fieldValues, cfMetaData);

        for (String fieldName : entryFields.keySet()) {

            String fieldNameId = getNameId("FLD", fieldName);
            String fieldHashNameId = getNameId("HSH", fieldName);
            String fieldValueNameId = getNameId("VAL", fieldName);

            Entry.FieldValue fieldValue = entryFields.get(fieldName);
            requireNonNull(fieldValue);
            LOG.debug("Field {} ({}) Value ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            Object fieldFormattedValue = TiesTypeHelper.formatToCassandraType(fieldValue.get(),
                    columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
            if (null != fieldFormattedValue) {
                LOG.debug("FormattedValue {} ({})", fieldFormattedValue, fieldFormattedValue.getClass());
            } else {
                LOG.debug("FormattedValue null");
            }

            fieldNames.add(fieldNameId);
            fieldValues.add(fieldFormattedValue);
            // fieldValues.add(columnType.compose(ByteBuffer.wrap(fieldValue.getBytes())));
            fieldNames.add(fieldHashNameId);
            fieldValues.add(ByteBuffer.wrap(fieldValue.getHash()));
            fieldNames.add(fieldValueNameId);
            fieldValues.add(ByteBuffer.wrap(fieldValue.getBytes()));
        }

        String query = String.format("INSERT INTO \"%s\".\"%s\"\n" + //
                "(%s)\n" + //
                "VALUES (%s)\n" + //
                "IF NOT EXISTS", //
                tablespaceNameId, tableNameId, concat(fieldNames, ", "), createValuePlaceholders(fieldNames.size()));
        LOG.debug("Insert query {}", query);

        UntypedResultSet result = QueryProcessor.execute(query, ConsistencyLevel.ALL, fieldValues.toArray());
        LOG.debug("Insert result {}", result);
        if (LOG.isTraceEnabled()) {
            for (UntypedResultSet.Row row : result) {
                LOG.trace("Insert result row {}", row);
                for (ColumnSpecification col : row.getColumns()) {
                    LOG.trace("Insert result row col {} = {}", col.name, col.type.compose(row.getBlob(col.name.toString())));
                }
            }
        }
        if (result.isEmpty()) {
            throw new TiesServiceScopeException("No insertion result found");
        } else if (result.size() > 1) {
            throw new TiesServiceScopeException("Multiple insertion results found");
        } else if (!result.one().getBoolean("[applied]")) {
            {
                List<ColumnDefinition> partKeyColumns = cfMetaData.partitionKeyColumns();
                ArrayList<String> partKeyColumnsNames = new ArrayList<>(partKeyColumns.size());
                for (ColumnDefinition columnDefinition : partKeyColumns) {
                    partKeyColumnsNames.add(columnDefinition.name.toString().toUpperCase());
                }
                partKeyColumnsNames.removeAll(fieldNames);
                if (!partKeyColumnsNames.isEmpty()) {
                    LOG.debug("Missing values for {}", partKeyColumnsNames);
                    Map<String, String> emptyNames = new HashMap<>();
                    TiesSchema.loadFieldDescriptions(tablespaceName, tableName, fd -> {
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
    }

    @Override
    public void update(TiesServiceScopeModification modificationRequest) throws TiesServiceScopeException {

        TiesServiceScopeModification.Entry entry = modificationRequest.getEntry();

        String tablespaceName = entry.getTablespaceName();
        String tableName = entry.getTableName();
        LOG.debug("Insert into `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        Map<String, Entry.FieldValue> entryFields = entry.getFieldValues();
        ArrayList<String> partKeyColumnsNames;
        {
            List<ColumnDefinition> partKeyColumns = cfMetaData.partitionKeyColumns();
            partKeyColumnsNames = new ArrayList<>(partKeyColumns.size());
            for (ColumnDefinition columnDefinition : partKeyColumns) {
                partKeyColumnsNames.add(columnDefinition.name.toString().toUpperCase());
            }
        }

        Map<String, String> emptyNames = new HashMap<>();
        TiesSchema.loadFieldDescriptions(tablespaceName, tableName, fd -> {
            emptyNames.put(getNameId("FLD", fd.getName()), fd.getName());
        });
        if (emptyNames.size() * 2 != cfMetaData.allColumns().size() - 1) {
            LOG.warn("Fields count missmatch. TiesDB schema need to be updated.");
            // TODO Do TiesSchema update table metadata
        }

        List<String> keyNames = new ArrayList<>(partKeyColumnsNames.size());
        List<Object> keyValues = new ArrayList<>(keyNames.size());
        List<String> fieldNames = new ArrayList<>(entryFields.size());
        List<Object> fieldValues = new ArrayList<>(fieldNames.size());

        addHeader(entry.getHeader(), fieldNames, fieldValues, cfMetaData);

        for (String fieldName : entryFields.keySet()) {

            String fieldNameId = getNameId("FLD", fieldName);
            String fieldHashNameId = getNameId("HSH", fieldName);
            String fieldValueNameId = getNameId("VAL", fieldName);

            emptyNames.remove(fieldNameId);

            Entry.FieldValue fieldValue = entryFields.get(fieldName);
            requireNonNull(fieldValue);
            LOG.debug("Field {} ({}) RawValue ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

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

        if (!partKeyColumnsNames.isEmpty()) {
            LOG.debug("Missing values for {}", partKeyColumnsNames);
            List<String> missingKeys = new LinkedList<>();
            for (String fieldNameId : partKeyColumnsNames) {
                missingKeys.add(emptyNames.get(fieldNameId));
            }
            throw new TiesServiceScopeException("Missing key fields for `" + tablespaceName + "`.`" + tableName + "`: " + missingKeys);
        }

        String query = String.format(//
                "UPDATE \"%s\".\"%s\"\n" + //
                        "SET %s = ?\n" + //
                        "WHERE %s = ?\n" + //
                        "IF \"" + ENTRY_HEADER + "\"." + HeaderField.HSH.name().toLowerCase() + " = ? " + //
                        "AND \"" + ENTRY_HEADER + "\"." + HeaderField.VER.name().toLowerCase() + " = ? " + //
                        "%s", //
                tablespaceNameId, tableNameId, //
                concat(fieldNames, " = ?, "), //
                concat(keyNames, " = ? AND "), //
                (emptyNames.isEmpty() ? "" : String.format("AND %s = NULL", concat(emptyNames.keySet(), " = NULL AND "))));
        LOG.debug("Update query {}", query);

        fieldValues.addAll(keyValues);
        fieldValues.add(TiesTypeHelper.formatToCassandraType(entry.getHeader().getEntryOldHash(), BytesType.instance));
        fieldValues.add(entry.getHeader().getEntryVersion().subtract(BigInteger.ONE));

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
            throw new TiesServiceScopeException("Update failed");
        }
    }

    @Override
    public void select(TiesServiceScopeRecollection scope) throws TiesServiceScopeException {

        Query request = scope.getQuery();

        String tablespaceName = request.getTablespaceName();
        String tableName = request.getTableName();
        LOG.debug("Select from `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        List<Object> qv = new LinkedList<>();
        StringBuilder qb = new StringBuilder();

        Argument.Visitor argVisitor = new Argument.Visitor() {

            @Override
            public void on(Argument.FunctionArgument a) throws TiesServiceScopeException {
                forFunction(this, qb, a);
            }

            @Override
            public void on(Argument.ValueArgument a) throws TiesServiceScopeException {
                qv.add(a.getValue());
                qb.append('?');
            }

            @Override
            public void on(Argument.FieldArgument a) {
                qb.append('"');
                qb.append(getNameId("FLD", a.getFieldName()));
                qb.append('"');
            }

        };

        List<FieldDescription> tiesFields = new LinkedList<>();
        TiesSchema.loadFieldDescriptions(tablespaceName, tableName, tiesFields::add);
        if (tiesFields.size() * 2 != cfMetaData.allColumns().size() - 1) {
            LOG.warn("Fields count missmatch. TiesDB schema need to be updated.");
            // TODO Do TiesSchema update table metadata
        }
        Map<FieldDescription, String> fieldMap = new HashMap<>();

        AtomicInteger tiesComputesCounter = new AtomicInteger(0);
        List<FieldDescription> tiesComputes = new LinkedList<>();
        Map<FieldDescription, String> aliasMap = new HashMap<>();

        List<Selector> selectors = request.getSelectors();
        qb.append("select ");
        qb.append('"');
        qb.append(ENTRY_HEADER);
        qb.append('"');
        qb.append(',');
        if (!selectors.isEmpty()) {
            Map<String, String> selectedFields = new HashMap<>();

            for (Selector sel : selectors) {
                sel.accept(new Selector.Visitor() {

                    @Override
                    public void on(Selector.FunctionSelector s) throws TiesServiceScopeException {
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
                    }

                    @Override
                    public void on(Selector.FieldSelector s) {
                        String fieldName = s.getFieldName();
                        String fieldNameId = getNameId("VAL", fieldName);
                        qb.append('"');
                        qb.append(fieldNameId);
                        qb.append('"');
                        selectedFields.put(fieldName, fieldNameId);
                    }

                });
                qb.append(',');
            }
            for (FieldDescription field : tiesFields) {
                String fieldNameId = selectedFields.remove(field.getName());
                if (null == fieldNameId) {
                    fieldNameId = getNameId("HSH", field.getName());
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

        List<Filter> filters = request.getFilters();
        if (!filters.isEmpty()) {
            qb.append(" where \"");
            for (Filter filter : filters) {
                forFilter(argVisitor, qb, filter);
                qb.append(" and ");
            }
            qb.setLength(qb.length() - 5);
        }

        System.out.println(qb.toString());

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
        for (UntypedResultSet.Row row : result) {
            scope.addResult(newResult(row, newEntryHeader(row, cfMetaData), tiesFields, tiesComputes, fieldMap, aliasMap));
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
        BigInteger ver = newEntryHeaderField(type, components, HeaderField.VER, IntegerType.instance);
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

    private static Result newResult(Row row, TiesEntryHeader entryHeader, List<FieldDescription> tiesFields,
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

        return new Result() {

            @Override
            public TiesEntryHeader getEntryHeader() {
                return entryHeader;
            }

            @Override
            public List<Field> getEntryFields() {
                return entryFields;
            }

            @Override
            public List<Field> getComputedFields() {
                return computedFields;
            }

        };
    }

    private static String prettyPrint(Object o) {
        if (o instanceof ByteBuffer) {
            byte[] buf = new byte[((ByteBuffer) o).remaining()];
            ((ByteBuffer) o).slice().get(buf);
            return DatatypeConverter.printHexBinary(buf);
        }
        return o.toString();
    }

    private static void forArguments(Argument.Visitor v, StringBuilder qb, List<Argument> arguments) throws TiesServiceScopeException {
        for (Argument arg : arguments) {
            arg.accept(v);
            qb.append(',');
        }
        qb.setLength(qb.length() - 1);
    }

    private static void forFilter(Argument.Visitor v, StringBuilder qb, Filter fil) throws TiesServiceScopeException {
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

    private static void forFunction(Argument.Visitor v, StringBuilder qb, Function fun) throws TiesServiceScopeException {
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

}
