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
import static network.tiesdb.type.Duration.DurationTimeUnit.DAY;
import static network.tiesdb.type.Duration.DurationTimeUnit.NANOSECOND;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.lib.crypto.digest.DigestManager;
import com.tiesdb.lib.crypto.digest.api.Digest;
import com.tiesdb.lib.crypto.encoder.EncoderManager;
import com.tiesdb.lib.crypto.encoder.api.Encoder;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.impl.elassandra.scope.db.ByteArrayType;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchema;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchema.FieldDescription;
import network.tiesdb.service.impl.elassandra.scope.db.TiesSchema.HeaderField;
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
import network.tiesdb.type.Duration;
import network.tiesdb.type.Duration.DurationUnit;

public class TiesServiceScopeImpl implements TiesServiceScope {

    private static final Logger LOG = LoggerFactory.getLogger(TiesServiceScopeImpl.class);

    private static final Encoder NAME_ENCODER = EncoderManager.getEncoder(EncoderManager.BASE32_NP);

    private static final BigDecimal MONTH_DURATION_VALUE = BigDecimal.valueOf(60 * 60 * 24 * 31, 0); // Standard month
                                                                                                     // is 31 day
    private static final DurationUnit MONTH = new DurationUnit() {

        @Override
        public BigDecimal getValue() {
            return MONTH_DURATION_VALUE;
        }

        @Override
        public String getName() {
            return "MONTH";
        }

    };

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

    private static String concat(List<String> fieldNames, String delim) {
        StringBuilder sb = new StringBuilder();
        for (String name : fieldNames) {
            sb.append(delim);
            sb.append('"');
            sb.append(name);
            sb.append('"');
        }
        return sb.substring(delim.length());
    }

    private static Object formatToCassandraType(Object value, AbstractType<?> cType) throws TiesServiceScopeException {

        if (null == value) {
            LOG.debug("Entry.FieldValue null", value);
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        LOG.debug("Entry.FieldValue {} ({})", value, value.getClass());

        Class<?> type = requireNonNull(requireNonNull(cType).getSerializer()).getType();
        if (type.isAssignableFrom(value.getClass())) {
            return value;
        }

        LOG.debug("Type mapping {} to {}", value.getClass(), type);

        if (type.equals(org.apache.cassandra.cql3.Duration.class)) {
            if (value instanceof Duration) {
                Duration duration = ((Duration) value);
                return org.apache.cassandra.cql3.Duration.newInstance(//
                        duration.getInteger(MONTH).intValueExact(), //
                        duration.getPartInteger(DAY, MONTH).intValueExact(), //
                        duration.getPartInteger(NANOSECOND, DAY).longValueExact() //
                );
            }
        } else if (type.equals(java.nio.ByteBuffer.class)) {
            if (value instanceof byte[]) {
                return java.nio.ByteBuffer.wrap((byte[]) value);
            }
        }

        throw new TiesServiceScopeException("Type mapping " + value.getClass() + " to " + type + " failed.");
    }

    private static Object formatFromCassandraType(Row row, String column, String type) throws TiesServiceScopeException {
        switch (type) {
        case "int":
        case "integer":
            return row.getInt(column);
        case "long":
            return row.getLong(column);
        case "float":
            return row.getFloat(column);
        case "double":
            return row.getDouble(column);
        case "decimal":
            return DecimalType.instance.compose(row.getBlob(column));
        case "bigint":
            return IntegerType.instance.compose(row.getBlob(column));
        case "string":
        case "ascii":
            return row.getString(column);
        case "binary":
            return row.getBytes(column).array();
        case "time":
            return row.getTimestamp(column);
        case "uuid":
            return row.getUUID(column);
        case "boolean":
            return row.getBoolean(column);
        case "duration":
            return new Duration(DecimalType.instance.compose(row.getBlob(column)));
        default:
            throw new TiesServiceScopeException("Unknown type " + type);
        }
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
            Object fieldFormattedValue = formatToCassandraType(getHeaderField(tiesEntryHeader, type.fieldNameAsString(i)), fieldType);
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

        TiesServiceScopeModification.Entry action = modificationRequest.getEntry();
        String tablespaceName = action.getTablespaceName();
        String tableName = action.getTableName();
        LOG.debug("Insert into `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        Map<String, Entry.FieldValue> actionFields = action.getFieldValues();
        List<String> fieldNames = new ArrayList<>(actionFields.size());
        List<Object> fieldValues = new ArrayList<>(actionFields.size());

        addHeader(action.getHeader(), fieldNames, fieldValues, cfMetaData);

        for (String fieldName : actionFields.keySet()) {

            String fieldNameId = getNameId("FLD", fieldName);
            String fieldHashNameId = getNameId("HSH", fieldName);

            Entry.FieldValue fieldValue = actionFields.get(fieldName);
            requireNonNull(fieldValue);
            LOG.debug("Field {} ({}) Value ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            Object fieldFormattedValue = formatToCassandraType(fieldValue.get(), columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
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
        }
        /*
         * String query1 =
         * String.format("INSERT INTO \"%s\".\"%s\" (%s) VALUES (%s) IF NOT EXISTS",
         * tablespaceNameId, tableNameId, concat(fieldNames, ','),
         * createValuePlaceholders(fieldNames.size()));
         */
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
            throw new TiesServiceScopeException("Insertion failed");
        }
    }

    @Override
    public void update(TiesServiceScopeModification modificationRequest) throws TiesServiceScopeException {

        TiesServiceScopeModification.Entry action = modificationRequest.getEntry();

        String tablespaceName = action.getTablespaceName();
        String tableName = action.getTableName();
        LOG.debug("Insert into `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        LOG.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        Map<String, Entry.FieldValue> actionFields = action.getFieldValues();
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
        List<String> fieldNames = new ArrayList<>(actionFields.size());
        List<Object> fieldValues = new ArrayList<>(fieldNames.size());

        addHeader(action.getHeader(), fieldNames, fieldValues, cfMetaData);

        for (String fieldName : actionFields.keySet()) {

            String fieldNameId = getNameId("FLD", fieldName);
            String fieldHashNameId = getNameId("HSH", fieldName);

            Entry.FieldValue fieldValue = actionFields.get(fieldName);
            requireNonNull(fieldValue);
            LOG.debug("Field {} ({}) RawValue ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            Object fieldFormattedValue = formatToCassandraType(fieldValue.get(), columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
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
            }
        }

        if (!partKeyColumnsNames.isEmpty()) {
            LOG.debug("Missing values for {}", partKeyColumnsNames);
            throw new TiesServiceScopeException("Missing key fields values for `" + tablespaceName + "`.`" + tableName + "`");
        }

        String query = String.format(//
                "UPDATE \"%s\".\"%s\"\n" + //
                        "SET %s = ?\n" + //
                        "WHERE %s = ?\n" + //
                        "IF \"" + ENTRY_HEADER + "\"." + HeaderField.HSH.name().toLowerCase() + " = ? " + //
                        "AND \"" + ENTRY_HEADER + "\"." + HeaderField.VER.name().toLowerCase() + " = ?", //
                tablespaceNameId, tableNameId, concat(fieldNames, " = ?, "), concat(keyNames, " = ? AND "));
        LOG.debug("Update query {}", query);

        fieldValues.addAll(keyValues);
        fieldValues.add(formatToCassandraType(action.getHeader().getEntryOldHash(), BytesType.instance));
        fieldValues.add(action.getHeader().getEntryVersion().subtract(BigInteger.ONE));

        UntypedResultSet result = QueryProcessor.execute(query, ConsistencyLevel.ALL, fieldValues.toArray());
        LOG.debug("Update result {}", result);
        if (LOG.isDebugEnabled()) {
            for (UntypedResultSet.Row row : result) {
                LOG.debug("Update result row {}", row);
                for (ColumnSpecification col : row.getColumns()) {
                    LOG.debug("Update result row col {} = {}", col.name, col.type.compose(row.getBlob(col.name.toString())));
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

        Argument.Visitor argVisitor = newArgumentVisitor(qv, qb);

        List<FieldDescription> tiesComputes = new LinkedList<>();
        Map<String, String> aliasMap = new TreeMap<>();

        List<FieldDescription> tiesFields = TiesSchema.getFieldDescriptions(request.getTablespaceName(), request.getTableName());
        if (tiesFields.size() != cfMetaData.allColumns().size() - 1) {
            LOG.warn("Fields count missmatch. TiesDB schema need to be updated.");
            // TODO Do TiesSchema update table metadata
        }
        Map<String, String> fieldMap = new HashMap<>();

        qb.append("select ");
        qb.append('"');
        qb.append(ENTRY_HEADER);
        qb.append('"');
        qb.append(',');
        for (Selector sel : request.getSelectors()) {
            sel.accept(new Selector.Visitor() {

                @Override
                public void on(Selector.FunctionSelector s) throws TiesServiceScopeException {
                    String aliasName = s.getAlias();
                    aliasName = null != aliasName ? aliasName : s.getName();
                    String aliasNameId = getNameId("COM", aliasName);
                    aliasMap.put(aliasName, aliasNameId);
                    tiesComputes.add(new FieldDescription(aliasName, s.getType()));
                    forFunction(argVisitor, qb, s);
                    qb.append(" as \"");
                    qb.append(aliasNameId);
                    qb.append('"');
                }

                @Override
                public void on(Selector.FieldSelector s) {
                    String fieldName = s.getFieldName();
                    String fieldNameId = getNameId("FLD", fieldName);
                    qb.append('"');
                    qb.append(fieldNameId);
                    qb.append('"');
                    fieldMap.put(fieldName, fieldNameId);
                }

            });
            qb.append(',');
        }
        for (FieldDescription field : tiesFields) {
            fieldMap.computeIfAbsent(field.getName(), fieldName -> {
                String hashFieldNameId = getNameId("HSH", fieldName);
                qb.append('"');
                qb.append(hashFieldNameId);
                qb.append('"');
                qb.append(',');
                return hashFieldNameId;
            });
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
                    LOG.debug("Select result {} = {}", col.name.toString(),
                            prettyPrint(col.type.compose(row.getBlob(col.name.toString()))));
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
            List<FieldDescription> tiesComputes, Map<String, String> fieldMap, Map<String, String> aliasMap)
            throws TiesServiceScopeException {

        List<Result.Field> entryFields = new LinkedList<>();
        List<Result.Field> computedFields = new LinkedList<>();

        for (FieldDescription fieldDescription : tiesFields) {
            String fieldNameId = fieldMap.get(fieldDescription.getName());
            switch (fieldNameId.substring(0, 3)) {
            case "FLD": {
                entryFields.add(//
                        new ResultValueField(fieldDescription, formatFromCassandraType(row, fieldNameId, fieldDescription.getType())));
                break;
            }
            case "HSH": {
                entryFields.add(new ResultHashField(fieldDescription, row.getBytes(fieldNameId)));
                break;
            }
            default:
                throw new TiesServiceScopeException("Unknown field prefix for field " + fieldNameId);
            }
        }

        for (FieldDescription fieldDescription : tiesComputes) {
            String fieldNameId = aliasMap.get(fieldDescription.getName());
            computedFields.add(//
                    new ResultValueField(fieldDescription, formatFromCassandraType(row, fieldNameId, fieldDescription.getType())));
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

    private static Argument.Visitor newArgumentVisitor(List<Object> qv, StringBuilder qb) {
        return new Argument.Visitor() {

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
    }

}
