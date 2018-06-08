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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.TypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.lib.crypto.digest.DigestManager;
import com.tiesdb.lib.crypto.digest.api.Digest;
import com.tiesdb.lib.crypto.encoder.EncoderManager;
import com.tiesdb.lib.crypto.encoder.api.Encoder;

import network.tiesdb.api.TiesVersion;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeAction;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesValue;
import network.tiesdb.type.Duration;
import network.tiesdb.type.Duration.DurationUnit;

import static network.tiesdb.type.Duration.DurationTimeUnit.*;

public class TiesServiceScopeImpl implements TiesServiceScope {

    private static final Logger logger = LoggerFactory.getLogger(TiesServiceScopeImpl.class);

    private static final Encoder nameEncoder = EncoderManager.getEncoder(EncoderManager.BASE32_NP);

    private static final BigDecimal MONTH_DURATION_VALUE = BigDecimal.valueOf(60 * 60 * 24 * 31, 0); // Standard month is 31 day
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

    private final TiesService service;

    public TiesServiceScopeImpl(TiesService service) {
        this.service = service;
        logger.debug(this + " is opened");
    }

    @Override
    public void close() throws IOException {
        // TODO free used resources
        logger.debug(this + " is closed");
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
            nameEncoder.encode(nameHash, b -> baos.write(b));
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

    @Override
    public void insert(TiesServiceScopeAction action) throws TiesServiceScopeException {

        String tablespaceName = action.getTablespaceName();
        String tableName = action.getTableName();
        logger.debug("Insert into `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        logger.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        Map<String, TiesValue> actionFields = action.getFieldValues();
        List<String> fieldNames = new ArrayList<>(actionFields.size());
        List<Object> fieldValues = new ArrayList<>(actionFields.size());

        {
            fieldNames.add("header");
            fieldValues.add(ByteBuffer.wrap(action.getHeaderRawBytes()));
            fieldNames.add("version");
            fieldValues.add(action.getEntryVersion());
        }

        for (String fieldName : actionFields.keySet()) {

            String fieldNameId = getNameId("FLD", fieldName);

            TiesValue fieldValue = actionFields.get(fieldName);
            logger.debug("Field {} ({}) Value ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            Object fieldFormattedValue = formatToCassandraType(fieldValue, columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
            if (null != fieldFormattedValue) {
                logger.debug("FormattedValue {} ({})", fieldFormattedValue, fieldFormattedValue.getClass());
            } else {
                logger.debug("FormattedValue null");
            }

            fieldNames.add(fieldNameId);
            fieldNames.add(fieldNameId + "_V");
            fieldValues.add(fieldFormattedValue);
            // fieldValues.add(columnType.compose(ByteBuffer.wrap(fieldValue.getBytes())));
            fieldValues.add(ByteBuffer.wrap(fieldValue.getFieldFullRawBytes()));
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

        UntypedResultSet result = QueryProcessor.execute(query, ConsistencyLevel.ALL, fieldValues.toArray());
        logger.debug("Insert result {}", result);
        if (logger.isTraceEnabled()) {
            for (UntypedResultSet.Row row : result) {
                logger.trace("Insert result row {}", row);
                for (ColumnSpecification col : row.getColumns()) {
                    logger.trace("Insert result row col {} = {}", col.name, col.type.compose(row.getBlob(col.name.toString())));
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
    public void update(TiesServiceScopeAction action) throws TiesServiceScopeException {

        String tablespaceName = action.getTablespaceName();
        String tableName = action.getTableName();
        logger.debug("Insert into `{}`.`{}`", tablespaceName, tableName);

        String tablespaceNameId = getNameId("TIE", tablespaceName);
        String tableNameId = getNameId("TBL", tableName);
        logger.debug("Mapping table `{}`.`{}` to {}.{}", tablespaceName, tableName, tablespaceNameId, tableNameId);

        CFMetaData cfMetaData = Schema.instance.getCFMetaData(tablespaceNameId, tableNameId);

        if (null == cfMetaData) {
            throw new TiesServiceScopeException("Table `" + tablespaceName + "`.`" + tableName + "` was not found");
        }

        Map<String, TiesValue> actionFields = action.getFieldValues();
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

        {
            fieldNames.add("header");
            fieldValues.add(ByteBuffer.wrap(action.getHeaderRawBytes()));
            fieldNames.add("version");
            fieldValues.add(action.getEntryVersion());
        }

        for (String fieldName : actionFields.keySet()) {

            String fieldNameId = getNameId("FLD", fieldName);

            TiesValue fieldValue = actionFields.get(fieldName);
            logger.debug("Field {} ({}) RawValue ({}) {}", fieldName, fieldNameId, fieldValue.getType(),
                    format(fieldValue.getType(), fieldValue.getBytes()));

            ColumnDefinition columnDefinition = cfMetaData.getColumnDefinition(ColumnIdentifier.getInterned(fieldNameId, true));
            if (null == columnDefinition) {
                throw new TiesServiceScopeException("Field `" + tablespaceName + "`.`" + tableName + "`.`" + fieldName + "` was not found");
            }

            Object fieldFormattedValue = formatToCassandraType(fieldValue, columnDefinition.getExactTypeIfKnown(cfMetaData.ksName));
            if (null != fieldFormattedValue) {
                logger.debug("FormattedValue {} ({})", fieldFormattedValue, fieldFormattedValue.getClass());
            } else {
                logger.debug("FormattedValue null");
            }

            if (partKeyColumnsNames.remove(fieldNameId)) {
                logger.debug("KeyField {}", fieldName);
                keyNames.add(fieldNameId);
                keyValues.add(fieldFormattedValue);
                // keyValues.add(columnType.compose(ByteBuffer.wrap(fieldValue.getBytes())));
            } else {
                logger.debug("DataField {}", fieldName);
                fieldNames.add(fieldNameId);
                fieldNames.add(fieldNameId + "_V");
                fieldValues.add(fieldFormattedValue);
                // fieldValues.add(columnType.compose(ByteBuffer.wrap(fieldValue.getBytes())));
                fieldValues.add(ByteBuffer.wrap(fieldValue.getFieldFullRawBytes()));
            }
        }

        if (!partKeyColumnsNames.isEmpty()) {
            logger.debug("Missing values for {}", partKeyColumnsNames);
            throw new TiesServiceScopeException("Missing key fields values for `" + tablespaceName + "`.`" + tableName + "`");
        }

        String query = String.format("UPDATE \"%s\".\"%s\"\n" + //
                "SET %s = ?\n" + //
                "WHERE %s = ?\n" + //
                "IF VERSION = ?", //
                tablespaceNameId, tableNameId, concat(fieldNames, " = ?, "), concat(keyNames, " = ? AND "));

        fieldValues.addAll(keyValues);
        fieldValues.add(action.getEntryVersion() - 1);

        UntypedResultSet result = QueryProcessor.execute(query, ConsistencyLevel.ALL, fieldValues.toArray());
        logger.debug("Update result {}", result);
        if (logger.isDebugEnabled()) {
            for (UntypedResultSet.Row row : result) {
                logger.debug("Update result row {}", row);
                for (ColumnSpecification col : row.getColumns()) {
                    logger.debug("Update result row col {} = {}", col.name, col.type.compose(row.getBlob(col.name.toString())));
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

    private Object formatToCassandraType(TiesValue fieldValue, AbstractType<?> columnType) throws TiesServiceScopeException {
        fieldValue = requireNonNull(fieldValue);
        logger.debug("Type mapping {} to {}", fieldValue.getType(), columnType);
        columnType = requireNonNull(columnType);

        Object value = fieldValue.get();

        if (null == value) {
            logger.debug("TiesValue null");
            return null;
        }

        logger.debug("TiesValue {} ({})", value, value.getClass());

        TypeSerializer<?> serializer = requireNonNull(columnType.getSerializer());

        if (value.getClass().equals(serializer.getType())) {
            return value;
        }

        if (columnType.getSerializer().getType().equals(org.apache.cassandra.cql3.Duration.class)) {
            if (value instanceof Duration) {
                Duration duration = ((Duration) value);
                return org.apache.cassandra.cql3.Duration.newInstance(//
                        duration.getInteger(MONTH).intValueExact(), //
                        duration.getPartInteger(DAY, MONTH).intValueExact(), //
                        duration.getPartInteger(NANOSECOND, DAY).longValueExact() //
                );
            }
        }

        if (columnType.getSerializer().getType().equals(java.nio.ByteBuffer.class)) {
            if (value instanceof byte[]) {
                return java.nio.ByteBuffer.wrap((byte[]) value);
            }
        }
        throw new TiesServiceScopeException("Mapping " + value.getClass() + " to " + columnType.getSerializer().getType() + " failed.");
    }

    // TODO Make useful utility class and remove this shit
    public static void main(String[] names) {
        for (String name : names) {
            System.out.println(getNameId("", name) + " " + name);
        }
    }
}
