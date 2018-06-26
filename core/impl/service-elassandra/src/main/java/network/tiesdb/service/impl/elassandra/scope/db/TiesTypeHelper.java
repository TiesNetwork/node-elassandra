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

import static java.util.Objects.requireNonNull;
import static network.tiesdb.type.Duration.DurationTimeUnit.DAY;
import static network.tiesdb.type.Duration.DurationTimeUnit.NANOSECOND;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.serializers.AbstractTextSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.type.Duration;
import network.tiesdb.type.Duration.DurationUnit;

public final class TiesTypeHelper {

    private static final Logger LOG = LoggerFactory.getLogger(TiesTypeHelper.class);

    private static final DurationUnit MONTH = new DurationUnit() {

        @Override
        public BigDecimal getValue() {
            return CassandraTypeHelper.MONTH_DURATION_VALUE;
        }

        @Override
        public String getName() {
            return "MONTH";
        }

    };

    private TiesTypeHelper() {
    }

    public static Object formatToCassandraType(Object value, AbstractType<?> cType) throws TiesServiceScopeException {

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
                        duration.getDecimal(MONTH).intValue(), //
                        duration.getPartInteger(DAY, MONTH).intValue(), //
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

    public static Object formatFromCassandraType(ByteBuffer data, AbstractType<?> type, String tiesType) throws TiesServiceScopeException {

        requireNonNull(tiesType);
        requireNonNull(type);
        requireNonNull(data);

        switch (tiesType) {
        case "int":
        case "integer":
            return cassandraTypeAs(data, type, Number.class).intValue();
        case "long":
            return cassandraTypeAs(data, type, Number.class).longValue();
        case "float":
            return cassandraTypeAs(data, type, Number.class).floatValue();
        case "double":
            return cassandraTypeAs(data, type, Number.class).doubleValue();
        case "decimal":
            return cassandraTypeAs(data, type, BigDecimal.class);
        case "bigint":
            return cassandraTypeAs(data, type, IntegerType.class);
        case "ascii":
            return cassandraTypeAs(data, type, String.class);
        case "string":
            if (type.getSerializer() instanceof AbstractTextSerializer) {
                return cassandraTypeAs(data, type, String.class);
            } else {
                @SuppressWarnings("unchecked")
                TypeSerializer<Object> ser = (TypeSerializer<Object>) type.getSerializer();
                return ser.toString(ser.deserialize(data));
            }
        case "time":
            if (type instanceof TimestampType) {
                return cassandraTypeAs(data, type, Date.class);
            } else if (type instanceof SimpleDateType) {
                Integer days = cassandraTypeAs(data, type, Integer.class);
                return new Date(TimeUnit.DAYS.toMillis(days - Integer.MIN_VALUE));
            } else {
                long timestamp = cassandraTypeAs(data, type, Number.class).longValue();
                return new Date(timestamp);
            }
        case "binary":
            return ByteArrayType.instance.compose(data);
        case "uuid":
            return cassandraTypeAs(data, type, UUID.class);
        case "boolean":
            return cassandraTypeAs(data, type, Boolean.class);
        case "duration":
            if (type instanceof DurationType) {
                return new Duration(CassandraTypeHelper.durationAsBigDecimal(DurationType.instance.compose(data)));
            } else if (type instanceof DecimalType) {
                return new Duration(DecimalType.instance.compose(data));
            }
            break;
        default:
            throw new TiesServiceScopeException("Unknown type " + tiesType);
        }

        throw new TiesServiceScopeException("Cassandra object of type " + type + " can't be converted to type: " + tiesType);
    }

    private static <T> T cassandraTypeAs(ByteBuffer data, AbstractType<?> type, Class<T> javaType) throws TiesServiceScopeException {
        if (!javaType.isAssignableFrom(type.getSerializer().getType())) {
            throw new TiesServiceScopeException("Cassandra object of type " + type + " can't be used as " + javaType);
        }
        return javaType.cast(type.compose(data));
    }

}
