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

import java.math.BigDecimal;

import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.AbstractTextSerializer;
import org.apache.cassandra.serializers.AsciiSerializer;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.ByteSerializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.DoubleSerializer;
import org.apache.cassandra.serializers.DurationSerializer;
import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.serializers.ShortSerializer;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.serializers.TimeSerializer;
import org.apache.cassandra.serializers.TimestampSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;

public final class CassandraTypeHelper {

    public static final BigDecimal DAY_DURATION_VALUE = BigDecimal.valueOf(60 * 60 * 24, 0);
    public static final BigDecimal MONTH_DURATION_VALUE = DAY_DURATION_VALUE.multiply(BigDecimal.valueOf(31)); // Standard month is 31 day

    private CassandraTypeHelper() {
    }

    public static String getTiesTypeByCassandraType(AbstractType<?> type) {
        return getTiesTypeByCassandraTypeSerializer(type.getSerializer());
    }

    private static String getTiesTypeByCassandraTypeSerializer(TypeSerializer<?> serializer) {
        if (serializer instanceof AsciiSerializer) {
            return "ascii";
        } else if (serializer instanceof AbstractTextSerializer) {
            return "string";
        } else if (serializer instanceof BooleanSerializer) {
            return "boolean";
        } else if (serializer instanceof DecimalSerializer) {
            return "decimal";
        } else if (serializer instanceof DoubleSerializer) {
            return "double";
        } else if (serializer instanceof FloatSerializer) {
            return "float";
        } else if (serializer instanceof IntegerSerializer) {
            return "bigint";
        } else if (serializer instanceof LongSerializer) {
            return "long";
        } else if (serializer instanceof Int32Serializer //
                || serializer instanceof ShortSerializer //
                || serializer instanceof ByteSerializer) {
            return "integer";
        } else if (serializer instanceof TimestampSerializer//
                || serializer instanceof SimpleDateSerializer) {
            return "time";
        } else if (serializer instanceof DurationSerializer) {
            return "duration";
        } else if (serializer instanceof UUIDSerializer) {
            return "uuid";
        }

        // Unsupported types as strings
        else if (serializer instanceof TimeSerializer //
                || serializer instanceof InetAddressSerializer) {
            return "string";
        }

        // All other types
        else {
            // else if (serializer instanceof BytesSerializer) {}
            // else if (serializer instanceof CollectionSerializer) {}
            // else if (serializer instanceof EmptySerializer) {}
            // else if (serializer instanceof InetAddressSerializer) {}
            return "binary";
        }
    }

    public static BigDecimal durationAsBigDecimal(Duration cqlDuration) {
        BigDecimal durationValue = BigDecimal.valueOf(cqlDuration.getNanoseconds(), 9).setScale(6, BigDecimal.ROUND_UNNECESSARY);
        durationValue.add(MONTH_DURATION_VALUE.multiply(BigDecimal.valueOf(cqlDuration.getMonths())));
        durationValue.add(DAY_DURATION_VALUE.multiply(BigDecimal.valueOf(cqlDuration.getDays())));
        return durationValue;
    }

}
