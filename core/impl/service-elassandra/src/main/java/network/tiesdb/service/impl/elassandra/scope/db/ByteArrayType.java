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

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

public class ByteArrayType extends AbstractType<byte[]> {

    public static final ByteArrayType instance = new ByteArrayType();

    ByteArrayType() {
        super(ComparisonType.BYTE_ORDER);
    } // singleton

    public ByteBuffer fromString(String source) {
        try {
            return ByteBuffer.wrap(Hex.hexToBytes(source));
        } catch (NumberFormatException e) {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException {
        try {
            String parsedString = (String) parsed;
            if (!parsedString.startsWith("0x"))
                throw new MarshalException(String.format("String representation of blob is missing 0x prefix: %s", parsedString));

            return new Constants.Value(ByteArrayType.instance.fromString(parsedString.substring(2)));
        } catch (ClassCastException | MarshalException exc) {
            throw new MarshalException(String.format("Value '%s' is not a valid blob representation: %s", parsed, exc.getMessage()));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion) {
        return "\"0x" + ByteBufferUtil.bytesToHex(buffer) + '"';
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous) {
        // Both asciiType and utf8Type really use bytes comparison and
        // bytesType validate everything, so it is compatible with the former.
        return this == previous || previous == AsciiType.instance || previous == UTF8Type.instance;
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
        // BytesType can read anything
        return true;
    }

    public CQL3Type asCQL3Type() {
        return CQL3Type.Native.BLOB;
    }

    public TypeSerializer<byte[]> getSerializer() {
        return ByteArraySerializer.instance;
    }
}
