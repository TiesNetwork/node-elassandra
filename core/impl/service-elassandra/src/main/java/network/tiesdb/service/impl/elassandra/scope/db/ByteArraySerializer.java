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

import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

public class ByteArraySerializer implements TypeSerializer<byte[]> {
    public static final ByteArraySerializer instance = new ByteArraySerializer();

    public void validate(ByteBuffer bytes) throws MarshalException {
        // all bytes are legal.
    }

    public String toString(ByteBuffer value) {
        return ByteBufferUtil.bytesToHex(value);
    }

    public Class<byte[]> getType() {
        return byte[].class;
    }

    @Override
    public String toCQLLiteral(ByteBuffer buffer) {
        return buffer == null ? "null" : "0x" + toString(deserialize(buffer));
    }

    @Override
    public ByteBuffer serialize(byte[] value) {
        return ByteBuffer.wrap(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer bytes) {
        byte[] buf = new byte[bytes.remaining()];
        bytes.slice().get(buf);
        return buf;
    }

    @Override
    public String toString(byte[] value) {
        return ByteBufferUtil.bytesToHex(ByteBuffer.wrap(value));
    }
}
