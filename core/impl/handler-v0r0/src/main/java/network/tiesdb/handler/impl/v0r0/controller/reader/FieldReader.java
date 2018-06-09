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
package network.tiesdb.handler.impl.v0r0.controller.reader;

import static network.tiesdb.handler.impl.v0r0.controller.reader.ReaderUtil.DEFAULT_DIGEST_ALG;
import static network.tiesdb.handler.impl.v0r0.controller.reader.ReaderUtil.acceptEach;
import static network.tiesdb.handler.impl.v0r0.controller.reader.ReaderUtil.end;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.lib.crypto.digest.DigestManager;
import com.tiesdb.lib.crypto.digest.api.Digest;
import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;

import network.tiesdb.handler.impl.v0r0.util.FormatUtil;
import one.utopic.sparse.ebml.format.ASCIIStringFormat;
import one.utopic.sparse.ebml.format.BytesFormat;
import one.utopic.sparse.ebml.format.UTF8StringFormat;

public class FieldReader implements Reader<FieldReader.Field> {

    private static final Logger LOG = LoggerFactory.getLogger(FieldReader.class);

    public static class Field {

        private String name;
        private String type;
        private byte[] hash;
        private byte[] rawValue;
        private byte[] rawBytes;

        @Override
        public String toString() {
            return "Field [name=" + name + ", type=" + type + ", hash=" + Arrays.toString(hash) + ", rawValue="
                    + FormatUtil.pringHex(rawValue) + ", rawBytes=" + FormatUtil.pringHex(rawBytes) + "]";
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public byte[] getHash() {
            return hash;
        }

        public byte[] getRawValue() {
            return rawValue;
        }

        public byte[] getRawBytes() {
            return rawBytes;
        }

    }

    private final Digest fieldDigest;
    private final Consumer<Byte> fieldHashListener;

    public FieldReader() {
        this.fieldDigest = DigestManager.getDigest(DEFAULT_DIGEST_ALG);
        this.fieldHashListener = fieldDigest::update;
    }

    public boolean acceptField(Conversation session, Event e, Field field) throws TiesDBProtocolException {
        switch (e.getType()) {
        case FIELD_NAME:
            field.name = session.read(UTF8StringFormat.INSTANCE);
            LOG.debug("FIELD_NAME: {}", field.name);
            end(session, e);
            return true;
        case FIELD_TYPE:
            session.removeReaderListener(fieldHashListener);
            field.type = session.read(ASCIIStringFormat.INSTANCE);
            LOG.debug("FIELD_TYPE: {}", field.type);
            end(session, e);
            session.addReaderListener(fieldHashListener);
            return true;
        case FIELD_HASH:
            session.removeReaderListener(fieldHashListener);
            field.hash = session.read(BytesFormat.INSTANCE);
            LOG.debug("FIELD_HASH: {}", new Object() {
                @Override
                public String toString() {
                    return DatatypeConverter.printHexBinary(field.hash);
                }
            });
            end(session, e);
            return true;
        case FIELD_VALUE:
            field.rawValue = session.read(BytesFormat.INSTANCE);
            LOG.debug("FIELD_VALUE: {}", new Object() {
                @Override
                public String toString() {
                    return DatatypeConverter.printHexBinary(field.rawValue);
                }
            });
            end(session, e);
            return true;
        // $CASES-OMITTED$
        default:
            return false;
        }
    }

    @Override
    public boolean accept(Conversation session, Event e, Field field) throws TiesDBProtocolException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            Consumer<Byte> rawBytesListener = baos::write;
            session.addReaderListener(rawBytesListener);
            try {
                fieldDigest.reset();
                session.addReaderListener(fieldHashListener);
                acceptEach(session, e, this::acceptField, field);
                if (null == field.hash) {
                    byte[] fieldHash = new byte[fieldDigest.getDigestSize()];
                    if (fieldDigest.getDigestSize() == fieldDigest.doFinal(fieldHash, 0)) {
                        LOG.debug("FIELD_HASH_CALCULATED: {}", new Object() {
                            @Override
                            public String toString() {
                                return DatatypeConverter.printHexBinary(fieldHash);
                            }
                        });
                        field.hash = fieldHash;
                    } else {
                        throw new TiesDBProtocolException("Field digest failed to compute hash");
                    }
                }
            } finally {
                session.removeReaderListener(fieldHashListener);
                session.removeReaderListener(rawBytesListener);
            }
            field.rawBytes = baos.toByteArray();
        } catch (IOException ex) {
            throw new TiesDBProtocolException(ex);
        }
        return true;
    }

}
