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

import static network.tiesdb.handler.impl.v0r0.controller.reader.ReaderUtil.end;

import java.util.function.Consumer;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;

import network.tiesdb.handler.impl.v0r0.util.FormatUtil;
import one.utopic.sparse.ebml.format.BytesFormat;

public class SignatureReader implements Reader<SignatureReader.Signature> {

    public static class Signature {

        private byte[] signature;
        private byte[] signer;

        @Override
        public String toString() {
            return "Signature [signature=" + FormatUtil.pringHex(signature) + ", signer=" + FormatUtil.pringHex(signer) + "]";
        }

        public byte[] getSignature() {
            return signature;
        }

        public byte[] getSigner() {
            return signer;
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(SignatureReader.class);
    private final Consumer<Byte> hashListener;

    public SignatureReader(Consumer<Byte> hashListener) {
        this.hashListener = hashListener;
    }

    @Override
    public boolean accept(Conversation session, Event e, Signature signature) throws TiesDBProtocolException {
        switch (e.getType()) {
        case SIGNATURE:
            session.removeReaderListener(hashListener);
            signature.signature = session.read(BytesFormat.INSTANCE);
            session.addReaderListener(hashListener);
            LOG.debug("SIGNATURE : {}", new Object() {
                @Override
                public String toString() {
                    return DatatypeConverter.printHexBinary(signature.signature);
                }
            });
            end(session, e);
            return true;
        case SIGNER:
            signature.signer = session.read(BytesFormat.INSTANCE);
            LOG.debug("SIGNER : {}", new Object() {
                @Override
                public String toString() {
                    return DatatypeConverter.printHexBinary(signature.signer);
                }
            });
            end(session, e);
            return true;
        // $CASES-OMITTED$
        default:
            return false;
        // throw new TiesDBProtocolException("Illegal packet format");
        }
    }

}
