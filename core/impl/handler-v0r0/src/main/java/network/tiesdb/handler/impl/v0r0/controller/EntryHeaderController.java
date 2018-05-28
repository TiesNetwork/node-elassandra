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
package network.tiesdb.handler.impl.v0r0.controller;

import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.DEFAULT_DIGEST_ALG;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.acceptEach;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.end;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.checkSignature;

import java.util.Date;
import java.util.function.Consumer;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.lib.crypto.digest.DigestManager;
import com.tiesdb.lib.crypto.digest.api.Digest;
import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;

import network.tiesdb.handler.impl.v0r0.controller.SignatureController.Signature;
import network.tiesdb.handler.impl.v0r0.util.FormatUtil;
import one.utopic.sparse.ebml.format.BytesFormat;
import one.utopic.sparse.ebml.format.DateFormat;
import one.utopic.sparse.ebml.format.IntegerFormat;
import one.utopic.sparse.ebml.format.UTF8StringFormat;

public class EntryHeaderController implements Controller<EntryHeaderController.EntryHeader> {

    private static final Logger LOG = LoggerFactory.getLogger(EntryHeaderController.class);

    public static class EntryHeader {

        private String tablespaceName;
        private String tableName;
        private Integer entryType;
        private Date entryTimestamp;
        private Integer entryVersion;
        private Integer entryNetwork;
        private byte[] entryOldHash;
        private byte[] entryFldHash;

        private final Signature signature = new Signature();

        @Override
        public String toString() {
            return "EntryHeader [tablespaceName=" + tablespaceName + ", tableName=" + tableName + ", entryType=" + entryType
                    + ", entryTimestamp=" + entryTimestamp + ", entryVersion=" + entryVersion + ", entryNetwork=" + entryNetwork
                    + ", entryOldHash=" + FormatUtil.pringHex(entryOldHash) + ", entryFldHash=" + FormatUtil.pringHex(entryFldHash)
                    + ", signature=" + signature + "]";
        }

        public String getTablespaceName() {
            return tablespaceName;
        }

        public String getTableName() {
            return tableName;
        }

        public Integer getEntryType() {
            return entryType;
        }

        public Date getEntryTimestamp() {
            return entryTimestamp;
        }

        public Integer getEntryVersion() {
            return entryVersion;
        }

        public Integer getEntryNetwork() {
            return entryNetwork;
        }

        public byte[] getEntryOldHash() {
            return entryOldHash;
        }

        public byte[] getEntryFldHash() {
            return entryFldHash;
        }

        public Signature getSignature() {
            return signature;
        }

    }

    private final Digest headerDigest;
    private final Consumer<Byte> headerHashListener;
    private final SignatureController signatureController;

    public EntryHeaderController() {
        this.headerDigest = DigestManager.getDigest(DEFAULT_DIGEST_ALG);
        this.headerHashListener = headerDigest::update;
        this.signatureController = new SignatureController(headerHashListener);
    }

    public boolean acceptEntryHeader(Conversation session, Event e, EntryHeader header) throws TiesDBProtocolException {
        switch (e.getType()) {
        case ENTRY_TABLESPACE_NAME:
            header.tablespaceName = session.read(UTF8StringFormat.INSTANCE);
            LOG.debug("ENTRY_TABLESPACE_NAME: {}", header.tablespaceName);
            end(session, e);
            return true;
        case ENTRY_TABLE_NAME:
            header.tableName = session.read(UTF8StringFormat.INSTANCE);
            LOG.debug("ENTRY_TABLE_NAME: {}", header.tableName);
            end(session, e);
            return true;
        case ENTRY_TYPE:
            header.entryType = session.read(IntegerFormat.INSTANCE);
            LOG.debug("ENTRY_TYPE: {}", header.entryType);
            end(session, e);
            return true;
        case ENTRY_TIMESTAMP:
            header.entryTimestamp = session.read(DateFormat.INSTANCE);
            LOG.debug("ENTRY_TIMESTAMP: {}", header.entryTimestamp);
            end(session, e);
            return true;
        case ENTRY_VERSION:
            header.entryVersion = session.read(IntegerFormat.INSTANCE);
            LOG.debug("ENTRY_VERSION: {}", header.entryVersion);
            end(session, e);
            return true;
        case ENTRY_NETWORK:
            header.entryNetwork = session.read(IntegerFormat.INSTANCE);
            LOG.debug("ENTRY_NETWORK: {}", header.entryNetwork);
            end(session, e);
            return true;
        case ENTRY_OLD_HASH:
            header.entryOldHash = session.read(BytesFormat.INSTANCE);
            LOG.debug("ENTRY_OLD_HASH: {}", new Object() {
                @Override
                public String toString() {
                    return DatatypeConverter.printHexBinary(header.entryOldHash);
                }
            });
            end(session, e);
            return true;
        case ENTRY_FLD_HASH:
            header.entryFldHash = session.read(BytesFormat.INSTANCE);
            LOG.debug("ENTRY_FLD_HASH: {}", new Object() {
                @Override
                public String toString() {
                    return DatatypeConverter.printHexBinary(header.entryFldHash);
                }
            });
            end(session, e);
            return true;
        // $CASES-OMITTED$
        default:
            return signatureController.accept(session, e, header.signature);
        // throw new TiesDBProtocolException("Illegal packet format");
        }
    }

    @Override
    public boolean accept(Conversation session, Event e, EntryHeader header) throws TiesDBProtocolException {
        try {
            headerDigest.reset();
            session.addReaderListener(headerHashListener);
            acceptEach(session, e, this::acceptEntryHeader, header);
            byte[] headerHash = new byte[headerDigest.getDigestSize()];
            if (headerDigest.getDigestSize() == headerDigest.doFinal(headerHash, 0)) {
                LOG.debug("ENTRY_HASH: {}", new Object() {
                    @Override
                    public String toString() {
                        return DatatypeConverter.printHexBinary(headerHash);
                    }
                });
                if (!checkSignature(headerHash, header.signature)) {
                    throw new TiesDBProtocolException("Header signature check failed.");
                }
            } else {
                throw new TiesDBProtocolException("Header digest failed to compute hash");
            }
        } finally {
            session.removeReaderListener(headerHashListener);
        }
        return true;
    }

}
