package network.tiesdb.handler.impl.v0r0.controller;

import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.DEFAULT_DIGEST_ALG;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.acceptEach;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.end;

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

public class FieldController implements Controller<FieldController.Field> {

    private static final Logger LOG = LoggerFactory.getLogger(FieldController.class);

    public static class Field {

        private String name;

        private String type;

        private byte[] hash;

        private byte[] value;

        @Override
        public String toString() {
            return "Field [name=" + name + ", type=" + type + ", hash=" + FormatUtil.pringHex(hash) + ", value="
                    + FormatUtil.pringHex(value) + "]";
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

        public byte[] getValue() {
            return value;
        }

    }

    private final Digest fieldDigest;
    private final Consumer<Byte> fieldHashListener;

    public FieldController() {
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
            field.value = session.read(BytesFormat.INSTANCE);
            LOG.debug("FIELD_VALUE: {}", new Object() {
                @Override
                public String toString() {
                    return DatatypeConverter.printHexBinary(field.value);
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
        }
        return true;
    }

}
