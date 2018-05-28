package network.tiesdb.handler.impl.v0r0.controller;

import java.util.function.Consumer;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;

import network.tiesdb.handler.impl.v0r0.util.FormatUtil;
import one.utopic.sparse.ebml.format.BytesFormat;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.end;

public class SignatureController implements Controller<SignatureController.Signature> {

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

    private static final Logger LOG = LoggerFactory.getLogger(SignatureController.class);
    private final Consumer<Byte> hashListener;

    public SignatureController(Consumer<Byte> hashListener) {
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
