package network.tiesdb.handler.impl.v0r0.controller;

import static java.util.Objects.requireNonNull;

import java.security.SignatureException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.lib.crypto.digest.DigestManager;
import com.tiesdb.lib.crypto.digest.api.Digest;
import com.tiesdb.lib.crypto.ecc.signature.ECKey;
import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.EventState;

import network.tiesdb.handler.impl.v0r0.controller.EntryController.Entry;
import network.tiesdb.handler.impl.v0r0.controller.FieldController.Field;
import network.tiesdb.handler.impl.v0r0.controller.SignatureController.Signature;

@FunctionalInterface
public interface Controller<T> {

    boolean accept(Conversation session, Event e, T t) throws TiesDBProtocolException;

}

final class ControllerUtil {

    public static final String DEFAULT_DIGEST_ALG = DigestManager.KECCAK_256;

    private static final Logger LOG = LoggerFactory.getLogger(ControllerUtil.class);

    private ControllerUtil() {
    }

    static <T> void acceptEach(Conversation session, Event rootEvent, Controller<T> controller, T t) throws TiesDBProtocolException {
        requireNonNull(session);
        requireNonNull(rootEvent);
        if (!EventState.BEGIN.equals(rootEvent.getState())) {
            throw new TiesDBProtocolException("Illegal root event: " + rootEvent);
        }
        // System.out.println("RootBEGINEvent: " + rootEvent);
        Event event;
        while (null != controller && null != (event = session.get())) {
            // System.out.println("\t Event: " + event);
            if (EventState.BEGIN.equals(event.getState())) {
                // System.out.println("BEGINEvent: " + event);
                if (!controller.accept(session, event, t)) {
                    session.skip();
                    LOG.warn("{} event skipped", event.getType());
                    end(session, event);
                }
            } else if (EventState.END.equals(event.getState()) && event.getType().equals(rootEvent.getType())) {
                // System.out.println("RootENDEvent: " + event);
                break;
            } else {
                throw new TiesDBProtocolException("Illegal event: " + event);
            }
        }
    }

    static void end(Conversation session, Event event) throws TiesDBProtocolException {
        requireNonNull(session);
        requireNonNull(event);
        if (!EventState.BEGIN.equals(event.getState())) {
            throw new TiesDBProtocolException("Illegal root event: " + event);
        }
        Event endEvent;
        if (null != (endEvent = session.get()) && EventState.END.equals(endEvent.getState())
                && endEvent.getType().equals(event.getType())) {
            // System.out.println("ENDEvent: " + endEvent);
        } else {
            throw new TiesDBProtocolException("Illegal event: " + endEvent);
        }
    }

    static boolean checkSignature(byte[] messageHash, Signature signature) throws TiesDBProtocolException {
        try {
            byte[] signer = ECKey.signatureToAddressBytes(messageHash, signature.getSignature());
            return Arrays.equals(signer, signature.getSigner());
        } catch (SignatureException e) {
            throw new TiesDBProtocolException(e);
        }
    }

    static boolean checkEntryFieldsHash(Entry entry) {
        HashMap<String, Field> fields = entry.getFields();
        Digest fldDigest = DigestManager.getDigest(DEFAULT_DIGEST_ALG);
        TreeSet<String> fieldNames = new TreeSet<>(fields.keySet());
        for (String fieldName : fieldNames) {
            fldDigest.update(fields.get(fieldName).getHash());
        }
        byte[] fldHash = new byte[fldDigest.getDigestSize()];
        fldDigest.doFinal(fldHash, 0);
        LOG.debug("ENTRY_FLD_HASH_CALCULATED: {}", DatatypeConverter.printHexBinary(fldHash));
        return Arrays.equals(fldHash, entry.getHeader().getEntryFldHash());
    }
}