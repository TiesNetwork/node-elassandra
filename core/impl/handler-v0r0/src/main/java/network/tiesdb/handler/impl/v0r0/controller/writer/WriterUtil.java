package network.tiesdb.handler.impl.v0r0.controller.writer;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.EventState;
import com.tiesdb.protocol.v0r0.ebml.TiesDBType;
import com.tiesdb.protocol.v0r0.util.CheckedConsumer;

import one.utopic.sparse.ebml.EBMLWriter;

final class WriterUtil {

    public static interface ConversationConsumer extends CheckedConsumer<Conversation, TiesDBProtocolException> {
    }

    private WriterUtil() {
    }

    public static ConversationConsumer write(TiesDBType t, ConversationConsumer... cs) {
        return write(t, s -> {
            for (ConversationConsumer c : cs) {
                c.accept(s);
            }
        });
    }

    public static ConversationConsumer write(TiesDBType t, ConversationConsumer c) {
        return s -> writeTag(s, t, c);
    }

    public static <O> ConversationConsumer write(TiesDBType t, EBMLWriter.EBMLWriteFormat<O> f, O d) {
        return write(t, s -> writeData(s, f, d));
    }

    private static void writeTag(Conversation s, TiesDBType t, ConversationConsumer c) throws TiesDBProtocolException {
        s.accept(new Event(t, EventState.BEGIN));
        c.accept(s);
        s.accept(new Event(t, EventState.END));
    }

    private static <O> void writeData(Conversation s, EBMLWriter.EBMLWriteFormat<O> format, O data) throws TiesDBProtocolException {
        s.write(format, data);
    }
}
