package network.tiesdb.handler.impl.v0r0.controller;

import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.acceptEach;
import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.checkEntryFieldsHash;

import java.util.HashMap;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.ebml.TiesDBType;

import network.tiesdb.handler.impl.v0r0.controller.EntryHeaderController.EntryHeader;
import network.tiesdb.handler.impl.v0r0.controller.FieldController.Field;

public class EntryController implements Controller<EntryController.Entry> {

    public static class Entry {

        private EntryHeader header;
        private HashMap<String, Field> fields = new HashMap<>();

        @Override
        public String toString() {
            return "Entry [header=" + header + ", fields=" + fields + "]";
        }

        public EntryHeader getHeader() {
            return header;
        }

        public HashMap<String, Field> getFields() {
            return fields;
        }

    }

    private final EntryHeaderController entryHeaderController;
    private final MultiController<Field> fieldListController;

    public EntryController() {
        this.entryHeaderController = new EntryHeaderController();
        this.fieldListController = new MultiController<>(TiesDBType.FIELD, () -> new Field(), new FieldController());
    }

    private boolean acceptEntry(Conversation session, Event e, Entry entry) throws TiesDBProtocolException {
        switch (e.getType()) {
        case ENTRY_HEADER:
            EntryHeader header = new EntryHeader();
            boolean result = entryHeaderController.accept(session, e, header);
            if (result) {
                if (null != entry.header) {
                    throw new TiesDBProtocolException("Multiple headers detected! Should be only one header in each entry.");
                }
                entry.header = header;
            }
            return true;
        case FIELD_LIST:
            return fieldListController.accept(session, e, field -> {
                entry.fields.put(field.getName(), field);
            });
        // $CASES-OMITTED$
        default:
            return false;
        // throw new TiesDBProtocolException("Illegal packet format");
        }
    }

    @Override
    public boolean accept(Conversation session, Event e, Entry entry) throws TiesDBProtocolException {
        acceptEach(session, e, this::acceptEntry, entry);
        if (!checkEntryFieldsHash(entry)) {
            throw new TiesDBProtocolException("Entry fields hash missmatch.");
        }
        return true;
    }

}
