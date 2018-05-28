package network.tiesdb.handler.impl.v0r0.controller;

import static network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.acceptEach;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.ebml.TiesDBType;

public class MultiController<T> implements Controller<Consumer<T>> {

    private final Controller<T> elementController;
    private final TiesDBType elementType;
    private final Supplier<T> supplier;

    public MultiController(TiesDBType elementType, Supplier<T> supplier, Controller<T> elementController) {
        this.elementType = elementType;
        this.supplier = supplier;
        this.elementController = elementController;
    }

    public boolean acceptElements(Conversation session, Event e, Consumer<T> t) throws TiesDBProtocolException {
        if (null != e && elementType.equals(e.getType())) {
            T element = supplier.get();
            boolean result = elementController.accept(session, e, element);
            if (result) {
                t.accept(element);
            }
            return result;
        }
        return false;
    }

    @Override
    public boolean accept(Conversation session, Event e, Consumer<T> t) throws TiesDBProtocolException {
        acceptEach(session, e, this::acceptElements, t);
        return true;
    }

}
