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

import static network.tiesdb.handler.impl.v0r0.controller.reader.ReaderUtil.acceptEach;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation.Event;
import com.tiesdb.protocol.v0r0.ebml.TiesDBType;

public class ListReader<T> implements Reader<Consumer<T>> {

    private final Reader<T> elementController;
    private final TiesDBType elementType;
    private final Supplier<T> supplier;

    public ListReader(TiesDBType elementType, Supplier<T> supplier, Reader<T> elementController) {
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
