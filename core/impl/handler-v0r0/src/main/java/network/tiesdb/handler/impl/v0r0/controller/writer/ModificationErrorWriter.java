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
package network.tiesdb.handler.impl.v0r0.controller.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;

import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationResponseWriter.ModificationResult;
import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationResponseWriter.ModificationResult.Visitor;

import static com.tiesdb.protocol.v0r0.ebml.TiesDBType.*;

import one.utopic.sparse.ebml.format.BytesFormat;
import one.utopic.sparse.ebml.format.UTF8StringFormat;

import static network.tiesdb.handler.impl.v0r0.controller.writer.WriterUtil.*;

public class ModificationErrorWriter implements Writer<ModificationErrorWriter.ModificationErrorResult> {

    private static final Logger LOG = LoggerFactory.getLogger(ModificationErrorWriter.class);

    public static interface ModificationErrorResult extends ModificationResult {

        Throwable getError();

        byte[] getEntryHeaderHash();

        @Override
        default void accept(Visitor v) throws TiesDBProtocolException {
            v.on(this);
        }

    }

    @Override
    public void accept(Conversation session, ModificationErrorResult error) throws TiesDBProtocolException {
        LOG.debug("ModificationErrorResult {}", error);

        write(MODIFICATION_ERROR, //
                write(ENTRY_HASH, BytesFormat.INSTANCE, error.getEntryHeaderHash()), //
                s -> {
                    if (null != error.getError()) {
                        write(ERROR_MESSAGE, UTF8StringFormat.INSTANCE, String.valueOf(error.getError().getMessage())).accept(s);
                    }
                } //
        ).accept(session);

    }

}
