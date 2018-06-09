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

import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationErrorWriter.ModificationErrorResult;
import network.tiesdb.handler.impl.v0r0.controller.writer.ModificationSuccessWriter.ModificationSuccessResult;

import static com.tiesdb.protocol.v0r0.ebml.TiesDBType.*;

import one.utopic.sparse.ebml.format.BigIntegerFormat;
import static network.tiesdb.handler.impl.v0r0.controller.writer.WriterUtil.*;

public class ModificationResponseWriter implements Writer<ModificationResponseWriter.ModificationResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(ModificationResponseWriter.class);

    private static final ModificationSuccessWriter modificationSuccessWriter = new ModificationSuccessWriter();

    private static final ModificationErrorWriter modificationErrorWriter = new ModificationErrorWriter();

    public static interface ModificationResponse extends Writer.Response {

        @Override
        public default void accept(Visitor v) throws TiesDBProtocolException {
            v.on(this);
        }

        public Iterable<ModificationResult> getResults();

    }

    public static interface ModificationResult {

        interface Visitor {

            void on(ModificationErrorResult result) throws TiesDBProtocolException;

            void on(ModificationSuccessResult result) throws TiesDBProtocolException;

        }

        public void accept(Visitor v) throws TiesDBProtocolException;

    }

    @Override
    public void accept(Conversation session, ModificationResponse response) throws TiesDBProtocolException {
        LOG.debug("ModificationResponse {}", response);

        write(MODIFICATION_RESPONSE, //
                write(MESSAGE_ID, BigIntegerFormat.INSTANCE, response.getMessageId()), //
                s -> {
                    for (ModificationResult result : response.getResults()) {
                        result.accept(new ModificationResult.Visitor() {

                            @Override
                            public void on(ModificationErrorResult result) throws TiesDBProtocolException {
                                modificationErrorWriter.accept(s, result);
                            }

                            @Override
                            public void on(ModificationSuccessResult result) throws TiesDBProtocolException {
                                modificationSuccessWriter.accept(s, result);
                            }

                        });
                    }
                }).accept(session);

    }

}
