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

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Iterator;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.reader.RecollectionRequestReader.RecollectionRequest;
import com.tiesdb.protocol.v0r0.writer.EntryHeaderWriter.EntryHeader;
import com.tiesdb.protocol.v0r0.writer.FieldWriter.Field;
import com.tiesdb.protocol.v0r0.writer.FieldWriter.Field.*;
import com.tiesdb.protocol.v0r0.writer.RecollectionResultWriter.RecollectionResult;
import com.tiesdb.protocol.v0r0.writer.ResponseWriter;
import com.tiesdb.protocol.v0r0.writer.Writer.Response;

import network.tiesdb.handler.impl.v0r0.controller.ControllerUtil.WriteConverter;
import network.tiesdb.service.scope.api.TiesEntryHeader;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeRecollection.Result;
import one.utopic.sparse.ebml.EBMLFormat;

public class ResponseController {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseController.class);

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF8");

    private final ResponseWriter responseWriter;

    public ResponseController(ResponseWriter responseWriter) {
        this.responseWriter = responseWriter;
    }

    public void handle(Conversation session, Response response) throws TiesDBProtocolException {
        responseWriter.accept(session, response);
    }

    public RecollectionResult convertRecollectionResult(RecollectionRequest request, Result result) {
        TiesEntryHeader resultHeader = result.getEntryHeader();
        EntryHeader entryHeader = new EntryHeader() {

            @Override
            public byte[] getSigner() {
                return resultHeader.getSigner();
            }

            @Override
            public byte[] getSignature() {
                return resultHeader.getSignature();
            }

            @Override
            public String getTablespaceName() {
                return request.getTablespaceName();
            }

            @Override
            public String getTableName() {
                return request.getTableName();
            }

            @Override
            public BigInteger getEntryVersion() {
                return resultHeader.getEntryVersion();
            }

            @Override
            public Date getEntryTimestamp() {
                return resultHeader.getEntryTimestamp();
            }

            @Override
            public byte[] getEntryOldHash() {
                return resultHeader.getEntryOldHash();
            }

            @Override
            public Integer getEntryNetwork() {
                return (int) resultHeader.getEntryNetwork();
            }

            @Override
            public byte[] getEntryFldHash() {
                return resultHeader.getEntryFldHash();
            }
        };

        Stream<Field> entryFieldStream = result.getEntryFields().stream().map(this::convertResultField);
        Iterable<Field> entryFields = new Iterable<Field>() {
            @Override
            public Iterator<Field> iterator() {
                return entryFieldStream.iterator();
            }
        };

        Stream<Field> computedFieldStream = result.getComputedFields().stream().map(this::convertResultField);
        Iterable<Field> computedFields = new Iterable<Field>() {
            @Override
            public Iterator<Field> iterator() {
                return computedFieldStream.iterator();
            }
        };
        return new RecollectionResult() {

            @Override
            public EntryHeader getEntryHeader() {
                return entryHeader;
            }

            @Override
            public Iterable<Field> getEntryFields() {
                return entryFields;
            }

            @Override
            public Iterable<Field> getComputedFields() {
                return computedFields;
            }
        };
    }

    protected Field convertResultField(Result.Field f) {
        try {
            return f.accept(new Result.Field.Visitor<Field>() {

                @Override
                public Field on(Result.Field.HashField field) {
                    return new HashField() {

                        @Override
                        public String getName() {
                            return field.getName();
                        }

                        @Override
                        public String getType() {
                            return field.getType();
                        }

                        @Override
                        public byte[] getHash() {
                            return field.getHash();
                        }

                    };
                }

                @Override
                public Field on(Result.Field.ValueField field) throws TiesServiceScopeException {

                    @SuppressWarnings("unchecked")
                    WriteConverter<Object> wc = (WriteConverter<Object>) ControllerUtil.writeConverterForType(field.getType());

                    return new ValueField() {

                        @Override
                        public String getName() {
                            return field.getName();
                        }

                        @Override
                        public String getType() {
                            return field.getType();
                        }

                        @Override
                        public EBMLFormat<Object> getFormat() {
                            return wc.getFormat();
                        }

                        @Override
                        public Object getValue() {
                            return wc.convert(field.getValue());
                        }

                    };
                }

            });
        } catch (Exception e) {
            LOG.error("Can't convert field {}", f);
            // FIXME Implement more robust error handling
            return new HashField() {

                @Override
                public String getName() {
                    return "ERROR(" + f.getName() + ")";
                }

                @Override
                public String getType() {
                    return f.getType();
                }

                @Override
                public byte[] getHash() {
                    return e.getMessage().getBytes(DEFAULT_CHARSET);
                }

            };
        }
    }
}
