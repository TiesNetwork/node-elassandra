package network.tiesdb.handler.impl.v0r0.controller;

import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tiesdb.protocol.exception.TiesDBProtocolException;
import com.tiesdb.protocol.v0r0.TiesDBProtocolV0R0.Conversation;
import com.tiesdb.protocol.v0r0.exception.TiesDBProtocolMessageException;
import com.tiesdb.protocol.v0r0.reader.ModificationResponseReader;
import com.tiesdb.protocol.v0r0.reader.ModificationResponseReader.ModificationResponse;
import com.tiesdb.protocol.v0r0.reader.ModificationResponseReader.ModificationResult;
import com.tiesdb.protocol.v0r0.reader.ModificationResultErrorReader.ModificationResultError;
import com.tiesdb.protocol.v0r0.reader.ModificationResultSuccessReader.ModificationResultSuccess;
import com.tiesdb.protocol.v0r0.reader.Reader.Response;
import com.tiesdb.protocol.v0r0.reader.RecollectionResponseReader.RecollectionResponse;
import com.tiesdb.protocol.v0r0.reader.SchemaResponseReader.SchemaResponse;

import network.tiesdb.exception.TiesException;
import network.tiesdb.service.api.TiesService;
import network.tiesdb.service.scope.api.TiesServiceScope;
import network.tiesdb.service.scope.api.TiesServiceScopeException;
import network.tiesdb.service.scope.api.TiesServiceScopeModification;
import network.tiesdb.service.scope.api.TiesServiceScopeResult;

public class ResponseHandler implements Response.Visitor<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(ResponseHandler.class);

    private final TiesService service;

    public ResponseHandler(TiesService service) {
        this.service = service;
    }

    public void handle(Conversation session, Response response) throws TiesDBProtocolException {
        if (null == response) {
            throw new TiesDBProtocolException("Empty response");
        }
        LOG.debug("Response: {}", response);
        if (null == response.getMessageId()) {
            throw new TiesDBProtocolException("Response MessageId is required");
        }
        try {
            response.accept(this);
        } catch (TiesDBProtocolMessageException e) {
            throw e;
        } catch (Exception e) {
            throw new TiesDBProtocolMessageException(response.getMessageId(), e);
        }
    }

    @Override
    public Void on(ModificationResponse modificationResponse) throws TiesDBProtocolException {

        BigInteger messageId = modificationResponse.getMessageId();

        TiesServiceScope serviceScope = service.newServiceScope();
        for (ModificationResult modificationResult : modificationResponse.getModificationResults()) {
            try {
                TiesServiceScopeResult.Result result = modificationResult
                        .accept(new ModificationResponseReader.ModificationResult.Visitor<TiesServiceScopeResult.Result>() {
                            @Override
                            public TiesServiceScopeResult.Result on(ModificationResultSuccess modificationResultSuccess) {
                                return new TiesServiceScopeModification.Result.Success() {
                                    @Override
                                    public byte[] getHeaderHash() {
                                        return modificationResultSuccess.getEntryHeaderHash();
                                    }
                                };
                            }

                            @Override
                            public TiesServiceScopeResult.Result on(ModificationResultError modificationResultError) {
                                return new TiesServiceScopeModification.Result.Error() {

                                    @Override
                                    public byte[] getHeaderHash() {
                                        return modificationResultError.getEntryHeaderHash();
                                    }

                                    @Override
                                    public Throwable getError() {
                                        return new TiesException(modificationResultError.getMessage());
                                    }
                                };
                            }
                        });
                serviceScope.result(new TiesServiceScopeResult() {

                    @Override
                    public BigInteger getMessageId() {
                        return messageId;
                    }

                    @Override
                    public Result getResult() {
                        return result;
                    }

                });
            } catch (TiesServiceScopeException e) {
                throw new TiesDBProtocolException("Response handling failed", e);
            }
        }
        return null;
    }

    @Override
    public Void on(RecollectionResponse recollectionResponse) throws TiesDBProtocolException {
        // TODO Auto-generated method stub
        throw new TiesDBProtocolException("Not implemented");
    }

    @Override
    public Void on(SchemaResponse schemaResponse) throws TiesDBProtocolException {
        // TODO Auto-generated method stub
        throw new TiesDBProtocolException("Not implemented");
    }

}
