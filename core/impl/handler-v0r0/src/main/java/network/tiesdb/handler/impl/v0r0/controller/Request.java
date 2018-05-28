package network.tiesdb.handler.impl.v0r0.controller;

import network.tiesdb.handler.impl.v0r0.controller.ModificationRequestController.ModificationRequest;
import network.tiesdb.service.scope.api.TiesServiceScopeException;

public interface Request {

    interface Visitor {

        void on(ModificationRequest modificationRequest) throws TiesServiceScopeException;

    }

    void accept(Visitor v) throws TiesServiceScopeException;
}