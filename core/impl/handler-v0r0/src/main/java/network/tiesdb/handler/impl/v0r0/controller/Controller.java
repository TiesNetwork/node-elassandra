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

import network.tiesdb.handler.impl.v0r0.controller.request.EntryController.Entry;
import network.tiesdb.handler.impl.v0r0.controller.request.FieldController.Field;
import network.tiesdb.handler.impl.v0r0.controller.request.SignatureController.Signature;

@FunctionalInterface
public interface Controller<T> {

    boolean accept(Conversation session, Event e, T t) throws TiesDBProtocolException;

}
