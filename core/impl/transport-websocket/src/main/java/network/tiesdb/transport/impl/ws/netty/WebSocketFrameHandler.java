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
package network.tiesdb.transport.impl.ws.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import network.tiesdb.transport.api.TiesTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger logger = LoggerFactory.getLogger(WebSocketFrameHandler.class);

	private final TiesTransport transport;
    
    public WebSocketFrameHandler(TiesTransport transport){
		if (null == transport) {
			throw new NullPointerException("The transport should not be null");
		}
		this.transport = transport;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
        // ping and pong frames already handled

        if (frame instanceof TextWebSocketFrame || frame instanceof BinaryWebSocketFrame) {
            // Send the uppercase string back.
            //String request = ((TextWebSocketFrame) frame).text();
            logger.info("{} received {} bytes", ctx.channel(), frame.content().readableBytes());
            
			try (WebSocketRequestHandler request = new WebSocketRequestHandler(frame)) {
				try (WebSocketResponseHandler response = new WebSocketResponseHandler(ctx)) {
					transport.getHandler().handle(request, response);
				}
			}

            //ctx.channel().writeAndFlush(new TextWebSocketFrame(request.toUpperCase(Locale.US)));
        } else {
            throw new UnsupportedOperationException("unsupported frame type: " + frame.getClass().getName());
        }
    }
}
