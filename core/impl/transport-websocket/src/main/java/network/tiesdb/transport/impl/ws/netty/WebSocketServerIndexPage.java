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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;

import org.stringtemplate.v4.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

/**
 * Generates the demo HTML page which is served at http://localhost:8080/
 */
public final class WebSocketServerIndexPage {

    private static final String NEWLINE = System.lineSeparator();
	private static final String STYLE = "eclipse";
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF8");

	//FIXME: Delete
    public static ByteBuf getContent(String webSocketLocation) {
        return Unpooled.copiedBuffer(
                "<html><head><title>Web Socket Test</title></head>" + NEWLINE +
        		"<link rel=stylesheet href=\"http://codemirror.net/lib/codemirror.css\">" + NEWLINE +
        		"<link rel=stylesheet href=\"http://codemirror.net/theme/" + STYLE + ".css\">" + NEWLINE +
        		"<style type=\"text/css\">" + NEWLINE +
        		".CodeMirror { border: 1px solid black; font-size:13px }" + NEWLINE +
        		"</style>" + NEWLINE +
                "<script src=\"http://codemirror.net/lib/codemirror.js\"></script>" + NEWLINE +
                "<script src=\"http://codemirror.net/mode/javascript/javascript.js\"></script>" + NEWLINE +
                "<script type=\"text/javascript\">" + NEWLINE +
                "  function onLoad() {" + NEWLINE +
                "    let options = {" + NEWLINE +
                "      mode: \"javascript\"," + NEWLINE +
                "      theme: \"" + STYLE + "\"" + NEWLINE +
                "    };" + NEWLINE +
                "    var editor = CodeMirror.fromTextArea(document.getElementById('responseText'), options);" + NEWLINE +
                "    document.getElementById('responseText').editor = editor;" + NEWLINE +
                "    var areaEditor = CodeMirror.fromTextArea(document.getElementById('messageArea'), options);" + NEWLINE +
                "    document.getElementById('messageArea').editor = areaEditor;" + NEWLINE +
                "    areaEditor.setValue(localStorage.getItem('messageAreaText') || \"" +
                //-----------------------------------------------------------------
                //"{\\\"Hello\\\":\\\"World!\\\"}" +
                "{\\\"Hello\\\":\\\"World!\\\"}" +
                //-----------------------------------------------------------------
                "\");" + NEWLINE +
                "  }" + NEWLINE +
                "</script>" + NEWLINE +
                "<body onload=\"onLoad()\">" + NEWLINE +
                "<script type=\"text/javascript\">" + NEWLINE +
                "let socket;" + NEWLINE +
                "function openSocket(callback) {" + NEWLINE +
                "  let socket;" + NEWLINE +
                "  if (!window.WebSocket) {" + NEWLINE +
                "    window.WebSocket = window.MozWebSocket;" + NEWLINE +
                "  }" + NEWLINE +
                "  if (window.WebSocket) {" + NEWLINE +
                "    socket = new WebSocket(\"" + webSocketLocation + "\");" + NEWLINE +
                "    socket.onmessage = function(event) {" + NEWLINE +
                "      var ta = document.getElementById('responseText').editor;" + NEWLINE +
                "      ta.setValue(\"/* \" + new Date().toLocaleString() + \" */\\n\" + event.data + \"\\n\" + ta.getValue());" + NEWLINE +
                "    };" + NEWLINE +
                "    socket.onopen = function(event) {" + NEWLINE +
                "      var ta = document.getElementById('responseText').editor;" + NEWLINE +
                "      ta.setValue(\"### Web Socket opened ###\\n\" + ta.getValue());" + NEWLINE +
                "      if(callback !== undefined){ callback(socket); }" + NEWLINE +
                "    };" + NEWLINE +
                "    socket.onclose = function(event) {" + NEWLINE +
                "      var ta = document.getElementById('responseText').editor;" + NEWLINE +
                "      ta.setValue(\"### Web Socket closed ###\\n\" + ta.getValue()); " + NEWLINE +
                "    };" + NEWLINE +
                "    return socket;" + NEWLINE +
                "  } else {" + NEWLINE +
                "    alert(\"Your browser does not support Web Socket.\");" + NEWLINE +
                "  }" + NEWLINE +
                "}" + NEWLINE +
                NEWLINE +
                "function send(message) {" + NEWLINE +
                "  localStorage.setItem('messageAreaText', message);" + NEWLINE +
                "  if (!window.WebSocket) { return; }" + NEWLINE +
                "  if (socket !== undefined && socket.readyState == WebSocket.OPEN) {" + NEWLINE +
                "    socket.send(message);" + NEWLINE +
                "  } else {" + NEWLINE +
                "    socket = openSocket(function(socket) { socket.send(message); });" + NEWLINE +
                "  }" + NEWLINE +
                '}' + NEWLINE +
                "</script>" + NEWLINE +
                "<form onsubmit=\"return false;\">" + NEWLINE +
                "<textarea id=\"messageArea\" name=\"message\" style=\"width:100%;height:300px;\"></textarea><br/>" + NEWLINE +
                "<input type=\"button\" value=\"Send Web Socket Data\"" + NEWLINE +
                "       onclick=\"send(document.getElementById('messageArea').editor.getValue())\" />" + NEWLINE +
                "<h3>Output</h3>" + NEWLINE +
                "<textarea id=\"responseText\" style=\"width:500px;height:300px;\"></textarea>" + NEWLINE +
                "</form>" + NEWLINE +
                "</body>" + NEWLINE +
                "</html>" + NEWLINE, CharsetUtil.UTF_8);
    }

    private WebSocketServerIndexPage() {
        // Unused
    }

    public static ByteBuf getContent(InputStream is) throws IOException {
        return getContent(is, null);
    }

    public static ByteBuf getContent(InputStream is, HashMap<String, Object> variables) throws IOException {
        String result = "";
        {
            StringBuilder sb = new StringBuilder();
            byte[] buffer = new byte[2048];
            for (int read = is.read(buffer); read != -1; read = is.read(buffer)) {
                sb.append(DEFAULT_CHARSET.decode(ByteBuffer.wrap(buffer, 0, read)));
            }
            result = sb.toString();
        }
        if (null != variables) {
            ST st = new ST(result, '$', '$');
            variables.forEach((key, value) -> st.add(key, value));
            result = st.render();
        }

        return Unpooled.copiedBuffer(result, CharsetUtil.UTF_8);
    }

}
