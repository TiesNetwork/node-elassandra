package network.tiesdb.handler.impl.v0r0.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;

import com.tiesdb.protocol.api.TiesDBProtocol.TiesDBChannelOutput;

public class StreamOutput implements TiesDBChannelOutput {

    private final OutputStream bos;
    private volatile boolean isClosed = false;

    public StreamOutput(OutputStream os) {
        this.bos = os;
    }

    @Override
    public boolean isFinished() {
        return isClosed;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (isFinished()) {
            throw new EOFException();
        }
        bos.write(b);
    }

    @Override
    public void flush() throws IOException {
        bos.flush();
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
    }

}
