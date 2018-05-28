package network.tiesdb.handler.impl.v0r0.util;

import java.io.IOException;
import java.io.InputStream;

import com.tiesdb.protocol.api.TiesDBProtocol.TiesDBChannelInput;

public class StreamInput implements TiesDBChannelInput {

    private final InputStream is;
    private volatile boolean isClosed = false;

    public StreamInput(InputStream is) {
        this.is = is;
    }

    @Override
    public boolean isFinished() {
        if (isClosed) {
            return true;
        }
        try {
            is.mark(1);
            return is.read() == -1;
        } catch (IOException e) {
            return true;
        } finally {
            try {
                is.reset();
            } catch (IOException e) {
                return true;
            }
        }
    }

    @Override
    public byte readByte() throws IOException {
        return (byte) is.read();
    }

    @Override
    public int skip(int byteCount) throws IOException {
        return (int) is.skip(byteCount);
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
