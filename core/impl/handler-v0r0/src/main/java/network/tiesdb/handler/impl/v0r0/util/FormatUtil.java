package network.tiesdb.handler.impl.v0r0.util;

import javax.xml.bind.DatatypeConverter;

public final class FormatUtil {

    private FormatUtil() {
    }

    public static String pringHex(byte[] bytes) {
        return null == bytes ? null : DatatypeConverter.printHexBinary(bytes);
    }
}
