package org.openjproxy.http;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The <code>LengthPrefixedProto</code>buf framing used only for streaming HTTP RPCs.
 */
final class LengthPrefixedProto {
    private static final int MAX_FRAME = 64 * 1024 * 1024;

    private LengthPrefixedProto() {}

    static byte[] read(InputStream input) throws IOException {
        DataInputStream in = new DataInputStream(input);
        final int length;

        try {
            length = in.readInt();
        } catch (EOFException eof) {
            return null;
        }

        if (length < 0 || length > MAX_FRAME) {
            throw new IOException("Invalid OJP HTTP frame length: " + length);
        }

        byte[] data = new byte[length];

        in.readFully(data);

        return data;
    }

    static void write(OutputStream output, byte[] data) throws IOException {
        DataOutputStream out = new DataOutputStream(output);
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }
}
