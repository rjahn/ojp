package org.openjproxy.http;

import com.google.protobuf.MessageLite;
import io.grpc.stub.StreamObserver;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

final class HttpRpcObserver<T> implements StreamObserver<T> {
    private final HttpServletResponse response;
    private final OutputStream output;
    private final AtomicBoolean completed = new AtomicBoolean();

    private final boolean framed;

    HttpRpcObserver(HttpServletResponse response, OutputStream output, boolean framed) {
        this.response = response;
        this.output = output;
        this.framed = framed;
    }

    @Override
    public synchronized void onNext(T value) {
        try {
            if (!(value instanceof MessageLite)) {
                throw new IOException("OJP response is not a protobuf message: " + value);
            }

            byte[] bytes = ((MessageLite) value).toByteArray();

            if (framed) {
                LengthPrefixedProto.write(output, bytes);
            } else {
                output.write(bytes);
                output.flush();
            }
        } catch (IOException ioe) {
            onError(ioe);
        }
    }

    @Override
    public synchronized void onError(Throwable throwable) {
        if (completed.compareAndSet(false, true)) {
            if (!response.isCommitted()) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }

            try {
                output.flush();
            } catch (IOException ioe) {
        	// ignore
            }
        }
    }

    @Override
    public synchronized void onCompleted() {
        if (completed.compareAndSet(false, true)) {
            try {
                output.flush();
            } catch (IOException ioe) {
        	// ignore
            }
        }
    }
}
