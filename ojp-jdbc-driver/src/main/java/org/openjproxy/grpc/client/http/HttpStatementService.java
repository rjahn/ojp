package org.openjproxy.grpc.client.http;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.openjproxy.grpc.CallResourceRequest;
import com.openjproxy.grpc.CallResourceResponse;
import com.openjproxy.grpc.ConnectionDetails;
import com.openjproxy.grpc.LobDataBlock;
import com.openjproxy.grpc.LobReference;
import com.openjproxy.grpc.OpResult;
import com.openjproxy.grpc.ReadLobRequest;
import com.openjproxy.grpc.ResultSetFetchRequest;
import com.openjproxy.grpc.SessionInfo;
import com.openjproxy.grpc.SessionTerminationStatus;
import com.openjproxy.grpc.StatementRequest;
import com.openjproxy.grpc.XaEndRequest;
import com.openjproxy.grpc.XaForgetRequest;
import com.openjproxy.grpc.XaGetTransactionTimeoutRequest;
import com.openjproxy.grpc.XaGetTransactionTimeoutResponse;
import com.openjproxy.grpc.XaIsSameRMRequest;
import com.openjproxy.grpc.XaIsSameRMResponse;
import com.openjproxy.grpc.XaPrepareRequest;
import com.openjproxy.grpc.XaPrepareResponse;
import com.openjproxy.grpc.XaRecoverRequest;
import com.openjproxy.grpc.XaRecoverResponse;
import com.openjproxy.grpc.XaResponse;
import com.openjproxy.grpc.XaSetTransactionTimeoutRequest;
import com.openjproxy.grpc.XaSetTransactionTimeoutResponse;
import com.openjproxy.grpc.XaStartRequest;
import org.openjproxy.grpc.client.StatementService;
import org.openjproxy.grpc.dto.Parameter;
import org.openjproxy.jdbc.Connection;


import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.openjproxy.grpc.ProtoConverter.propertiesToProto;
import static org.openjproxy.grpc.ProtoConverter.toProtoList;

/**
 * HTTP transport for the OJP StatementService. The payload remains protobuf;
 * HTTP only replaces the gRPC transport.
 *
 * <p>Unary calls use one protobuf message as the request and response body.
 * Server-streaming calls use a four-byte big-endian length followed by one
 * protobuf message per frame. The streaming response is consumed lazily so a
 * large result set is not buffered completely in client memory.</p>
 */
public class HttpStatementService implements StatementService {

    private static final int MAX_FRAME_SIZE = 64 * 1024 * 1024;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(10);

    private final URI baseUri;
    private final HttpClient client;

    public HttpStatementService(String endpoint) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("HTTP endpoint must not be empty");
        }

        String normalized = endpoint.trim();
        if (!normalized.endsWith("/")) {
            normalized += "/";
        }

        this.baseUri = URI.create(normalized);

        String scheme = baseUri.getScheme();

        if (!"https".equalsIgnoreCase(scheme)
                && !"http".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("OJP HTTP endpoint must use http or https: " + endpoint);
        }

        this.client = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .build();
    }

    @Override
    public SessionInfo connect(ConnectionDetails connectionDetails) throws SQLException {
        return unary("connect", connectionDetails, SessionInfo.parser());
    }

    @Override
    public OpResult executeUpdate(SessionInfo sessionInfo, String sql, List<Parameter> params,
                                  Map<String, Object> properties) throws SQLException {
        return executeUpdate(sessionInfo, sql, params, "", properties);
    }

    @Override
    public OpResult executeUpdate(SessionInfo sessionInfo, String sql, List<Parameter> params,
                                  String statementUUID, Map<String, Object> properties) throws SQLException {
        StatementRequest.Builder builder = statementRequest(sessionInfo, sql, statementUUID, params, properties);

        return unary("executeUpdate", builder.build(), OpResult.parser());
    }

    @Override
    public Iterator<OpResult> executeQuery(SessionInfo sessionInfo, String sql, List<Parameter> params,
                                           Map<String, Object> properties) throws SQLException {
        return executeQuery(sessionInfo, sql, params, "", properties);
    }

    @Override
    public Iterator<OpResult> executeQuery(SessionInfo sessionInfo, String sql, List<Parameter> params,
                                           String statementUUID, Map<String, Object> properties) throws SQLException {
        StatementRequest.Builder builder = statementRequest(sessionInfo, sql, statementUUID, params, properties);

        return streaming("executeQuery", builder.build(), OpResult.parser());
    }

    @Override
    public OpResult fetchNextRows(SessionInfo sessionInfo, String resultSetUUID, int size) throws SQLException {
        ResultSetFetchRequest request = ResultSetFetchRequest.newBuilder()
                .setSession(sessionInfo)
                .setResultSetUUID(resultSetUUID)
                .setSize(size)
                .build();

        return unary("fetchNextRows", request, OpResult.parser());
    }

    @Override
    public LobReference createLob(Connection connection, Iterator<LobDataBlock> lobDataBlock) throws SQLException {
        HttpResponse<InputStream> response = sendStreamingRequest(
                "createLob", HttpRequest.BodyPublishers.ofInputStream(() -> new LobFrameInputStream(lobDataBlock)));

        try (InputStream input = checkedResponse(response, "createLob")) {
            FrameIterator<LobReference> iterator = new FrameIterator<>(input, LobReference.parser());
            if (!iterator.hasNext()) {
                throw new SQLException("OJP HTTP endpoint returned no response frame for createLob");
            }

            return iterator.next();
        } catch (IOException ioe) {
            throw new SQLException("Unable to read OJP HTTP createLob response", ioe);
        }
    }

    @Override
    public Iterator<LobDataBlock> readLob(LobReference lobReference, long pos, int length) throws SQLException {
        ReadLobRequest request = ReadLobRequest.newBuilder()
                .setLobReference(lobReference)
                .setPosition(pos)
                .setLength(length)
                .build();

        return streaming("readLob", request, LobDataBlock.parser());
    }

    @Override
    public void terminateSession(SessionInfo session) throws SQLException {
        SessionTerminationStatus status = unary("terminateSession", session, SessionTerminationStatus.parser());

        if (!status.getTerminated()) {
            throw new SQLException("Session termination was not confirmed by the server");
        }
    }

    @Override
    public SessionInfo startTransaction(SessionInfo session) throws SQLException {
        return unary("startTransaction", session, SessionInfo.parser());
    }

    @Override
    public SessionInfo commitTransaction(SessionInfo session) throws SQLException {
        return unary("commitTransaction", session, SessionInfo.parser());
    }

    @Override
    public SessionInfo rollbackTransaction(SessionInfo session) throws SQLException {
        return unary("rollbackTransaction", session, SessionInfo.parser());
    }

    @Override
    public CallResourceResponse callResource(CallResourceRequest request) throws SQLException {
        return unary("callResource", request, CallResourceResponse.parser());
    }

    @Override
    public XaResponse xaStart(XaStartRequest request) throws SQLException {
        return unary("xaStart", request, XaResponse.parser());
    }

    @Override
    public XaResponse xaEnd(XaEndRequest request) throws SQLException {
        return unary("xaEnd", request, XaResponse.parser());
    }

    @Override
    public XaPrepareResponse xaPrepare(XaPrepareRequest request) throws SQLException {
        return unary("xaPrepare", request, XaPrepareResponse.parser());
    }

    @Override
    public XaResponse xaCommit(com.openjproxy.grpc.XaCommitRequest request) throws SQLException {
        return unary("xaCommit", request, XaResponse.parser());
    }

    @Override
    public XaResponse xaRollback(com.openjproxy.grpc.XaRollbackRequest request) throws SQLException {
        return unary("xaRollback", request, XaResponse.parser());
    }

    @Override
    public XaRecoverResponse xaRecover(XaRecoverRequest request) throws SQLException {
        return unary("xaRecover", request, XaRecoverResponse.parser());
    }

    @Override
    public XaResponse xaForget(XaForgetRequest request) throws SQLException {
        return unary("xaForget", request, XaResponse.parser());
    }

    @Override
    public XaSetTransactionTimeoutResponse xaSetTransactionTimeout(XaSetTransactionTimeoutRequest request)
            throws SQLException {
        return unary("xaSetTransactionTimeout", request, XaSetTransactionTimeoutResponse.parser());
    }

    @Override
    public XaGetTransactionTimeoutResponse xaGetTransactionTimeout(XaGetTransactionTimeoutRequest request)
            throws SQLException {
        return unary("xaGetTransactionTimeout", request, XaGetTransactionTimeoutResponse.parser());
    }

    @Override
    public XaIsSameRMResponse xaIsSameRM(XaIsSameRMRequest request) throws SQLException {
        return unary("xaIsSameRM", request, XaIsSameRMResponse.parser());
    }

    private static StatementRequest.Builder statementRequest(SessionInfo sessionInfo, String sql,
                                                              String statementUUID, List<Parameter> params,
                                                              Map<String, Object> properties) {
        StatementRequest.Builder builder = StatementRequest.newBuilder()
                .setSession(sessionInfo)
                .setStatementUUID(statementUUID == null ? "" : statementUUID)
                .setSql(sql);

        if (params != null) {
            builder.addAllParameters(toProtoList(params));
        }

        if (properties != null) {
            builder.addAllProperties(propertiesToProto(properties));
        }

        return builder;
    }

    private <T extends MessageLite> T unary(String method, MessageLite request, Parser<T> parser) throws SQLException {
        HttpResponse<byte[]> response = sendUnaryRequest(method, request.toByteArray());

        byte[] body = checkedResponse(response, method);

        try {
            return parser.parseFrom(body);
        } catch (IOException ioe) {
            throw new SQLException("Invalid protobuf response from OJP HTTP endpoint: " + method, ioe);
        }
    }

    private <T extends MessageLite> Iterator<T> streaming(String method, MessageLite request, Parser<T> parser)
            throws SQLException {
        HttpResponse<InputStream> response = sendStreamingRequest(
                method, HttpRequest.BodyPublishers.ofByteArray(request.toByteArray()));

        InputStream input = checkedResponse(response, method);

        return new FrameIterator<>(input, parser);
    }

    private HttpResponse<byte[]> sendUnaryRequest(String method, byte[] request) throws SQLException {
        try {
            HttpRequest httpRequest = HttpRequest.newBuilder(operationUri(method))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Content-Type", "application/x-protobuf")
                    .header("Accept", "application/x-protobuf")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request))
                    .build();

            return client.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();

            throw new SQLException("Interrupted while calling OJP HTTP endpoint: " + method, iex);
        } catch (IOException ex) {
            throw new SQLException("HTTP transport error while calling OJP: " + method, ex);
        }
    }

    private HttpResponse<InputStream> sendStreamingRequest(String method, HttpRequest.BodyPublisher publisher)
            throws SQLException {
        try {
            HttpRequest httpRequest = HttpRequest.newBuilder(operationUri(method))
                    .timeout(REQUEST_TIMEOUT)
                    .header("Content-Type", "application/x-protobuf")
                    .header("Accept", "application/x-protobuf")
                    .POST(publisher)
                    .build();

            return client.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
        } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();

            throw new SQLException("Interrupted while calling OJP HTTP endpoint: " + method, iex);
        } catch (IOException ex) {
            throw new SQLException("HTTP transport error while calling OJP: " + method, ex);
        }
    }

    private URI operationUri(String method) {
        return baseUri.resolve("StatementService/" + method);
    }

    private static <T> T checkedResponse(HttpResponse<T> response, String method) throws SQLException {
        if (response.statusCode() / 100 == 2) {
            return response.body();
        }

        String message;

        try {
            if (response.body() instanceof InputStream) {
                InputStream input = (InputStream) response.body();

                try (InputStream closeable = input) {
                    message = new String(closeable.readAllBytes(), StandardCharsets.UTF_8);
                }
            } else {
                T body = response.body();

                if (body instanceof byte[]) {
                    message = new String((byte[])body, StandardCharsets.UTF_8);
                }
                else {
                    message = String.valueOf(response.body());
                }
            }
        } catch (IOException ex) {
            message = "<unable to read error response: " + ex.getMessage() + ">";
        }

        throw new SQLException("OJP HTTP " + response.statusCode() + " for " + method + ": " + message);
    }

    private static final class LobFrameInputStream extends InputStream {
        private final Iterator<LobDataBlock> iterator;
        private byte[] current;
        private int position;
        private boolean eof;

        private LobFrameInputStream(Iterator<LobDataBlock> iterator) {
            this.iterator = iterator;
        }

        @Override
        public int read() throws IOException {
            if (!ensureData()) {
                return -1;
            }

            return current[position++] & 0xff;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (buffer == null) {
                throw new NullPointerException("buffer");
            }

            if (offset < 0 || length < 0 || length > buffer.length - offset) {
                throw new IndexOutOfBoundsException();
            }

            if (length == 0) {
                return 0;
            }

            if (!ensureData()) {
                return -1;
            }

            int count = Math.min(length, current.length - position);

            System.arraycopy(current, position, buffer, offset, count);

            position += count;

            return count;
        }

        private boolean ensureData() throws IOException {
            if (eof) {
                return false;
            }

            if (current != null && position < current.length) {
                return true;
            }

            if (iterator == null || !iterator.hasNext()) {
                eof = true;
                return false;
            }

            byte[] protobuf = iterator.next().toByteArray();

            if (protobuf.length > MAX_FRAME_SIZE) {
                throw new IOException("Protobuf frame exceeds maximum size: " + protobuf.length);
            }

            current = new byte[Integer.BYTES + protobuf.length];
            current[0] = (byte) (protobuf.length >>> 24);
            current[1] = (byte) (protobuf.length >>> 16);
            current[2] = (byte) (protobuf.length >>> 8);
            current[3] = (byte) protobuf.length;

            System.arraycopy(protobuf, 0, current, Integer.BYTES, protobuf.length);

            position = 0;

            return true;
        }
    }

    private static final class FrameIterator<T extends MessageLite> implements Iterator<T>, AutoCloseable {
        private final InputStream input;
        private final Parser<T> parser;

        private T next;

        private boolean loaded;
        private boolean closed;

        private FrameIterator(InputStream input, Parser<T> parser) {
            this.input = input;
            this.parser = parser;
        }

        @Override
        public boolean hasNext() {
            if (loaded) {
                return next != null;
            }

            if (closed) {
                return false;
            }

            loaded = true;

            try {
                byte[] header = input.readNBytes(Integer.BYTES);

                if (header.length == 0) {
                    close();
                    next = null;
                    return false;
                }

                if (header.length != Integer.BYTES) {
                    throw new IOException("Truncated OJP HTTP protobuf frame header");
                }

                int length = ((header[0] & 0xff) << 24)
                        | ((header[1] & 0xff) << 16)
                        | ((header[2] & 0xff) << 8)
                        | (header[3] & 0xff);

                if (length < 0 || length > MAX_FRAME_SIZE) {
                    throw new IOException("Invalid OJP HTTP protobuf frame length: " + length);
                }

                byte[] frame = input.readNBytes(length);

                if (frame.length != length) {
                    throw new EOFException("Truncated OJP HTTP protobuf frame");
                }

                next = parser.parseFrom(new ByteArrayInputStream(frame));

                return true;
            } catch (IOException ex) {
                closeQuietly();

                throw new IllegalStateException("Invalid OJP HTTP protobuf stream", ex);
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            T result = next;

            next = null;

            loaded = false;

            return result;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;

                input.close();
            }
        }

        private void closeQuietly() {
            try {
                close();
            } catch (IOException ioe) {
                // Original parsing exception is more useful to the caller.
            }
        }
    }
}
