package org.openjproxy.http;

import com.openjproxy.grpc.StatementServiceGrpc;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;
import org.openjproxy.grpc.server.CircuitBreakerMetrics;
import org.openjproxy.grpc.server.CircuitBreakerRegistry;
import org.openjproxy.grpc.server.ServerConfiguration;
import org.openjproxy.grpc.server.SessionManagerImpl;
import org.openjproxy.grpc.server.StatementServiceImpl;
import org.openjproxy.grpc.server.utils.DriverLoader;
import org.openjproxy.grpc.server.utils.DriverUtils;

import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HTTP transport adapter around the existing OJP StatementServiceImpl.
 * No JDBC operation is reimplemented here; the existing OJP actions are called directly.
 */
final class OjpHttpService implements AutoCloseable {
    private static final String BASE = "/StatementService/";

    private final StatementServiceImpl delegate;
    private final ScheduledExecutorService cleanupExecutor;
    private final Map<String, RpcDefinition> definitions = new HashMap<>();

    private OjpHttpService(StatementServiceImpl delegate,
                           ScheduledExecutorService cleanupExecutor) {
        this.delegate = delegate;
        this.cleanupExecutor = cleanupExecutor;

        register("connect", StatementServiceGrpc.getConnectMethod(), "connect", false);
        register("executeUpdate", StatementServiceGrpc.getExecuteUpdateMethod(), "executeUpdate", false);
        register("executeQuery", StatementServiceGrpc.getExecuteQueryMethod(), "executeQuery", false);
        register("fetchNextRows", StatementServiceGrpc.getFetchNextRowsMethod(), "fetchNextRows", false);
        register("createLob", StatementServiceGrpc.getCreateLobMethod(), "createLob", true);
        register("readLob", StatementServiceGrpc.getReadLobMethod(), "readLob", false);
        register("terminateSession", StatementServiceGrpc.getTerminateSessionMethod(), "terminateSession", false);
        register("startTransaction", StatementServiceGrpc.getStartTransactionMethod(), "startTransaction", false);
        register("commitTransaction", StatementServiceGrpc.getCommitTransactionMethod(), "commitTransaction", false);
        register("rollbackTransaction", StatementServiceGrpc.getRollbackTransactionMethod(),
                "rollbackTransaction", false);
        register("callResource", StatementServiceGrpc.getCallResourceMethod(), "callResource", false);
        register("xaStart", StatementServiceGrpc.getXaStartMethod(), "xaStart", false);
        register("xaEnd", StatementServiceGrpc.getXaEndMethod(), "xaEnd", false);
        register("xaPrepare", StatementServiceGrpc.getXaPrepareMethod(), "xaPrepare", false);
        register("xaCommit", StatementServiceGrpc.getXaCommitMethod(), "xaCommit", false);
        register("xaRollback", StatementServiceGrpc.getXaRollbackMethod(), "xaRollback", false);
        register("xaRecover", StatementServiceGrpc.getXaRecoverMethod(), "xaRecover", false);
        register("xaForget", StatementServiceGrpc.getXaForgetMethod(), "xaForget", false);
        register("xaSetTransactionTimeout", StatementServiceGrpc.getXaSetTransactionTimeoutMethod(),
                "xaSetTransactionTimeout", false);
        register("xaGetTransactionTimeout", StatementServiceGrpc.getXaGetTransactionTimeoutMethod(),
                "xaGetTransactionTimeout", false);
        register("xaIsSameRM", StatementServiceGrpc.getXaIsSameRMMethod(), "xaIsSameRM", false);
    }

    static OjpHttpService create(ServletContext context) {
        ServerConfiguration config = new ServerConfiguration();
        DriverLoader.loadDriversFromPath(config.getDriversPath());
        DriverUtils.registerDrivers(config.getDriversPath());

        Map<String, org.openjproxy.grpc.server.cache.CacheConfiguration> cache = new ConcurrentHashMap<>();
        SessionManagerImpl sessionManager = new SessionManagerImpl(cache);
        CircuitBreakerRegistry breakers = new CircuitBreakerRegistry(
                config.getCircuitBreakerTimeout(),
                config.getCircuitBreakerThreshold(),
                CircuitBreakerMetrics.noop());

        StatementServiceImpl delegate = new StatementServiceImpl(
                sessionManager, breakers, config, cache);

        ScheduledExecutorService cleanup = null;
        if (config.isSessionCleanupEnabled()) {
            cleanup = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ojp-http-session-cleanup");
                t.setDaemon(true);
                return t;
            });

            long interval = config.getSessionCleanupIntervalMinutes();
            long timeout = config.getSessionTimeoutMinutes() * 60_000L;

            cleanup.scheduleAtFixedRate(
                    new org.openjproxy.grpc.server.SessionCleanupTask(sessionManager, timeout),
                    interval, interval, TimeUnit.MINUTES);
        }

        return new OjpHttpService(delegate, cleanup);
    }

    private void register(String path, MethodDescriptor<?, ?> descriptor,
                          String javaMethod, boolean clientStreaming) {
        definitions.put(path, new RpcDefinition(descriptor, javaMethod, clientStreaming));
    }

    void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        String path = request.getPathInfo();
        if (path == null || !path.startsWith(BASE)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        String operation = path.substring(BASE.length());

        RpcDefinition definition = definitions.get(operation);
        if (definition == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        response.setContentType("application/x-protobuf");
        response.setBufferSize(16 * 1024);

        if (definition.clientStreaming) {
            handleClientStreaming(request, response, definition);
        } else {
            handleUnaryOrServerStreaming(request, response, definition);
        }
    }

    private void handleUnaryOrServerStreaming(HttpServletRequest request,
                                               HttpServletResponse response,
                                               RpcDefinition definition) throws Exception {
        byte[] body = request.getInputStream().readAllBytes();
        Object message = definition.descriptor.getRequestMarshaller()
                .parse(new ByteArrayInputStream(body));

        boolean streamingResponse = !definition.descriptor.getType().serverSendsOneMessage();

        OutputStream output = response.getOutputStream();
        HttpRpcObserver<Object> observer = new HttpRpcObserver<>(response, output, streamingResponse);
        invoke(definition, message, observer);
    }

    private void handleClientStreaming(HttpServletRequest request,
                                       HttpServletResponse response,
                                       RpcDefinition definition) throws Exception {
        OutputStream output = response.getOutputStream();
        HttpRpcObserver<Object> observer = new HttpRpcObserver<>(response, output, true);

        Method method = findMethod(definition.javaMethod, definition.clientStreaming);
        @SuppressWarnings("unchecked")
        StreamObserver<Object> requestObserver =
                (StreamObserver<Object>) method.invoke(delegate, observer);

        while (true) {
            byte[] frame = LengthPrefixedProto.read(request.getInputStream());

            if (frame == null) {
                break;
            }

            Object message = definition.descriptor.getRequestMarshaller()
                    .parse(new ByteArrayInputStream(frame));

            requestObserver.onNext(message);
        }

        requestObserver.onCompleted();
    }

    private void invoke(RpcDefinition definition, Object message,
                        StreamObserver<Object> observer) throws Exception {
        Method method = findMethod(definition.javaMethod, false);

        try {
            method.invoke(delegate, message, observer);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;

            observer.onError(cause);

            if (cause instanceof Exception) {
                throw (Exception) cause;
            }

            throw new RuntimeException(cause);
        }
    }

    private Method findMethod(String name, boolean clientStreaming) {
        // Unary/server-streaming methods use (request, StreamObserver); the
        // createLob client-streaming method uses only (StreamObserver).
        int parameterCount = clientStreaming ? 1 : 2;

        for (Method method : StatementServiceImpl.class.getMethods()) {
            if (method.getName().equals(name) && method.getParameterCount() == parameterCount) {
                return method;
            }
        }

        throw new IllegalStateException("No StatementServiceImpl method: " + name);
    }

    @Override
    public void close() {
        if (cleanupExecutor != null) {
            cleanupExecutor.shutdownNow();
        }

        delegate.shutdown();
    }

    private static final class RpcDefinition {
        private final MethodDescriptor<?, ?> descriptor;
        private final String javaMethod;
        private final boolean clientStreaming;

        private RpcDefinition(MethodDescriptor<?, ?> descriptor,
                              String javaMethod,
                              boolean clientStreaming) {
            this.descriptor = descriptor;
            this.javaMethod = javaMethod;
            this.clientStreaming = clientStreaming;
        }
    }
}
