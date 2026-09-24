# OJP HTTP Servlet transport

This module exposes the existing OJP `StatementServiceImpl` through HTTP(S) for deployment in a Jakarta Servlet compatible container such as Tomcat 10+.

The transport does not implement JDBC operations itself. It maps HTTP requests to the existing OJP protobuf messages and `StatementServiceImpl` methods.

## Endpoint

The WAR is named `ojp.war` and exposes:

```text
POST /jdbc/connect
POST /jdbc/executeUpdate
POST /jdbc/executeQuery
POST /jdbc/fetchNextRows
POST /jdbc/createLob
POST /jdbc/readLob
POST /jdbc/terminateSession
POST /jdbc/startTransaction
POST /jdbc/commitTransaction
POST /jdbc/rollbackTransaction
POST /jdbc/callResource
POST /jdbc/xaStart
POST /jdbc/xaEnd
POST /jdbc/xaPrepare
POST /jdbc/xaCommit
POST /jdbc/xaRollback
POST /jdbc/xaRecover
POST /jdbc/xaForget
POST /jdbc/xaSetTransactionTimeout
POST /jdbc/xaGetTransactionTimeout
POST /jdbc/xaIsSameRM
```

Unary calls use a raw protobuf request and raw protobuf response with content type `application/x-protobuf`.

Server-streaming calls are consumed incrementally and use a sequence of protobuf messages framed as:

```text
4-byte big-endian length
protobuf payload
4-byte big-endian length
protobuf payload
...
```

`createLob` uses the same framing for both the request and response stream. The client sends the LOB frames incrementally instead of buffering the complete LOB in memory.

## JDBC URL

The JDBC driver contains a second transport implementation based on the JDK `java.net.http.HttpClient`; no additional HTTP client library is required.

Example:

```text
jdbc:ojp[https://dbproxy.example.com/ojp/jdbc]_oracle:thin:@oracle01:1521/ORCL
```

The part before `]_` is the OJP HTTP endpoint. The JDBC URL after `]_` is sent to the server as `ConnectionDetails.url` and is therefore used by the existing OJP server-side JDBC logic.

## Tomcat

Deploy `ojp.war` to Tomcat 10+ and configure the normal OJP server properties/drivers as required by the existing OJP server module.

Use HTTPS at Tomcat or at a reverse proxy. Authentication/authorization is intentionally not part of this transport module; protect the endpoint with the existing infrastructure (mTLS, reverse proxy authentication, network ACLs, etc.).

## Important

The current implementation is a private OJP transport protocol, not gRPC-Web. Both sides are included in this module set and therefore use the same protobuf/framing contract.
